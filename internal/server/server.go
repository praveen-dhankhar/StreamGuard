package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"streamguard/internal/auth"
	"streamguard/internal/breaker"
	"streamguard/internal/calibration"
	"streamguard/internal/cascade"
	"streamguard/internal/config"
	"streamguard/internal/ledger"
	"streamguard/internal/parser"
	"streamguard/internal/protocol"
	"streamguard/internal/ratelimit"
	"streamguard/internal/tokenizer"
)

type Server struct {
	cfg       config.Config
	auth      *auth.Store
	ledger    *ledger.Store
	limit     *ratelimit.Limiter
	cal       *calibration.Logger
	tokenizer tokenizer.Counter
	breakers  map[string]*breaker.ProviderCircuitState
	client    *http.Client
	registry  *tokenizer.Registry

	sessionSeq atomic.Uint64
	draining   atomic.Bool

	mu      sync.Mutex
	active  map[string]*activeSession
	forceAt *time.Timer
}

type activeSession struct {
	session *cascade.Session
	cancel  context.CancelCauseFunc
}

type streamRequest struct {
	Model    string          `json:"model"`
	Messages json.RawMessage `json:"messages"`
	Stream   bool            `json:"stream"`
}

type allowPrivateUpstreamKey struct{}

type ipResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

func New(cfg config.Config, keys *auth.Store) *Server {
	breakers := make(map[string]*breaker.ProviderCircuitState)
	registry := tokenizer.NewRegistry()
	for _, p := range cfg.Providers {
		breakers[p.Name] = breaker.New(cfg.BreakerConfigFor(p))
	}
	return &Server{
		cfg:       cfg,
		auth:      keys,
		ledger:    ledger.New(cfg.Reconciliation.Interval),
		limit:     ratelimit.New(time.Duration(cfg.RateLimit.WindowSeconds)*time.Second, cfg.RateLimit.MaxTokens),
		cal:       calibration.New(),
		tokenizer: tokenizer.NewProviderAwareCounter(registry),
		breakers:  breakers,
		client:    &http.Client{Transport: newValidatedTransport(net.DefaultResolver)},
		registry:  registry,
		active:    make(map[string]*activeSession),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream", s.handleStream)
	mux.HandleFunc("/usage/", s.handleUsage)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/livez", s.handleLivez)
	return mux
}

func (s *Server) Calibration() *calibration.Logger { return s.cal }
func (s *Server) Ledger() *ledger.Store            { return s.ledger }
func (s *Server) TokenizerRegistry() *tokenizer.Registry {
	return s.registry
}
func (s *Server) Breaker(name string) *breaker.ProviderCircuitState {
	return s.breakers[name]
}

func (s *Server) ActiveSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

func (s *Server) BeginShutdown(timeout time.Duration) {
	if !s.draining.CompareAndSwap(false, true) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forceAt != nil {
		s.forceAt.Stop()
	}
	s.forceAt = time.AfterFunc(timeout, s.forceCloseActive)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeError(w, http.StatusServiceUnavailable, "server_shutting_down", "server is draining in-flight streams")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body", "failed to read request body")
		return
	}
	var req streamRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Model == "" || len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_body", "body must include model and messages")
		return
	}
	rawKey, ok := bearer(r.Header.Get("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return
	}
	key, ok := s.auth.LookupRaw(rawKey)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return
	}
	providers := s.allowedProviders(key.ProviderAllowlist)
	if len(providers) == 0 {
		writeError(w, http.StatusForbidden, "provider_not_allowed", "no configured provider is allowed for this key")
		return
	}
	if key.Exhausted() {
		writeError(w, http.StatusTooManyRequests, "budget_exhausted", "token budget is exhausted")
		return
	}
	if !s.limit.Admit(key.KeyHash, time.Now()) {
		writeError(w, http.StatusTooManyRequests, "rate_limited", "rate limit window is saturated")
		return
	}
	active, streamCtx := s.registerSession(r.Context(), key.KeyHash)
	defer s.releaseSession(active.session.ID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)

	totalDelivered := 0
	finalAttemptTokens := 0
	statusSent := false
	failovers := 0
	truncated := false
	truncatedReason := protocol.ReasonAllProvidersExhausted
	attempted := 0
	claimedProbes := map[string]bool{}
	active.session.Status = cascade.StatusStreaming

	for i, p := range providers {
		br := s.breakers[p.Name]
		allowed := claimedProbes[p.Name]
		if allowed {
			delete(claimedProbes, p.Name)
		} else {
			allowed = br.AllowAttempt()
		}
		if !allowed {
			continue
		}
		attempted++
		attemptIndex := active.session.StartAttempt(p.Name, time.Now())
		active.session.FinalProvider = p.Name
		if !statusSent {
			_ = protocol.WriteSSE(w, protocol.EventStatus, protocol.StatusData{State: "healthy", Provider: p.Name})
			statusSent = true
			flush(flusher)
		}
		result := s.runAttempt(streamCtx, w, flusher, body, req.Model, p, key.KeyHash, key, &totalDelivered)
		finalAttemptTokens = result.tokens
		active.session.FinalAttemptTokens = result.tokens
		if result.err == nil {
			br.RecordSuccess()
			active.session.Status = cascade.StatusComplete
			active.session.FinishAttempt(attemptIndex, "success", 0, time.Now())
			s.ledger.RecordTerminal(key.KeyHash, p.Name, time.Now(), finalAttemptTokens, false)
			return
		}
		if errors.Is(result.err, errForcedShutdown) {
			active.session.Status = cascade.StatusShutdown
			active.session.FinishAttempt(attemptIndex, "shutdown", result.tokens, time.Now())
			log.Printf("forced_shutdown session=%s provider=%s tokens=%d", active.session.ID, p.Name, result.tokens)
			s.ledger.RecordTerminal(key.KeyHash, p.Name, time.Now(), result.tokens, true)
			return
		}
		if errors.Is(result.err, context.Canceled) {
			active.session.FinishAttempt(attemptIndex, "client_cancelled", result.tokens, time.Now())
			return
		}
		if errors.Is(result.err, errBudgetExceeded) {
			br.RecordSuccess()
			truncated = true
			truncatedReason = protocol.ReasonBudgetExceeded
			active.session.Status = cascade.StatusTruncated
			active.session.FinishAttempt(attemptIndex, "budget_exceeded", result.tokens, time.Now())
			break
		}
		br.RecordFailure()
		reason := classify(result.err)
		active.session.Status = cascade.StatusFailover
		active.session.FinishAttempt(attemptIndex, string(reason), result.tokens, time.Now())
		if next, ok := nextAttemptableProvider(providers, i+1, s.breakers, claimedProbes); ok {
			failovers++
			key.Release(int64(result.tokens))
			_ = protocol.WriteSSE(w, protocol.EventFailover, protocol.FailoverData{
				Reason:                       reason,
				TokensDeliveredBeforeFailure: result.tokens,
				ProviderFrom:                 p.Name,
				ProviderTo:                   next.Name,
				Attempt:                      failovers,
			})
			_ = protocol.WriteSSE(w, protocol.EventRegenerating, protocol.RegeneratingData{KeepPartialVisible: true})
			flush(flusher)
			continue
		}
		truncated = true
		truncatedReason = protocol.ReasonAllProvidersExhausted
		break
	}

	if attempted == 0 || truncated {
		active.session.Status = cascade.StatusTruncated
		_ = protocol.WriteSSE(w, protocol.EventTruncated, protocol.TruncatedData{
			Reason:          truncatedReason,
			TokensDelivered: totalDelivered,
			Final:           true,
		})
		flush(flusher)
		s.ledger.RecordTerminal(key.KeyHash, active.session.FinalProvider, time.Now(), finalAttemptTokens, true)
	}
}

type attemptResult struct {
	tokens int
	err    error
}

var errBudgetExceeded = errors.New("budget_exceeded")
var errForcedShutdown = errors.New("forced_shutdown")

func (s *Server) runAttempt(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, body []byte, model string, p config.Provider, keyHash string, key interface {
	TryReserve(int64) bool
	Release(int64)
}, total *int) attemptResult {
	req, err := s.upstreamRequest(ctx, p, body, keyHash)
	if err != nil {
		return attemptResult{err: err}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), errForcedShutdown) {
			return attemptResult{err: errForcedShutdown}
		}
		return attemptResult{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return attemptResult{err: parser.ErrMalformed}
	}
	tokenizerHint := resp.Header.Get("X-StreamGuard-Tokenizer")
	pr := parser.NewReaderForProvider(resp.Body, s.cal, p.ProviderType())
	deadline := time.Duration(s.cfg.Timeouts.SilentHangDeadlineMS) * time.Millisecond
	var attemptTokens int
	for {
		frame, err := pr.Next(ctx, deadline)
		if err != nil {
			if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), errForcedShutdown) {
				return attemptResult{tokens: attemptTokens, err: errForcedShutdown}
			}
			return attemptResult{tokens: attemptTokens, err: err}
		}
		if frame.Event == "done" {
			return attemptResult{tokens: attemptTokens}
		}
		if frame.HasUsage {
			delta := frame.UsageTokens - attemptTokens
			if delta > 0 {
				if !key.TryReserve(int64(delta)) {
					return attemptResult{tokens: attemptTokens, err: errBudgetExceeded}
				}
				s.limit.Add(keyHash, int64(delta), time.Now())
			}
			*total += delta
			attemptTokens = frame.UsageTokens
			continue
		}
		if frame.Event != "" {
			continue
		}
		n := s.tokenizer.Count(p.Name, model, tokenizerHint, frame.Text)
		if n == 0 {
			n = 1
		}
		if !key.TryReserve(int64(n)) {
			return attemptResult{tokens: attemptTokens, err: errBudgetExceeded}
		}
		s.limit.Add(keyHash, int64(n), time.Now())
		if err := protocol.WriteContent(w, frame.Text); err != nil {
			return attemptResult{tokens: attemptTokens, err: err}
		}
		flush(flusher)
		attemptTokens += n
		*total += n
	}
}

func (s *Server) upstreamRequest(ctx context.Context, p config.Provider, body []byte, keyHash string) (*http.Request, error) {
	if p.ProviderType() == "mock" {
		ctx = context.WithValue(ctx, allowPrivateUpstreamKey{}, true)
	}
	base := strings.TrimRight(p.BaseURL, "/")
	var url string
	switch p.ProviderType() {
	case "openai":
		url = base + "/v1/chat/completions"
	case "anthropic":
		url = base + "/v1/messages"
	default:
		url = base + "/v1/stream"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	switch p.ProviderType() {
	case "openai":
		if s.cfg.OpenAIAPIKey != "" {
			req.Header.Set("Authorization", "Bearer "+s.cfg.OpenAIAPIKey)
		}
	case "anthropic":
		if s.cfg.AnthropicAPIKey != "" {
			req.Header.Set("x-api-key", s.cfg.AnthropicAPIKey)
		}
		req.Header.Set("anthropic-version", "2023-06-01")
	default:
		req.Header.Set("X-StreamGuard-Key-Hash", keyHash)
	}
	return req, nil
}

func newValidatedTransport(resolver ipResolver) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		allowPrivate, _ := ctx.Value(allowPrivateUpstreamKey{}).(bool)
		validatedAddress, err := validatedDialAddress(ctx, resolver, address, allowPrivate)
		if err != nil {
			return nil, err
		}
		return dialer.DialContext(ctx, network, validatedAddress)
	}
	return transport
}

func validatedDialAddress(ctx context.Context, resolver ipResolver, address string, allowPrivate bool) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !allowPrivate && restrictedUpstreamIP(ip) {
			return "", fmt.Errorf("upstream host %q resolved to restricted address %s", host, ip)
		}
		return net.JoinHostPort(ip.String(), port), nil
	}
	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", err
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("upstream host %q resolved to no addresses", host)
	}
	var first netip.Addr
	for i, addr := range addrs {
		ip, ok := netip.AddrFromSlice(addr.IP)
		if !ok {
			return "", fmt.Errorf("upstream host %q resolved to invalid address", host)
		}
		ip = ip.Unmap()
		if i == 0 {
			first = ip
		}
		if !allowPrivate && restrictedUpstreamIP(ip) {
			return "", fmt.Errorf("upstream host %q resolved to restricted address %s", host, ip)
		}
	}
	return net.JoinHostPort(first.String(), port), nil
}

func restrictedUpstreamIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

func (s *Server) allowedProviders(allow []string) []config.Provider {
	out := make([]config.Provider, 0, len(s.cfg.Providers))
	for _, p := range s.cfg.Providers {
		for _, a := range allow {
			if a == p.Name {
				out = append(out, p)
				break
			}
		}
	}
	return out
}

func nextAttemptableProvider(providers []config.Provider, start int, breakers map[string]*breaker.ProviderCircuitState, claimedProbes map[string]bool) (config.Provider, bool) {
	for i := start; i < len(providers); i++ {
		br := breakers[providers[i].Name]
		switch br.State() {
		case breaker.Closed:
			return providers[i], true
		case breaker.HalfOpen:
			if br.TryClaimProbe() {
				claimedProbes[providers[i].Name] = true
				return providers[i], true
			}
		}
	}
	return config.Provider{}, false
}

func classify(err error) protocol.FailoverReason {
	if errors.Is(err, parser.ErrMalformed) {
		return protocol.ReasonMalformed
	}
	if errors.Is(err, parser.ErrSilentHang) {
		return protocol.ReasonSilentHang
	}
	return protocol.ReasonDeadSocket
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	pathKey := strings.TrimPrefix(r.URL.Path, "/usage/")
	rawKey, ok := bearer(r.Header.Get("Authorization"))
	key, valid := s.auth.LookupRaw(rawKey)
	if !ok || !valid || !constantTimeEqualString(rawKey, pathKey) {
		writeError(w, http.StatusForbidden, "unauthorized", "usage requires matching API key")
		return
	}
	writeJSON(w, http.StatusOK, s.ledger.Summary(key.KeyHash, auth.Redact(rawKey)))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	raw, ok := bearer(r.Header.Get("Authorization"))
	if !ok || !constantTimeEqualString(raw, s.cfg.OperatorToken) {
		writeError(w, http.StatusForbidden, "unauthorized", "operator token required")
		return
	}
	breakers := map[string]any{}
	for name, br := range s.breakers {
		breakers[name] = br.Snapshot()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "breakers": breakers})
}

func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": code, "message": msg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write_json_error=%v", err)
	}
}

func bearer(v string) (string, bool) {
	if !strings.HasPrefix(v, "Bearer ") {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
	return token, token != ""
}

func constantTimeEqualString(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func flush(f http.Flusher) {
	if f != nil {
		f.Flush()
	}
}

func (s *Server) registerSession(parent context.Context, apiKeyHash string) (*activeSession, context.Context) {
	id := fmt.Sprintf("sg-%d", s.sessionSeq.Add(1))
	ctx, cancel := context.WithCancelCause(parent)
	active := &activeSession{
		session: cascade.NewSession(id, apiKeyHash),
		cancel:  cancel,
	}
	s.mu.Lock()
	s.active[id] = active
	s.mu.Unlock()
	return active, ctx
}

func (s *Server) releaseSession(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, id)
	if len(s.active) == 0 && s.forceAt != nil {
		s.forceAt.Stop()
		s.forceAt = nil
	}
}

func (s *Server) forceCloseActive() {
	s.mu.Lock()
	sessions := make([]*activeSession, 0, len(s.active))
	for _, session := range s.active {
		sessions = append(sessions, session)
	}
	s.mu.Unlock()
	for _, session := range sessions {
		session.cancel(errForcedShutdown)
	}
}
