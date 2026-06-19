package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"streamguard/internal/auth"
	"streamguard/internal/breaker"
	"streamguard/internal/calibration"
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
}

type streamRequest struct {
	Model    string          `json:"model"`
	Messages json.RawMessage `json:"messages"`
	Stream   bool            `json:"stream"`
}

func New(cfg config.Config, keys *auth.Store) *Server {
	breakers := make(map[string]*breaker.ProviderCircuitState)
	for _, p := range cfg.Providers {
		breakers[p.Name] = breaker.New(cfg.BreakerConfigFor(p))
	}
	return &Server{
		cfg:       cfg,
		auth:      keys,
		ledger:    ledger.New(cfg.Reconciliation.Interval),
		limit:     ratelimit.New(time.Duration(cfg.RateLimit.WindowSeconds)*time.Second, cfg.RateLimit.MaxTokens),
		cal:       calibration.New(),
		tokenizer: tokenizer.ChunkCounter{},
		breakers:  breakers,
		client:    &http.Client{},
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
func (s *Server) Breaker(name string) *breaker.ProviderCircuitState {
	return s.breakers[name]
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
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

	for i, p := range providers {
		br := s.breakers[p.Name]
		if !br.AllowAttempt() {
			continue
		}
		attempted++
		if !statusSent {
			_ = protocol.WriteSSE(w, protocol.EventStatus, protocol.StatusData{State: "healthy", Provider: p.Name})
			statusSent = true
			flush(flusher)
		}
		result := s.runAttempt(r.Context(), w, flusher, body, p, key.KeyHash, key, &totalDelivered)
		finalAttemptTokens = result.tokens
		if result.err == nil {
			br.RecordSuccess()
			s.ledger.RecordTerminal(key.KeyHash, time.Now(), finalAttemptTokens, false)
			return
		}
		br.RecordFailure()
		if errors.Is(result.err, errBudgetExceeded) {
			truncated = true
			truncatedReason = protocol.ReasonBudgetExceeded
			break
		}
		reason := classify(result.err)
		if next, ok := nextAllowedProvider(providers, i+1, s.breakers); ok {
			failovers++
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
		_ = protocol.WriteSSE(w, protocol.EventTruncated, protocol.TruncatedData{
			Reason:          truncatedReason,
			TokensDelivered: totalDelivered,
			Final:           true,
		})
		flush(flusher)
		s.ledger.RecordTerminal(key.KeyHash, time.Now(), finalAttemptTokens, true)
	}
}

type attemptResult struct {
	tokens int
	err    error
}

var errBudgetExceeded = errors.New("budget_exceeded")

func (s *Server) runAttempt(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, body []byte, p config.Provider, keyHash string, key interface{ TryReserve(int64) bool }, total *int) attemptResult {
	url := strings.TrimRight(p.BaseURL, "/") + "/v1/stream"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return attemptResult{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return attemptResult{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return attemptResult{err: parser.ErrMalformed}
	}
	pr := parser.NewReader(resp.Body, s.cal)
	deadline := time.Duration(s.cfg.Timeouts.SilentHangDeadlineMS) * time.Millisecond
	var attemptTokens int
	for {
		frame, err := pr.Next(ctx, deadline)
		if err != nil {
			return attemptResult{tokens: attemptTokens, err: err}
		}
		if frame.Event == "done" {
			return attemptResult{tokens: attemptTokens}
		}
		if frame.Event != "" {
			continue
		}
		n := s.tokenizer.Count(p.Name, frame.Text)
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

func nextAllowedProvider(providers []config.Provider, start int, breakers map[string]*breaker.ProviderCircuitState) (config.Provider, bool) {
	for i := start; i < len(providers); i++ {
		if breakers[providers[i].Name].State() != breaker.Open {
			return providers[i], true
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
	if !ok || !valid || rawKey != pathKey {
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
	if !ok || raw != s.cfg.OperatorToken {
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

func flush(f http.Flusher) {
	if f != nil {
		f.Flush()
	}
}
