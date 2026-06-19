package mockupstream

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"streamguard/internal/protocol"
)

type FailureMode string

const (
	FailureNone       FailureMode = ""
	FailureDeadSocket FailureMode = "dead_socket"
	FailureSilentHang FailureMode = "silent_hang"
	FailureMalformed  FailureMode = "malformed"
)

type Options struct {
	Provider         string
	Tokens           []string
	DelayMin         time.Duration
	DelayMax         time.Duration
	Failure          FailureMode
	FailAfterTokens  int
	SplitFrames      bool
	UsageOffsetPct   float64
	RandomFailurePct int
	PerModel         map[string]Options
}

type Server struct {
	*httptest.Server
	mu    sync.Mutex
	opts  Options
	usage map[string]int
}

func NewHandler(opts Options) http.Handler {
	if opts.Provider == "" {
		opts.Provider = "mock"
	}
	if len(opts.Tokens) == 0 {
		opts.Tokens = []string{"hello ", "from ", opts.Provider}
	}
	if opts.DelayMin <= 0 {
		opts.DelayMin = 50 * time.Millisecond
	}
	if opts.DelayMax < opts.DelayMin {
		opts.DelayMax = opts.DelayMin + 100*time.Millisecond
	}
	s := &Server{opts: opts, usage: make(map[string]int)}
	return s.handler()
}

func New(opts Options) *Server {
	if opts.Provider == "" {
		opts.Provider = "mock"
	}
	if len(opts.Tokens) == 0 {
		opts.Tokens = []string{"hello ", "from ", opts.Provider}
	}
	if opts.DelayMin <= 0 {
		opts.DelayMin = 50 * time.Millisecond
	}
	if opts.DelayMax < opts.DelayMin {
		opts.DelayMax = opts.DelayMin + 100*time.Millisecond
	}
	s := &Server{opts: opts, usage: make(map[string]int)}
	s.Server = httptest.NewServer(s.handler())
	return s
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream", s.stream)
	mux.HandleFunc("/usage", s.usageHandler)
	return mux
}

func (s *Server) SetOptions(opts Options) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if opts.Provider == "" {
		opts.Provider = s.opts.Provider
	}
	s.opts = opts
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-StreamGuard-Tokenizer", "mock-chunk-v1")
	flusher, _ := w.(http.Flusher)
	opts := s.snapshot(r)
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	failure := opts.Failure
	if failure == FailureNone && opts.RandomFailurePct > 0 && random.Intn(100) < opts.RandomFailurePct {
		failure = []FailureMode{FailureDeadSocket, FailureSilentHang, FailureMalformed}[random.Intn(3)]
	}
	failAfter := opts.FailAfterTokens
	if failAfter <= 0 {
		failAfter = len(opts.Tokens) / 2
	}
	for i, tok := range opts.Tokens {
		if i == failAfter {
			if injectFailure(w, r, flusher, failure) {
				return
			}
		}
		sleepContext(r.Context(), opts.delay(random))
		if opts.SplitFrames {
			_ = writeSplit(w, tok)
		} else {
			_ = protocol.WriteContent(w, tok)
		}
		flusher.Flush()
	}
	if failAfter >= len(opts.Tokens) {
		if injectFailure(w, r, flusher, failure) {
			return
		}
	}
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	flusher.Flush()
	s.mu.Lock()
	period := time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339) + "/" + time.Hour.String()
	keyHash := r.Header.Get("X-StreamGuard-Key-Hash")
	if keyHash == "" {
		keyHash = "default"
	}
	s.usage[usageKey(period, keyHash)] += len(opts.Tokens)
	s.mu.Unlock()
}

func injectFailure(w http.ResponseWriter, r *http.Request, flusher http.Flusher, failure FailureMode) bool {
	switch failure {
	case FailureDeadSocket:
		if h, ok := w.(http.Hijacker); ok {
			conn, _, err := h.Hijack()
			if err == nil {
				_ = conn.Close()
				return true
			}
		}
		return true
	case FailureSilentHang:
		<-r.Context().Done()
		return true
	case FailureMalformed:
		_, _ = w.Write([]byte("data: {broken\n\n"))
		flusher.Flush()
		return true
	default:
		return false
	}
}

func (s *Server) usageHandler(w http.ResponseWriter, r *http.Request) {
	period := r.URL.Query().Get("period")
	keyHash := r.URL.Query().Get("key")
	s.mu.Lock()
	trueCount := s.usage[usageKey(period, keyHash)]
	if keyHash == "" {
		trueCount = 0
		for k, v := range s.usage {
			if strings.HasPrefix(k, period+"|") {
				trueCount += v
			}
		}
	}
	offset := s.opts.UsageOffsetPct
	s.mu.Unlock()
	reported := trueCount + int(float64(trueCount)*(offset/100))
	_ = json.NewEncoder(w).Encode(map[string]any{"period": period, "tokens": reported})
}

func (s *Server) snapshot(r *http.Request) Options {
	s.mu.Lock()
	defer s.mu.Unlock()
	opts := s.opts
	model, ok := requestModel(r)
	if !ok || len(opts.PerModel) == 0 {
		return opts
	}
	override, ok := opts.PerModel[model]
	if !ok {
		return opts
	}
	return mergeOptions(opts, override)
}

func (o Options) delay(r *rand.Rand) time.Duration {
	if o.DelayMax <= o.DelayMin {
		return o.DelayMin
	}
	delta := o.DelayMax - o.DelayMin
	return o.DelayMin + time.Duration(r.Int63n(int64(delta)))
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func writeSplit(w http.ResponseWriter, text string) error {
	payload := `data: {"choices":[{"delta":{"content":` + strconv.Quote(text) + `}}]}`
	mid := len(payload) / 2
	if _, err := w.Write([]byte(payload[:mid])); err != nil {
		return err
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	time.Sleep(5 * time.Millisecond)
	_, err := w.Write([]byte(payload[mid:] + "\n\n"))
	return err
}

func usageKey(period, keyHash string) string {
	return period + "|" + keyHash
}

func requestModel(r *http.Request) (string, bool) {
	if r.Body == nil {
		return "", false
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", false
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	var payload struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Model == "" {
		return "", false
	}
	return payload.Model, true
}

func mergeOptions(base, override Options) Options {
	merged := override
	if merged.Provider == "" {
		merged.Provider = base.Provider
	}
	if len(merged.Tokens) == 0 {
		merged.Tokens = base.Tokens
	}
	if merged.DelayMin <= 0 {
		merged.DelayMin = base.DelayMin
	}
	if merged.DelayMax < merged.DelayMin {
		merged.DelayMax = merged.DelayMin
	}
	if merged.UsageOffsetPct == 0 {
		merged.UsageOffsetPct = base.UsageOffsetPct
	}
	return merged
}
