package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"streamguard/internal/auth"
	"streamguard/internal/breaker"
	"streamguard/internal/config"
	"streamguard/internal/mockupstream"
)

func TestBillingWorkedExampleFailoverThenSuccess(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(142), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
		Failure: mockupstream.FailureDeadSocket, FailAfterTokens: 142,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(80), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
	})
	defer p2.Close()
	srv, raw := testServer(t, p1.URL, p2.URL, 1000)
	resp := postStream(t, srv, raw)
	body := readAll(t, resp.Body)
	if !strings.Contains(body, "gateway_failover") || !strings.Contains(body, "\"tokens_delivered_before_failure\":142") {
		t.Fatalf("missing failover event or token count:\n%s", body)
	}
	sum := srv.Ledger().Summary(auth.HashKey(raw), "sg_live_***")
	if sum.TokensBilled != 80 {
		t.Fatalf("tokens_billed = %d, want 80", sum.TokensBilled)
	}
}

func TestBillingWorkedExampleExhaustion(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(142), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
		Failure: mockupstream.FailureDeadSocket, FailAfterTokens: 142,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(80), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
		Failure: mockupstream.FailureMalformed, FailAfterTokens: 80,
	})
	defer p2.Close()
	srv, raw := testServer(t, p1.URL, p2.URL, 1000)
	body := readAll(t, postStream(t, srv, raw).Body)
	if !strings.Contains(body, "\"reason\":\"all_providers_exhausted\"") || !strings.Contains(body, "\"tokens_delivered\":222") {
		t.Fatalf("missing exhaustion truncation:\n%s", body)
	}
	sum := srv.Ledger().Summary(auth.HashKey(raw), "sg_live_***")
	if sum.TokensBilled != 80 || sum.TruncatedRequests != 1 {
		t.Fatalf("summary = %+v, want billed 80 truncated 1", sum)
	}
}

func TestBudgetExceededWorkedExample(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(5), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{Provider: "anthropic"})
	defer p2.Close()
	srv, raw := testServer(t, p1.URL, p2.URL, 3)
	body := readAll(t, postStream(t, srv, raw).Body)
	if !strings.Contains(body, "\"reason\":\"budget_exceeded\"") || !strings.Contains(body, "\"tokens_delivered\":3") {
		t.Fatalf("missing budget truncation:\n%s", body)
	}
	sum := srv.Ledger().Summary(auth.HashKey(raw), "sg_live_***")
	if sum.TokensBilled != 3 || sum.TruncatedRequests != 1 {
		t.Fatalf("summary = %+v, want billed 3 truncated 1", sum)
	}
}

func TestBudgetExceededDoesNotOpenProviderCircuit(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(5), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{Provider: "anthropic"})
	defer p2.Close()
	raw := "sg_live_test"
	store := auth.NewStore(24 * time.Hour)
	if err := store.Add(raw, []string{"openai", "anthropic"}, 3, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.OperatorToken = "operator"
	cfg.Timeouts.SilentHangDeadlineMS = 50
	cfg.CircuitBreaker.FailureThreshold = 1
	cfg.Providers = []config.Provider{
		{Name: "openai", Priority: 0, BaseURL: p1.URL},
		{Name: "anthropic", Priority: 1, BaseURL: p2.URL},
	}
	srv := New(cfg, store)

	body := readAll(t, postStream(t, srv, raw).Body)
	if !strings.Contains(body, "\"reason\":\"budget_exceeded\"") {
		t.Fatalf("missing budget truncation:\n%s", body)
	}
	if got := srv.Breaker("openai").State(); got != breaker.Closed {
		t.Fatalf("budget exhaustion changed circuit state to %s, want closed", got)
	}
}

func TestFailoverDoesNotAnnounceUnclaimableHalfOpenProvider(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(2), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
		Failure: mockupstream.FailureDeadSocket, FailAfterTokens: 2,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(2), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
	})
	defer p2.Close()
	srv, raw := testServer(t, p1.URL, p2.URL, 100)
	srv.Breaker("anthropic").ForceState(breaker.HalfOpen)
	if !srv.Breaker("anthropic").TryClaimProbe() {
		t.Fatal("expected test to pre-claim anthropic half-open probe")
	}

	body := readAll(t, postStream(t, srv, raw).Body)
	if strings.Contains(body, "gateway_failover") {
		t.Fatalf("announced failover to an unclaimable provider:\n%s", body)
	}
	if !strings.Contains(body, "\"reason\":\"all_providers_exhausted\"") {
		t.Fatalf("missing exhaustion truncation:\n%s", body)
	}
}

func TestUsageOwnershipAndHealthCredentialSeparation(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{Provider: "openai"})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{Provider: "anthropic"})
	defer p2.Close()
	srv, raw := testServer(t, p1.URL, p2.URL, 100)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/usage/other", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("usage mismatch status = %d, want 403", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/healthz", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("healthz client key status = %d, want 403", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestOpenAIAdapterUsesNativeEndpointAndBearer(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s, want /v1/chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer openai-test-key" {
			t.Fatalf("authorization = %q, want bearer key", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	raw := "sg_live_test"
	store := auth.NewStore(24 * time.Hour)
	if err := store.Add(raw, []string{"openai"}, 100, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.OpenAIAPIKey = "openai-test-key"
	cfg.Providers = []config.Provider{{Name: "openai", Type: "openai", Priority: 0, BaseURL: upstream.URL}}
	srv := New(cfg, store)

	body := readAll(t, postStream(t, srv, raw).Body)
	if !strings.Contains(body, "hello ") {
		t.Fatalf("missing streamed content: %s", body)
	}
}

func TestAnthropicAdapterUsesNativeEndpointHeadersAndUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("path = %s, want /v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("x-api-key"); got != "anthropic-test-key" {
			t.Fatalf("x-api-key = %q, want test key", got)
		}
		if got := r.Header.Get("anthropic-version"); got != "2023-06-01" {
			t.Fatalf("anthropic-version = %q, want 2023-06-01", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\"}\n\n"))
		_, _ = w.Write([]byte("event: ping\ndata: {\"type\":\"ping\"}\n\n"))
		_, _ = w.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello \"}}\n\n"))
		_, _ = w.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"there\"}}\n\n"))
		_, _ = w.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n"))
		_, _ = w.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer upstream.Close()

	raw := "sg_live_test"
	store := auth.NewStore(24 * time.Hour)
	if err := store.Add(raw, []string{"anthropic"}, 100, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.AnthropicAPIKey = "anthropic-test-key"
	cfg.Providers = []config.Provider{{Name: "anthropic", Type: "anthropic", Priority: 0, BaseURL: upstream.URL}}
	srv := New(cfg, store)

	body := readAll(t, postStream(t, srv, raw).Body)
	if !strings.Contains(body, "hello ") || !strings.Contains(body, "there") {
		t.Fatalf("missing streamed content: %s", body)
	}
	sum := srv.Ledger().Summary(auth.HashKey(raw), "sg_live_***")
	if sum.TokensBilled != 5 {
		t.Fatalf("tokens billed = %d, want Anthropic usage output_tokens 5", sum.TokensBilled)
	}
}

func TestUsageEndpointAggregatesSuccessAndTruncation(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(4), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(3), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
	})
	defer p2.Close()
	srv, raw := testServer(t, p1.URL, p2.URL, 100)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	_ = readAll(t, postStreamURL(t, ts.URL, raw).Body)
	p1.SetOptions(mockupstream.Options{
		Provider: "openai", Tokens: tokens(2), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
		Failure: mockupstream.FailureDeadSocket, FailAfterTokens: 2,
	})
	p2.SetOptions(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(3), DelayMin: time.Nanosecond, DelayMax: time.Nanosecond,
		Failure: mockupstream.FailureMalformed, FailAfterTokens: 3,
	})
	body := readAll(t, postStreamURL(t, ts.URL, raw).Body)
	if !strings.Contains(body, "gateway_truncated") {
		t.Fatalf("expected truncation body, got %s", body)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/usage/"+raw, nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("usage status = %d, want 200", resp.StatusCode)
	}
	var summary struct {
		TokensBilled      int  `json:"tokens_billed"`
		TruncatedRequests int  `json:"truncated_requests"`
		DriftFlag         bool `json:"drift_flag"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		t.Fatal(err)
	}
	if summary.TokensBilled != 7 || summary.TruncatedRequests != 1 || summary.DriftFlag {
		t.Fatalf("bad usage summary: %+v", summary)
	}
}

func testServer(t *testing.T, p1URL, p2URL string, budget int64) (*Server, string) {
	t.Helper()
	raw := "sg_live_test"
	store := auth.NewStore(24 * time.Hour)
	if err := store.Add(raw, []string{"openai", "anthropic"}, budget, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.OperatorToken = "operator"
	cfg.Timeouts.SilentHangDeadlineMS = 50
	cfg.Providers = []config.Provider{
		{Name: "openai", Priority: 0, BaseURL: p1URL},
		{Name: "anthropic", Priority: 1, BaseURL: p2URL},
	}
	return New(cfg, store), raw
}

func postStream(t *testing.T, srv *Server, raw string) *http.Response {
	t.Helper()
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return postStreamURL(t, ts.URL, raw)
}

func postStreamURL(t *testing.T, baseURL, raw string) *http.Response {
	t.Helper()
	reqBody := []byte(`{"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/stream", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+raw)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAll(t, resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	return resp
}

func readAll(t *testing.T, r io.ReadCloser) string {
	t.Helper()
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func tokens(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "x "
	}
	return out
}
