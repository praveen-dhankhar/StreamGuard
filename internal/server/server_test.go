package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"streamguard/internal/auth"
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
	reqBody := []byte(`{"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/stream", bytes.NewReader(reqBody))
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
