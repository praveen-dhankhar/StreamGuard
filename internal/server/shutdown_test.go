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

func TestGracefulShutdownDrainsInFlightAndRejectsNewRequests(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(5), DelayMin: 20 * time.Millisecond, DelayMax: 20 * time.Millisecond,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(5), DelayMin: 20 * time.Millisecond, DelayMax: 20 * time.Millisecond,
	})
	defer p2.Close()

	srv, raw := testServer(t, p1.URL, p2.URL, 100)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/stream", bytes.NewReader([]byte(`{"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true}`)))
	req.Header.Set("Authorization", "Bearer "+raw)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)
	srv.BeginShutdown(500 * time.Millisecond)

	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/stream", bytes.NewReader([]byte(`{"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true}`)))
	req2.Header.Set("Authorization", "Bearer "+raw)
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp2.Body)
		_ = resp2.Body.Close()
		t.Fatalf("status = %d body=%s, want 503", resp2.StatusCode, body)
	}
	_ = resp2.Body.Close()

	body := readAll(t, resp.Body)
	if !strings.Contains(body, "gateway_status") {
		t.Fatalf("expected original stream to drain cleanly, got %q", body)
	}
	sum := srv.Ledger().Summary(auth.HashKey(raw), "sg_live_***")
	if sum.TokensBilled != 5 || sum.TruncatedRequests != 0 {
		t.Fatalf("summary = %+v, want billed=5 truncated=0", sum)
	}
}

func TestGracefulShutdownForceCloseRecordsPartialBilling(t *testing.T) {
	p1 := mockupstream.New(mockupstream.Options{
		Provider: "openai", Tokens: tokens(20), DelayMin: 25 * time.Millisecond, DelayMax: 25 * time.Millisecond,
	})
	defer p1.Close()
	p2 := mockupstream.New(mockupstream.Options{
		Provider: "anthropic", Tokens: tokens(5), DelayMin: 25 * time.Millisecond, DelayMax: 25 * time.Millisecond,
	})
	defer p2.Close()

	srv, raw := testServer(t, p1.URL, p2.URL, 100)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/stream", bytes.NewReader([]byte(`{"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true}`)))
	req.Header.Set("Authorization", "Bearer "+raw)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(40 * time.Millisecond)
	srv.BeginShutdown(50 * time.Millisecond)

	_ = readAll(t, resp.Body)
	sum := srv.Ledger().Summary(auth.HashKey(raw), "sg_live_***")
	if sum.TokensBilled <= 0 || sum.TokensBilled >= 20 {
		t.Fatalf("expected partial billed tokens after force-close, got %+v", sum)
	}
	if sum.TruncatedRequests != 1 {
		t.Fatalf("expected one truncated request after force-close, got %+v", sum)
	}
}

func TestDrainingRequestReturnsServiceUnavailable(t *testing.T) {
	raw := "sg_live_test"
	store := auth.NewStore(24 * time.Hour)
	if err := store.Add(raw, []string{"openai"}, 100, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.OperatorToken = "operator"
	cfg.Timeouts.SilentHangDeadlineMS = 50
	cfg.Providers = []config.Provider{
		{Name: "openai", Priority: 0, BaseURL: "http://127.0.0.1:65535"},
	}
	srv := New(cfg, store)
	srv.BeginShutdown(time.Second)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/stream", bytes.NewReader([]byte(`{"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true}`)))
	req.Header.Set("Authorization", "Bearer "+raw)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s, want 503", resp.StatusCode, body)
	}
}
