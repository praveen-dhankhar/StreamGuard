package main

import (
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
)

func TestReferenceClientRendersFailoverAndRegeneration(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/stream" {
			t.Fatalf("path = %s, want /v1/stream", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: gateway_status\ndata: {\"state\":\"healthy\",\"provider\":\"openai\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"primary \"}}]}\n\n"))
		_, _ = w.Write([]byte("event: gateway_failover\ndata: {\"reason\":\"dead_socket\",\"tokens_delivered_before_failure\":1,\"provider_from\":\"openai\",\"provider_to\":\"anthropic\",\"attempt\":1}\n\n"))
		_, _ = w.Write([]byte("event: gateway_regenerating\ndata: {\"keep_partial_visible\":true}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"secondary\"}}]}\n\n"))
	}))
	defer ts.Close()

	cmd := exec.Command("go", "run", ".", "--no-color", "--endpoint", ts.URL+"/v1/stream", "--api-key", "sg_live_test", "hello")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reference client failed: %v\n%s", err, out)
	}
	text := string(out)
	for _, want := range []string{
		"provider=openai state=healthy",
		"failover 1: openai -> anthropic reason=dead_socket",
		"retained partial remains visible; regenerated block starts below",
		"--- regenerated output ---",
		"secondary",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q:\n%s", want, text)
		}
	}
}
