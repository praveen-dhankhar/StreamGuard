package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsDuplicateProviderPriority(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.yaml")
	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "config.yaml")
	body := strings.ReplaceAll(`circuit_breaker:
  failure_threshold: 3
  open_timeout_s: 30
  half_open_success_threshold: 1
providers:
  - name: openai
    priority: 0
    base_url: http://127.0.0.1:9001
  - name: anthropic
    priority: 0
    base_url: http://127.0.0.1:9002
timeouts:
  silent_hang_deadline_ms: 4500
reconciliation:
  interval: 1h
  drift_threshold_pct: 4.2
rate_limit:
  window_s: 60
budget:
  default_period: 24h
auth:
  keys_file: KEYS
shutdown:
  drain_timeout_s: 30
`, "KEYS", keys)
	if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(cfg)
	if err == nil || !strings.Contains(err.Error(), "duplicate provider priority") {
		t.Fatalf("expected duplicate priority error, got %v", err)
	}
}

func TestLoadParsesProviderType(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.yaml")
	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "config.yaml")
	body := strings.ReplaceAll(`providers:
  - name: openai
    type: openai
    priority: 0
    base_url: https://198.51.100.10
auth:
  keys_file: KEYS
`, "KEYS", keys)
	if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Providers[0].ProviderType(); got != "openai" {
		t.Fatalf("provider type = %q, want openai", got)
	}
}

func TestLoadRejectsInvalidProviderType(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.yaml")
	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "config.yaml")
	body := strings.ReplaceAll(`providers:
  - name: openai
    type: browser
    priority: 0
    base_url: https://api.openai.com
auth:
  keys_file: KEYS
`, "KEYS", keys)
	if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(cfg)
	if err == nil || !strings.Contains(err.Error(), "type must be mock, openai, or anthropic") {
		t.Fatalf("expected invalid provider type error, got %v", err)
	}
}

func TestLoadRejectsInvalidRateLimitMaxTokens(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.yaml")
	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "config.yaml")
	body := strings.ReplaceAll(`providers:
  - name: openai
    priority: 0
    base_url: http://127.0.0.1:9001
rate_limit:
  window_s: 60
  max_tokens: 0
auth:
  keys_file: KEYS
`, "KEYS", keys)
	if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(cfg)
	if err == nil || !strings.Contains(err.Error(), "rate_limit.max_tokens") {
		t.Fatalf("expected invalid max_tokens error, got %v", err)
	}
}

func TestLoadRejectsProductionProviderHTTPAndInternalHosts(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.yaml")
	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		provider  string
		wantError string
	}{
		{
			name: "http scheme",
			provider: `providers:
  - name: openai
    type: openai
    priority: 0
    base_url: http://198.51.100.10
auth:
  keys_file: KEYS
`,
			wantError: "scheme must be https",
		},
		{
			name: "loopback ip",
			provider: `providers:
  - name: openai
    type: openai
    priority: 0
    base_url: https://127.0.0.1:8080
auth:
  keys_file: KEYS
`,
			wantError: "private, loopback",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+".yaml")
			body := strings.ReplaceAll(tc.provider, "KEYS", keys)
			if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("expected %q error, got %v", tc.wantError, err)
			}
		})
	}
}
