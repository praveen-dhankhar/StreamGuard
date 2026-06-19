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
