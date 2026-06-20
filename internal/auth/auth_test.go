package auth

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadKeysFileParsesIndentedListEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.yaml")
	body := `keys:
  - key: sg_live_demo
    provider_allowlist: [openai, anthropic]
    token_budget: 10000
    budget_period: 24h
  - key: sg_live_tiny
    provider_allowlist: [openai]
    token_budget: 3
    budget_period: 24h
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	store := NewStore(24 * time.Hour)
	if err := store.LoadKeysFile(path); err != nil {
		t.Fatalf("load keys file: %v", err)
	}

	if _, ok := store.LookupRaw("sg_live_demo"); !ok {
		t.Fatal("expected sg_live_demo to be loaded")
	}
	if _, ok := store.LookupRaw("sg_live_tiny"); !ok {
		t.Fatal("expected sg_live_tiny to be loaded")
	}
}

func TestLoadKeysFileRejectsNegativeBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.yaml")
	body := `keys:
  - key: sg_live_bad
    provider_allowlist: [openai]
    token_budget: -1
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	store := NewStore(24 * time.Hour)
	if err := store.LoadKeysFile(path); err == nil {
		t.Fatal("expected negative token budget to be rejected")
	}
}

func TestResetExpiredClearsUsedBudget(t *testing.T) {
	store := NewStore(24 * time.Hour)
	if err := store.Add("sg_live_demo", []string{"openai"}, 10, time.Hour); err != nil {
		t.Fatal(err)
	}
	rec, ok := store.LookupRaw("sg_live_demo")
	if !ok {
		t.Fatal("expected key")
	}
	if !rec.TryReserve(7) {
		t.Fatal("expected reservation to succeed")
	}
	rec.BudgetPeriodStart = time.Date(2026, 6, 19, 1, 0, 0, 0, time.UTC)

	if reset := store.ResetExpired(time.Date(2026, 6, 19, 2, 0, 0, 0, time.UTC)); reset != 1 {
		t.Fatalf("reset count = %d, want 1", reset)
	}
	if used := atomic.LoadInt64(&rec.TokensUsed); used != 0 {
		t.Fatalf("tokens used = %d, want 0", used)
	}
}
