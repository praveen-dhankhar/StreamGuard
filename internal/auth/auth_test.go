package auth

import (
	"os"
	"path/filepath"
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
