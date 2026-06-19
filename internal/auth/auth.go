package auth

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"streamguard/internal/budget"
)

type Store struct {
	mu      sync.RWMutex
	byHash  map[string]*budget.APIKeyRecord
	rawHash map[string]string
	period  time.Duration
}

func NewStore(defaultPeriod time.Duration) *Store {
	return &Store{
		byHash:  make(map[string]*budget.APIKeyRecord),
		rawHash: make(map[string]string),
		period:  defaultPeriod,
	}
}

func HashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func Redact(raw string) string {
	if raw == "" {
		return "***"
	}
	if len(raw) <= 8 {
		return raw[:min(3, len(raw))] + "***"
	}
	return raw[:8] + "***"
}

func (s *Store) Add(raw string, allowlist []string, tokenBudget int64, period time.Duration) error {
	if raw == "" {
		return errors.New("api key cannot be empty")
	}
	if len(allowlist) == 0 {
		return errors.New("provider_allowlist cannot be empty")
	}
	if tokenBudget < 0 {
		return errors.New("token_budget cannot be negative")
	}
	if period <= 0 {
		period = s.period
	}
	rec := &budget.APIKeyRecord{
		KeyHash:           HashKey(raw),
		ProviderAllowlist: allowlist,
		TokenBudget:       tokenBudget,
		BudgetPeriod:      period,
		BudgetPeriodStart: time.Now().UTC(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byHash[rec.KeyHash] = rec
	s.rawHash[raw] = rec.KeyHash
	return nil
}

func (s *Store) LookupRaw(raw string) (*budget.APIKeyRecord, bool) {
	hash := HashKey(raw)
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.byHash[hash]
	return rec, ok
}

func (s *Store) LookupHash(hash string) (*budget.APIKeyRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.byHash[hash]
	return rec, ok
}

func (s *Store) LoadKeysFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var raw string
	var allowlist []string
	var tokenBudget int64
	var period time.Duration
	flush := func() error {
		if raw == "" {
			return nil
		}
		err := s.Add(raw, allowlist, tokenBudget, period)
		raw, allowlist, tokenBudget, period = "", nil, 0, 0
		return err
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = strings.TrimPrefix(line, "- ")
		if line == "" || strings.HasPrefix(line, "#") || line == "keys:" {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), "\"")
		switch key {
		case "key":
			if err := flush(); err != nil {
				return err
			}
			raw = val
		case "provider_allowlist":
			allowlist = parseList(val)
		case "token_budget":
			tokenBudget = parseInt64(val)
		case "budget_period":
			d, err := time.ParseDuration(val)
			if err != nil {
				return err
			}
			period = d
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush()
}

func parseList(v string) []string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(strings.TrimSuffix(v, "]"), "[")
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), "\"")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseInt64(v string) int64 {
	var n int64
	for _, r := range strings.TrimSpace(v) {
		if r >= '0' && r <= '9' {
			n = n*10 + int64(r-'0')
		}
	}
	return n
}
