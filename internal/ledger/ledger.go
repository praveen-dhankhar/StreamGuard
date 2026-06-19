package ledger

import (
	"sync"
	"time"
)

type Entry struct {
	APIKeyHash        string
	BillingPeriod     string
	TokensBilled      int
	TruncatedRequests int
	LastReconciledAt  *time.Time
	DriftFlag         bool
	ProviderTokens    map[string]int
}

type Summary struct {
	APIKey            string     `json:"api_key"`
	TokensBilled      int        `json:"tokens_billed"`
	TruncatedRequests int        `json:"truncated_requests"`
	LastReconciledAt  *time.Time `json:"last_reconciled_at"`
	DriftFlag         bool       `json:"drift_flag"`
}

type Store struct {
	mu       sync.Mutex
	entries  map[string]*Entry
	interval time.Duration
}

func New(interval time.Duration) *Store {
	if interval <= 0 {
		interval = time.Hour
	}
	return &Store{entries: make(map[string]*Entry), interval: interval}
}

func BillingPeriod(t time.Time, interval time.Duration) string {
	if interval <= 0 {
		interval = time.Hour
	}
	start := t.UTC().Truncate(interval)
	return start.Format(time.RFC3339) + "/" + interval.String()
}

func (s *Store) RecordTerminal(apiKeyHash string, provider string, terminalAt time.Time, tokensBilled int, truncated bool) {
	if tokensBilled < 0 {
		tokensBilled = 0
	}
	period := BillingPeriod(terminalAt, s.interval)
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(apiKeyHash, period)
	e.TokensBilled += tokensBilled
	if provider != "" && tokensBilled > 0 {
		e.ProviderTokens[provider] += tokensBilled
	}
	if truncated {
		e.TruncatedRequests++
	}
}

func (s *Store) UpsertReconciliation(apiKeyHash, period string, at time.Time, driftFlag bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(apiKeyHash, period)
	ts := at.UTC()
	e.LastReconciledAt = &ts
	e.DriftFlag = driftFlag
}

func (s *Store) Summary(apiKeyHash, redacted string) Summary {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out Summary
	out.APIKey = redacted
	for _, e := range s.entries {
		if e.APIKeyHash != apiKeyHash {
			continue
		}
		out.TokensBilled += e.TokensBilled
		out.TruncatedRequests += e.TruncatedRequests
		out.DriftFlag = out.DriftFlag || e.DriftFlag
		if e.LastReconciledAt != nil && (out.LastReconciledAt == nil || e.LastReconciledAt.After(*out.LastReconciledAt)) {
			ts := *e.LastReconciledAt
			out.LastReconciledAt = &ts
		}
	}
	return out
}

func (s *Store) Entries() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		clone := Entry{
			APIKeyHash:        e.APIKeyHash,
			BillingPeriod:     e.BillingPeriod,
			TokensBilled:      e.TokensBilled,
			TruncatedRequests: e.TruncatedRequests,
			DriftFlag:         e.DriftFlag,
		}
		if e.LastReconciledAt != nil {
			ts := *e.LastReconciledAt
			clone.LastReconciledAt = &ts
		}
		if len(e.ProviderTokens) > 0 {
			clone.ProviderTokens = make(map[string]int, len(e.ProviderTokens))
			for k, v := range e.ProviderTokens {
				clone.ProviderTokens[k] = v
			}
		}
		out = append(out, clone)
	}
	return out
}

func (s *Store) getLocked(apiKeyHash, period string) *Entry {
	key := apiKeyHash + "|" + period
	e := s.entries[key]
	if e == nil {
		e = &Entry{
			APIKeyHash:     apiKeyHash,
			BillingPeriod:  period,
			ProviderTokens: make(map[string]int),
		}
		s.entries[key] = e
	}
	return e
}
