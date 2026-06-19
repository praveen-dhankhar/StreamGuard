package ledger

import (
	"testing"
	"time"
)

func TestSummaryAggregatesAcrossPeriods(t *testing.T) {
	s := New(time.Hour)
	hash := "hash"
	t1 := time.Date(2026, 6, 19, 3, 1, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 19, 4, 1, 0, 0, time.UTC)
	s.RecordTerminal(hash, "openai", t1, 10, false)
	s.RecordTerminal(hash, "anthropic", t2, 20, true)
	s.UpsertReconciliation(hash, BillingPeriod(t1, time.Hour), t1, true)
	s.UpsertReconciliation(hash, BillingPeriod(t2, time.Hour), t2, false)
	got := s.Summary(hash, "sg_live_***")
	if got.TokensBilled != 30 || got.TruncatedRequests != 1 || !got.DriftFlag {
		t.Fatalf("bad summary: %+v", got)
	}
	if got.LastReconciledAt == nil || !got.LastReconciledAt.Equal(t2) {
		t.Fatalf("bad last_reconciled_at: %+v", got.LastReconciledAt)
	}
}
