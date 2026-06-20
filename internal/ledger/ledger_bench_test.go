package ledger

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkRecordTerminal(b *testing.B) {
	s := New(time.Hour)
	now := time.Now().UTC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.RecordTerminal("hash", "openai", now, 10, false)
	}
}

func BenchmarkRecordTerminalContended(b *testing.B) {
	s := New(time.Hour)
	now := time.Now().UTC()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.RecordTerminal("hash", "openai", now, 10, false)
		}
	})
}

func BenchmarkBillingPeriod(b *testing.B) {
	now := time.Now().UTC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BillingPeriod(now, time.Hour)
	}
}

func BenchmarkSummarySingleEntry(b *testing.B) {
	s := New(time.Hour)
	now := time.Now().UTC()
	s.RecordTerminal("hash", "openai", now, 100, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Summary("hash", "sg_live_***")
	}
}

func BenchmarkSummaryManyEntries(b *testing.B) {
	s := New(time.Hour)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		s.RecordTerminal("hash", "openai", base.Add(time.Duration(i)*time.Hour), 10, i%5 == 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Summary("hash", "sg_live_***")
	}
}

func BenchmarkUpsertReconciliation(b *testing.B) {
	s := New(time.Hour)
	now := time.Now().UTC()
	period := BillingPeriod(now, time.Hour)
	s.RecordTerminal("hash", "openai", now, 100, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.UpsertReconciliation("hash", period, now, true)
	}
}

func BenchmarkEntries(b *testing.B) {
	s := New(time.Hour)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 50; i++ {
		s.RecordTerminal(fmt.Sprintf("hash_%d", i), "openai", base.Add(time.Duration(i)*time.Hour), 10, false)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Entries()
	}
}
