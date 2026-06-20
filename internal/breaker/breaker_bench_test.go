package breaker

import (
	"testing"
	"time"
)

func BenchmarkAllowAttemptClosed(b *testing.B) {
	br := New(Config{FailureThreshold: 5, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.AllowAttempt()
	}
}

func BenchmarkAllowAttemptContended(b *testing.B) {
	br := New(Config{FailureThreshold: 5, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.AllowAttempt()
		}
	})
}

func BenchmarkRecordSuccess(b *testing.B) {
	br := New(Config{FailureThreshold: 5, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.RecordSuccess()
	}
}

func BenchmarkRecordFailure(b *testing.B) {
	br := New(Config{FailureThreshold: 1000, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.RecordFailure()
	}
}

func BenchmarkStateTransitionCycle(b *testing.B) {
	br := New(Config{FailureThreshold: 1, OpenTimeout: time.Nanosecond, HalfOpenSuccessThreshold: 1})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.RecordFailure() // closed -> open
		br.State()         // open -> half_open (timeout is nanosecond)
		br.RecordSuccess() // half_open -> closed
	}
}

func BenchmarkSnapshot(b *testing.B) {
	br := New(Config{FailureThreshold: 5, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.Snapshot()
	}
}

func BenchmarkSnapshotContended(b *testing.B) {
	br := New(Config{FailureThreshold: 5, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Snapshot()
		}
	})
}
