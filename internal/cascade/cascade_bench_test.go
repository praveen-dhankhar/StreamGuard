package cascade

import (
	"testing"
	"time"
)

func BenchmarkNewSession(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewSession("session-id-123", "api-key-hash-abc")
	}
}

func BenchmarkStartAttempt(b *testing.B) {
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSession("id", "hash")
		s.StartAttempt("openai", now)
	}
}

func BenchmarkFinishAttempt(b *testing.B) {
	now := time.Now()
	s := NewSession("id", "hash")
	idx := s.StartAttempt("openai", now)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.FinishAttempt(idx, "success", 100, now)
	}
}

func BenchmarkFullCascadeFlow(b *testing.B) {
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSession("session-id", "key-hash")
		idx1 := s.StartAttempt("openai", now)
		s.FinishAttempt(idx1, "silent_hang", 25, now.Add(time.Second))
		idx2 := s.StartAttempt("anthropic", now.Add(time.Second))
		s.FinishAttempt(idx2, "success", 100, now.Add(2*time.Second))
		s.Status = StatusComplete
		s.TokensDelivered = 125
		s.FinalProvider = "anthropic"
		s.FinalAttemptTokens = 100
	}
}
