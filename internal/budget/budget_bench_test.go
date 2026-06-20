package budget

import (
	"testing"
)

func BenchmarkTryReserve(b *testing.B) {
	rec := &APIKeyRecord{TokenBudget: int64(b.N) + 1000}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.TryReserve(1)
	}
}

func BenchmarkTryReserveContended(b *testing.B) {
	rec := &APIKeyRecord{TokenBudget: 1 << 60}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec.TryReserve(1)
		}
	})
}

func BenchmarkExhausted(b *testing.B) {
	rec := &APIKeyRecord{TokenBudget: 100, TokensUsed: 50}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Exhausted()
	}
}

func BenchmarkAllows(b *testing.B) {
	rec := &APIKeyRecord{
		ProviderAllowlist: []string{"openai", "anthropic", "google", "cohere"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Allows("anthropic")
	}
}

func BenchmarkAllowsMiss(b *testing.B) {
	rec := &APIKeyRecord{
		ProviderAllowlist: []string{"openai", "anthropic", "google", "cohere"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Allows("unknown")
	}
}
