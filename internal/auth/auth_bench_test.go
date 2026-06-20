package auth

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkHashKey(b *testing.B) {
	key := "sg_live_abcdefghij1234567890"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HashKey(key)
	}
}

func BenchmarkRedact(b *testing.B) {
	key := "sg_live_abcdefghij1234567890"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Redact(key)
	}
}

func BenchmarkRedactShort(b *testing.B) {
	key := "sg_ab"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Redact(key)
	}
}

func BenchmarkLookupRaw(b *testing.B) {
	store := NewStore(24 * time.Hour)
	store.Add("sg_live_bench_key", []string{"openai"}, 10000, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.LookupRaw("sg_live_bench_key")
	}
}

func BenchmarkLookupHash(b *testing.B) {
	store := NewStore(24 * time.Hour)
	store.Add("sg_live_bench_key", []string{"openai"}, 10000, 0)
	hash := HashKey("sg_live_bench_key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.LookupHash(hash)
	}
}

func BenchmarkLookupRawContended(b *testing.B) {
	store := NewStore(24 * time.Hour)
	store.Add("sg_live_bench_key", []string{"openai"}, 10000, 0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.LookupRaw("sg_live_bench_key")
		}
	})
}

func BenchmarkLookupManyKeys(b *testing.B) {
	store := NewStore(24 * time.Hour)
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("sg_live_key_%04d", i)
		keys[i] = k
		store.Add(k, []string{"openai", "anthropic"}, 10000, 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.LookupRaw(keys[i%len(keys)])
	}
}
