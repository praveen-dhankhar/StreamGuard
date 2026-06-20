package ratelimit

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkAdmitSingleKey(b *testing.B) {
	l := New(time.Minute, 1<<60)
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Admit("key1", now)
	}
}

func BenchmarkAdmitManyKeys(b *testing.B) {
	l := New(time.Minute, 1<<60)
	now := time.Now()
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key_%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Admit(keys[i%len(keys)], now)
	}
}

func BenchmarkAddSingleKey(b *testing.B) {
	l := New(time.Minute, 1<<60)
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Add("key1", 10, now)
	}
}

func BenchmarkAdmitContended(b *testing.B) {
	l := New(time.Minute, 1<<60)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Admit("key1", now)
		}
	})
}

func BenchmarkAdmitAfterPrune(b *testing.B) {
	l := New(time.Second, 1<<60)
	base := time.Now()
	// Pre-fill with events that will be pruned
	for i := 0; i < 100; i++ {
		l.Add("key1", 1, base.Add(-2*time.Second))
	}
	now := base
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Admit("key1", now)
	}
}
