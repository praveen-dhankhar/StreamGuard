package tokenizer

import (
	"strings"
	"testing"
)

func BenchmarkChunkCounterShort(b *testing.B) {
	c := ChunkCounter{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("", "", "", "hello world")
	}
}

func BenchmarkChunkCounterLong(b *testing.B) {
	c := ChunkCounter{}
	text := strings.Repeat("word ", 200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("", "", "", text)
	}
}

func BenchmarkChunkCounterEmpty(b *testing.B) {
	c := ChunkCounter{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("", "", "", "")
	}
}

func BenchmarkProviderAwareCounterOpenAI(b *testing.B) {
	reg := NewRegistry()
	c := NewProviderAwareCounter(reg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("openai", "gpt-4", "", "Hello, how are you doing today?")
	}
}

func BenchmarkProviderAwareCounterAnthropic(b *testing.B) {
	reg := NewRegistry()
	c := NewProviderAwareCounter(reg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("anthropic", "claude-3", "", "Hello, how are you doing today?")
	}
}

func BenchmarkProviderAwareCounterFallback(b *testing.B) {
	reg := NewRegistry()
	c := NewProviderAwareCounter(reg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("unknown", "", "", "Hello, how are you doing today?")
	}
}

func BenchmarkProviderAwareCounterMockHint(b *testing.B) {
	reg := NewRegistry()
	c := NewProviderAwareCounter(reg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("openai", "gpt-4", "mock-chunk-v1", "Hello, how are you doing today?")
	}
}

func BenchmarkRegistryObserve(b *testing.B) {
	reg := NewRegistry()
	reg.Register("openai", "tiktoken-go")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.Observe("openai", i%2 == 0)
	}
}

func BenchmarkRegistrySnapshot(b *testing.B) {
	reg := NewRegistry()
	reg.Register("openai", "tiktoken-go")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.Snapshot("openai")
	}
}
