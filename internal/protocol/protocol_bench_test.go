package protocol

import (
	"io"
	"testing"
)

func BenchmarkWriteSSE(b *testing.B) {
	data := StatusData{State: "healthy", Provider: "openai"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteSSE(io.Discard, EventStatus, data)
	}
}

func BenchmarkWriteSSEFailover(b *testing.B) {
	data := FailoverData{
		Reason:                       ReasonSilentHang,
		TokensDeliveredBeforeFailure: 42,
		ProviderFrom:                 "openai",
		ProviderTo:                   "anthropic",
		Attempt:                      2,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteSSE(io.Discard, EventFailover, data)
	}
}

func BenchmarkWriteContent(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteContent(io.Discard, "Hello, this is a streaming response token.")
	}
}

func BenchmarkWriteContentLong(b *testing.B) {
	text := "This is a much longer content string that simulates a bigger token chunk from a language model. " +
		"It contains multiple sentences to better represent real-world usage patterns in streaming."
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteContent(io.Discard, text)
	}
}

func BenchmarkValidateFailoverReason(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ValidateFailoverReason(ReasonSilentHang)
	}
}

func BenchmarkValidateTruncatedReason(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ValidateTruncatedReason(ReasonBudgetExceeded)
	}
}
