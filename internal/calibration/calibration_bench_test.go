package calibration

import (
	"testing"
)

func BenchmarkSample(b *testing.B) {
	l := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Sample("inter_token_gap", float64(i%1000))
	}
}

func BenchmarkSampleContended(b *testing.B) {
	l := New()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.Sample("inter_token_gap", float64(i%1000))
			i++
		}
	})
}

func BenchmarkPercentile100(b *testing.B) {
	l := New()
	for i := 0; i < 100; i++ {
		l.Sample("drift", float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Percentile("drift", 95)
	}
}

func BenchmarkPercentile1000(b *testing.B) {
	l := New()
	for i := 0; i < 1000; i++ {
		l.Sample("inter_token_gap", float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Percentile("inter_token_gap", 99)
	}
}

func BenchmarkPercentile10000(b *testing.B) {
	l := New()
	for i := 0; i < 10000; i++ {
		l.Sample("inter_token_gap", float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Percentile("inter_token_gap", 99)
	}
}

func BenchmarkSilentHangDeadline(b *testing.B) {
	l := New()
	for i := 0; i < 1000; i++ {
		l.Sample("inter_token_gap", float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.SilentHangDeadline(5)
	}
}

func BenchmarkDriftThreshold(b *testing.B) {
	l := New()
	for i := 0; i < 100; i++ {
		l.Sample("drift", float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.DriftThreshold()
	}
}

func BenchmarkClamp(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Clamp(float64(i), 100, 15000)
	}
}
