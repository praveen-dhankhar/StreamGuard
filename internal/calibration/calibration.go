package calibration

import (
	"math"
	"sort"
	"sync"
)

type Logger struct {
	mu      sync.Mutex
	samples map[string][]float64
}

type DerivedValue struct {
	Raw         float64
	Final       float64
	Clamped     bool
	SampleCount int
}

func New() *Logger {
	return &Logger{samples: make(map[string][]float64)}
}

func (l *Logger) Sample(kind string, value float64) {
	if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.samples[kind] = append(l.samples[kind], value)
}

func (l *Logger) Count(kind string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.samples[kind])
}

func (l *Logger) Percentile(kind string, pct float64) (float64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	values := append([]float64(nil), l.samples[kind]...)
	if len(values) == 0 {
		return 0, false
	}
	sort.Float64s(values)
	if pct <= 0 {
		return values[0], true
	}
	if pct >= 100 {
		return values[len(values)-1], true
	}
	idx := int(math.Ceil((pct/100)*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx], true
}

func Clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func (l *Logger) SilentHangDeadline(multiplier float64) (DerivedValue, bool) {
	if multiplier <= 0 {
		multiplier = 5
	}
	p99, ok := l.Percentile("inter_token_gap", 99)
	if !ok {
		return DerivedValue{}, false
	}
	raw := p99 * multiplier
	final := Clamp(raw, 1000, 15000)
	return DerivedValue{
		Raw:         raw,
		Final:       final,
		Clamped:     raw != final,
		SampleCount: l.Count("inter_token_gap"),
	}, true
}

func (l *Logger) DriftThreshold() (DerivedValue, bool) {
	p95, ok := l.Percentile("drift", 95)
	if !ok {
		return DerivedValue{}, false
	}
	final := Clamp(p95, 1, 25)
	return DerivedValue{
		Raw:         p95,
		Final:       final,
		Clamped:     p95 != final,
		SampleCount: l.Count("drift"),
	}, true
}

func (l *Logger) ReadyForSilentHangCalibration() bool {
	return l.Count("inter_token_gap") >= 1000
}

func (l *Logger) ReadyForDriftCalibration() bool {
	return l.Count("drift") >= 100
}
