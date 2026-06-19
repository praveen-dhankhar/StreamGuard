package ratelimit

import (
	"sync"
	"time"
)

type Limiter struct {
	mu     sync.Mutex
	window time.Duration
	limit  int64
	byKey  map[string][]event
}

type event struct {
	at     time.Time
	tokens int64
}

func New(window time.Duration, limit int64) *Limiter {
	if window <= 0 {
		window = time.Minute
	}
	if limit <= 0 {
		limit = 1 << 60
	}
	return &Limiter{window: window, limit: limit, byKey: make(map[string][]event)}
}

func (l *Limiter) Admit(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	events, total := l.pruneLocked(key, now)
	l.byKey[key] = events
	return total < l.limit
}

func (l *Limiter) Add(key string, tokens int64, now time.Time) {
	if tokens <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	events, _ := l.pruneLocked(key, now)
	events = append(events, event{at: now, tokens: tokens})
	l.byKey[key] = events
}

func (l *Limiter) pruneLocked(key string, now time.Time) ([]event, int64) {
	cutoff := now.Add(-l.window)
	events := l.byKey[key]
	out := events[:0]
	var total int64
	for _, e := range events {
		if e.at.After(cutoff) {
			out = append(out, e)
			total += e.tokens
		}
	}
	return out, total
}
