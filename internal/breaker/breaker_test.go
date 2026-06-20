package breaker

import (
	"sync"
	"testing"
	"time"
)

func TestTryClaimProbeExclusive(t *testing.T) {
	br := New(Config{FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	br.ForceState(HalfOpen)

	const n = 100
	var wg sync.WaitGroup
	winners := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			winners <- br.TryClaimProbe()
		}()
	}
	wg.Wait()
	close(winners)

	count := 0
	for won := range winners {
		if won {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one half-open probe winner, got %d", count)
	}
}

func TestClosedOpenHalfOpenClosedTransition(t *testing.T) {
	now := time.Date(2026, 6, 19, 1, 0, 0, 0, time.UTC)
	br := New(Config{FailureThreshold: 2, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	br.now = func() time.Time { return now }

	br.RecordFailure()
	if got := br.State(); got != Closed {
		t.Fatalf("state after first failure = %s, want closed", got)
	}
	br.RecordFailure()
	if got := br.State(); got != Open {
		t.Fatalf("state after threshold failure = %s, want open", got)
	}
	now = now.Add(time.Second)
	if got := br.State(); got != HalfOpen {
		t.Fatalf("state after open timeout = %s, want half_open", got)
	}
	if !br.TryClaimProbe() {
		t.Fatal("expected half-open probe claim")
	}
	br.RecordSuccess()
	if got := br.State(); got != Closed {
		t.Fatalf("state after probe success = %s, want closed", got)
	}
}

func TestClosedOpenHalfOpenOpenTransition(t *testing.T) {
	now := time.Date(2026, 6, 19, 1, 0, 0, 0, time.UTC)
	br := New(Config{FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenSuccessThreshold: 1})
	br.now = func() time.Time { return now }

	br.RecordFailure()
	if got := br.State(); got != Open {
		t.Fatalf("state after failure = %s, want open", got)
	}
	now = now.Add(time.Second)
	if got := br.State(); got != HalfOpen {
		t.Fatalf("state after timeout = %s, want half_open", got)
	}
	if !br.TryClaimProbe() {
		t.Fatal("expected half-open probe claim")
	}
	br.RecordFailure()
	if got := br.State(); got != Open {
		t.Fatalf("state after probe failure = %s, want open", got)
	}
}
