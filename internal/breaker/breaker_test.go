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
