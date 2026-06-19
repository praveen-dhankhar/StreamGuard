package budget

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestTryReserveConcurrentBoundary(t *testing.T) {
	rec := &APIKeyRecord{TokenBudget: 100}
	const goroutines = 64
	var reserved int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rec.TryReserve(3) {
				atomic.AddInt64(&reserved, 3)
			}
		}()
	}
	wg.Wait()
	if reserved > rec.TokenBudget {
		t.Fatalf("reserved %d over budget %d", reserved, rec.TokenBudget)
	}
	if rec.TokensUsed != reserved {
		t.Fatalf("tokens used %d != reserved %d", rec.TokensUsed, reserved)
	}
}
