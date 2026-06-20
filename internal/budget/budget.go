package budget

import (
	"sync/atomic"
	"time"
)

type APIKeyRecord struct {
	KeyHash           string
	ProviderAllowlist []string
	TokenBudget       int64
	TokensUsed        int64
	BudgetPeriod      time.Duration
	BudgetPeriodStart time.Time
}

func (k *APIKeyRecord) TryReserve(n int64) bool {
	if n < 0 {
		return false
	}
	for {
		used := atomic.LoadInt64(&k.TokensUsed)
		if used+n > k.TokenBudget {
			return false
		}
		if atomic.CompareAndSwapInt64(&k.TokensUsed, used, used+n) {
			return true
		}
	}
}

func (k *APIKeyRecord) Release(n int64) {
	if n <= 0 {
		return
	}
	for {
		used := atomic.LoadInt64(&k.TokensUsed)
		next := used - n
		if next < 0 {
			next = 0
		}
		if atomic.CompareAndSwapInt64(&k.TokensUsed, used, next) {
			return
		}
	}
}

func (k *APIKeyRecord) Exhausted() bool {
	return atomic.LoadInt64(&k.TokensUsed) >= k.TokenBudget
}

func (k *APIKeyRecord) Allows(provider string) bool {
	for _, allowed := range k.ProviderAllowlist {
		if allowed == provider {
			return true
		}
	}
	return false
}
