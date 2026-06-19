package breaker

import (
	"sync"
	"time"
)

type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half_open"
)

type Config struct {
	FailureThreshold         int
	OpenTimeout              time.Duration
	HalfOpenSuccessThreshold int
}

type ProviderCircuitState struct {
	mu                  sync.RWMutex
	state               State
	consecutiveFailures int
	consecutiveSuccess  int
	lastStateChange     time.Time
	probing             bool
	cfg                 Config
	now                 func() time.Time
}

func New(cfg Config) *ProviderCircuitState {
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 3
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	if cfg.HalfOpenSuccessThreshold < 1 {
		cfg.HalfOpenSuccessThreshold = 1
	}
	return &ProviderCircuitState{
		state:           Closed,
		lastStateChange: time.Now().UTC(),
		cfg:             cfg,
		now:             func() time.Time { return time.Now().UTC() },
	}
}

func (s *ProviderCircuitState) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked()
	return s.state
}

func (s *ProviderCircuitState) ForceState(state State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.probing = false
	s.consecutiveFailures = 0
	s.consecutiveSuccess = 0
	s.lastStateChange = s.now()
}

func (s *ProviderCircuitState) TryClaimProbe() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked()
	if s.state != HalfOpen || s.probing {
		return false
	}
	s.probing = true
	return true
}

func (s *ProviderCircuitState) AllowAttempt() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked()
	switch s.state {
	case Closed:
		return true
	case Open:
		return false
	case HalfOpen:
		if s.probing {
			return false
		}
		s.probing = true
		return true
	default:
		return false
	}
}

func (s *ProviderCircuitState) RecordSuccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveFailures = 0
	if s.state == HalfOpen {
		s.consecutiveSuccess++
		s.probing = false
		if s.consecutiveSuccess >= s.cfg.HalfOpenSuccessThreshold {
			s.transitionLocked(Closed)
		}
		return
	}
	s.probing = false
}

func (s *ProviderCircuitState) RecordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probing = false
	s.consecutiveSuccess = 0
	if s.state == HalfOpen {
		s.transitionLocked(Open)
		return
	}
	s.consecutiveFailures++
	if s.consecutiveFailures >= s.cfg.FailureThreshold {
		s.transitionLocked(Open)
	}
}

func (s *ProviderCircuitState) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked()
	return map[string]any{
		"state":                s.state,
		"consecutive_failures": s.consecutiveFailures,
		"last_state_change":    s.lastStateChange.Format(time.RFC3339),
	}
}

func (s *ProviderCircuitState) refreshLocked() {
	if s.state == Open && s.now().Sub(s.lastStateChange) >= s.cfg.OpenTimeout {
		s.transitionLocked(HalfOpen)
	}
}

func (s *ProviderCircuitState) transitionLocked(state State) {
	s.state = state
	s.probing = false
	s.consecutiveSuccess = 0
	if state == Closed {
		s.consecutiveFailures = 0
	}
	s.lastStateChange = s.now()
}
