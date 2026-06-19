package cascade

import "time"

type Status string

const (
	StatusStreaming Status = "streaming"
	StatusFailover  Status = "failover"
	StatusTruncated Status = "truncated"
	StatusComplete  Status = "complete"
	StatusShutdown  Status = "shutdown"
)

type Session struct {
	ID                 string
	APIKeyHash         string
	TokensDelivered    int
	Status             Status
	ProviderAttempts   []ProviderAttempt
	FinalAttemptTokens int
	FinalProvider      string
}

type ProviderAttempt struct {
	Provider                     string
	StartedAt                    time.Time
	EndedAt                      time.Time
	Outcome                      string
	TokensDeliveredBeforeFailure int
}

func NewSession(id, apiKeyHash string) *Session {
	return &Session{
		ID:               id,
		APIKeyHash:       apiKeyHash,
		Status:           StatusStreaming,
		ProviderAttempts: make([]ProviderAttempt, 0, 2),
	}
}

func (s *Session) StartAttempt(provider string, now time.Time) int {
	s.ProviderAttempts = append(s.ProviderAttempts, ProviderAttempt{
		Provider:  provider,
		StartedAt: now.UTC(),
	})
	return len(s.ProviderAttempts) - 1
}

func (s *Session) FinishAttempt(index int, outcome string, tokensBeforeFailure int, endedAt time.Time) {
	if index < 0 || index >= len(s.ProviderAttempts) {
		return
	}
	s.ProviderAttempts[index].Outcome = outcome
	s.ProviderAttempts[index].EndedAt = endedAt.UTC()
	s.ProviderAttempts[index].TokensDeliveredBeforeFailure = tokensBeforeFailure
}
