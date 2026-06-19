package cascade

type Session struct {
	ID              string
	APIKeyHash      string
	TokensDelivered int
	Status          string
	Attempts        []ProviderAttempt
}

type ProviderAttempt struct {
	Provider                     string
	Outcome                      string
	TokensDeliveredBeforeFailure int
}
