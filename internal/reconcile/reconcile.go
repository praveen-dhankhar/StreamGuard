package reconcile

import (
	"math"
	"time"

	"streamguard/internal/calibration"
	"streamguard/internal/ledger"
	"streamguard/internal/tokenizer"
)

type Job struct {
	Ledger       *ledger.Store
	Calibration  *calibration.Logger
	TokenizerReg *tokenizer.Registry
	ThresholdPct float64
}

func (j Job) Apply(apiKeyHash, provider, billingPeriod string, localTokens, providerTokens int, now time.Time) float64 {
	drift := 0.0
	if providerTokens > 0 {
		drift = math.Abs(float64(localTokens-providerTokens)) / float64(providerTokens) * 100
	}
	if j.Calibration != nil {
		j.Calibration.Sample("drift", drift)
	}
	flag := drift > j.ThresholdPct
	if j.Ledger != nil {
		j.Ledger.UpsertReconciliation(apiKeyHash, billingPeriod, now, flag)
	}
	if j.TokenizerReg != nil {
		j.TokenizerReg.Observe(provider, flag)
	}
	return drift
}
