package reconcile

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"streamguard/internal/calibration"
	"streamguard/internal/config"
	"streamguard/internal/ledger"
	"streamguard/internal/tokenizer"
)

type Job struct {
	Ledger       *ledger.Store
	Calibration  *calibration.Logger
	TokenizerReg *tokenizer.Registry
	ThresholdPct float64
	Fetcher      UsageFetcher
}

type UsageFetcher interface {
	FetchUsage(ctx context.Context, provider config.Provider, apiKeyHash, billingPeriod string) (int, error)
}

type HTTPUsageFetcher struct {
	Client *http.Client
}

type Result struct {
	APIKeyHash       string
	BillingPeriod    string
	Provider         string
	LocalTokens      int
	ProviderTokens   int
	DriftPct         float64
	DriftFlag        bool
	LastReconciledAt time.Time
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
		tokenizer.LogIfSuspected(provider, j.TokenizerReg.Observe(provider, flag))
	}
	return drift
}

func (j Job) Run(ctx context.Context, providers []config.Provider, entries []ledger.Entry, now time.Time) ([]Result, error) {
	if j.Fetcher == nil {
		return nil, nil
	}
	providerByName := make(map[string]config.Provider, len(providers))
	for _, provider := range providers {
		providerByName[strings.ToLower(provider.Name)] = provider
	}
	results := make([]Result, 0)
	for _, entry := range entries {
		if len(entry.ProviderTokens) == 0 {
			continue
		}
		flag := false
		for providerName, localTokens := range entry.ProviderTokens {
			provider, ok := providerByName[strings.ToLower(providerName)]
			if !ok {
				continue
			}
			providerTokens, err := j.Fetcher.FetchUsage(ctx, provider, entry.APIKeyHash, entry.BillingPeriod)
			if err != nil {
				return nil, err
			}
			drift := j.Apply(entry.APIKeyHash, providerName, entry.BillingPeriod, localTokens, providerTokens, now)
			driftFlag := drift > j.ThresholdPct
			flag = flag || driftFlag
			results = append(results, Result{
				APIKeyHash:       entry.APIKeyHash,
				BillingPeriod:    entry.BillingPeriod,
				Provider:         providerName,
				LocalTokens:      localTokens,
				ProviderTokens:   providerTokens,
				DriftPct:         drift,
				DriftFlag:        driftFlag,
				LastReconciledAt: now.UTC(),
			})
		}
		if j.Ledger != nil {
			j.Ledger.UpsertReconciliation(entry.APIKeyHash, entry.BillingPeriod, now, flag)
		}
	}
	return results, nil
}

func (f HTTPUsageFetcher) FetchUsage(ctx context.Context, provider config.Provider, apiKeyHash, billingPeriod string) (int, error) {
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	base := strings.TrimRight(provider.BaseURL, "/") + "/usage"
	query := url.Values{}
	query.Set("period", billingPeriod)
	query.Set("key", apiKeyHash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"?"+query.Encode(), nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var payload struct {
		Tokens int `json:"tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}
	return payload.Tokens, nil
}

type Runner struct {
	Job       Job
	Providers []config.Provider
	Interval  time.Duration
	Now       func() time.Time
}

func (r Runner) RunOnce(ctx context.Context) ([]Result, error) {
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	return r.Job.Run(ctx, r.Providers, r.Job.Ledger.Entries(), now)
}
