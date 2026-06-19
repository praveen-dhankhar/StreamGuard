package reconcile

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"streamguard/internal/calibration"
	"streamguard/internal/config"
	"streamguard/internal/ledger"
	"streamguard/internal/tokenizer"
)

type fakeFetcher struct {
	tokens map[string]int
}

func (f fakeFetcher) FetchUsage(_ context.Context, provider config.Provider, apiKeyHash, billingPeriod string) (int, error) {
	return f.tokens[strings.ToLower(provider.Name)+"|"+apiKeyHash+"|"+billingPeriod], nil
}

func TestRunSetsAndClearsDriftFlagIdempotently(t *testing.T) {
	now := time.Date(2026, 6, 19, 3, 0, 0, 0, time.UTC)
	store := ledger.New(time.Hour)
	store.RecordTerminal("hash", "openai", now, 100, false)
	cal := calibration.New()
	reg := tokenizer.NewRegistry()
	job := Job{
		Ledger:       store,
		Calibration:  cal,
		TokenizerReg: reg,
		ThresholdPct: 10,
		Fetcher: fakeFetcher{tokens: map[string]int{
			"openai|hash|" + ledger.BillingPeriod(now, time.Hour): 200,
		}},
	}
	providers := []config.Provider{{Name: "openai", BaseURL: "http://mock"}}

	results, err := job.Run(context.Background(), providers, store.Entries(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !results[0].DriftFlag {
		t.Fatalf("expected flagged reconciliation result, got %+v", results)
	}
	sum := store.Summary("hash", "sg_live_***")
	if !sum.DriftFlag {
		t.Fatalf("expected drift flag after first run: %+v", sum)
	}
	if cal.Count("drift") != 1 {
		t.Fatalf("expected one drift sample, got %d", cal.Count("drift"))
	}

	job.Fetcher = fakeFetcher{tokens: map[string]int{
		"openai|hash|" + ledger.BillingPeriod(now, time.Hour): 102,
	}}
	results, err = job.Run(context.Background(), providers, store.Entries(), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].DriftFlag {
		t.Fatalf("expected cleared reconciliation result, got %+v", results)
	}
	sum = store.Summary("hash", "sg_live_***")
	if sum.DriftFlag {
		t.Fatalf("expected drift flag to clear: %+v", sum)
	}
	if cal.Count("drift") != 2 {
		t.Fatalf("expected two drift samples, got %d", cal.Count("drift"))
	}
	entries := store.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected one ledger entry, got %d", len(entries))
	}
	if entries[0].TokensBilled != 100 {
		t.Fatalf("tokens billed mutated to %d", entries[0].TokensBilled)
	}
}

func TestApplyHandlesZeroProviderTokens(t *testing.T) {
	job := Job{
		Ledger:       ledger.New(time.Hour),
		Calibration:  calibration.New(),
		TokenizerReg: tokenizer.NewRegistry(),
		ThresholdPct: 4.2,
	}

	drift := job.Apply("hash", "openai", "period", 25, 0, time.Now().UTC())
	if drift != 0 {
		t.Fatalf("drift = %v, want 0", drift)
	}
	sum := job.Ledger.Summary("hash", "sg_live_***")
	if sum.DriftFlag {
		t.Fatalf("drift flag should remain clear: %+v", sum)
	}
}

func TestApplyLogsTokenizerDriftEscalation(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	reg := tokenizer.NewRegistry()
	job := Job{
		Ledger:       ledger.New(time.Hour),
		Calibration:  calibration.New(),
		TokenizerReg: reg,
		ThresholdPct: 1,
	}
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		job.Apply("hash", "openai", "period", 100, 10, now.Add(time.Duration(i)*time.Minute))
	}
	entry, ok := reg.Snapshot("openai")
	if !ok {
		t.Fatal("expected tokenizer registry entry")
	}
	if entry.ConsecutiveAboveThreshold != 3 {
		t.Fatalf("consecutive above threshold = %d, want 3", entry.ConsecutiveAboveThreshold)
	}
	if !strings.Contains(buf.String(), "tokenizer_drift_suspected provider=openai") {
		t.Fatalf("expected tokenizer drift escalation log, got %q", buf.String())
	}
}
