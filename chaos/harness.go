//go:build chaos_enabled

package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"streamguard/internal/auth"
	"streamguard/internal/config"
	"streamguard/internal/mockupstream"
	"streamguard/internal/reconcile"
	"streamguard/internal/server"
)

func Enabled() error {
	if os.Getenv("STREAMGUARD_CHAOS_ENABLED") != "true" {
		return errors.New("chaos harness requires STREAMGUARD_CHAOS_ENABLED=true")
	}
	return nil
}

type Config struct {
	Streams            int
	Seed               int64
	ReconciliationRuns int
}

type Result struct {
	Streams              int
	ExpectedBilled       int
	ActualBilled         int
	TruncatedRequests    int
	Failures             map[string]int
	InterTokenGapSamples int
	DriftSamples         int
	SilentHangReady      bool
	DriftReady           bool
	SilentHangRawMS      float64
	SilentHangFinalMS    float64
	SilentHangClamped    bool
	DriftRawPct          float64
	DriftFinalPct        float64
	DriftClamped         bool
}

type scenario struct {
	model          string
	expectedBilled int
	truncated      bool
	failures       []string
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := Enabled(); err != nil {
		return Result{}, err
	}
	if cfg.Streams < 50 {
		cfg.Streams = 50
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	if cfg.ReconciliationRuns <= 0 {
		cfg.ReconciliationRuns = 60
	}

	scenarios := []scenario{
		{model: "sg-success", expectedBilled: 10},
		{model: "sg-dead-socket", expectedBilled: 6, failures: []string{"dead_socket"}},
		{model: "sg-silent-hang", expectedBilled: 8, failures: []string{"silent_hang"}},
		{model: "sg-malformed", expectedBilled: 7, failures: []string{"malformed"}},
		{model: "sg-exhausted", expectedBilled: 6, truncated: true, failures: []string{"dead_socket"}},
	}

	primary := mockupstream.New(mockupstream.Options{
		Provider: "openai",
		DelayMin: 2 * time.Millisecond,
		DelayMax: 4 * time.Millisecond,
		Tokens:   tokens(10),
		PerModel: map[string]mockupstream.Options{
			"sg-success":     {Tokens: tokens(10)},
			"sg-dead-socket": {Tokens: tokens(10), Failure: mockupstream.FailureDeadSocket, FailAfterTokens: 4},
			"sg-silent-hang": {Tokens: tokens(10), Failure: mockupstream.FailureSilentHang, FailAfterTokens: 5},
			"sg-malformed":   {Tokens: tokens(10), Failure: mockupstream.FailureMalformed, FailAfterTokens: 3},
			"sg-exhausted":   {Tokens: tokens(10), Failure: mockupstream.FailureDeadSocket, FailAfterTokens: 4},
		},
	})
	defer primary.Close()

	secondary := mockupstream.New(mockupstream.Options{
		Provider:       "anthropic",
		DelayMin:       2 * time.Millisecond,
		DelayMax:       4 * time.Millisecond,
		Tokens:         tokens(10),
		UsageOffsetPct: 25,
		PerModel: map[string]mockupstream.Options{
			"sg-success":     {Tokens: tokens(10)},
			"sg-dead-socket": {Tokens: tokens(6)},
			"sg-silent-hang": {Tokens: tokens(8)},
			"sg-malformed":   {Tokens: tokens(7)},
			"sg-exhausted":   {Tokens: tokens(6), Failure: mockupstream.FailureMalformed, FailAfterTokens: 6},
		},
	})
	defer secondary.Close()

	key := "sg_live_demo"
	keys := auth.NewStore(24 * time.Hour)
	if err := keys.Add(key, []string{"openai", "anthropic"}, 100000, 24*time.Hour); err != nil {
		return Result{}, err
	}
	sgCfg := config.Defaults()
	sgCfg.CircuitBreaker.FailureThreshold = 1000
	sgCfg.Timeouts.SilentHangDeadlineMS = 15
	sgCfg.Providers = []config.Provider{
		{Name: "openai", Priority: 0, BaseURL: primary.URL},
		{Name: "anthropic", Priority: 1, BaseURL: secondary.URL},
	}
	srv := server.New(sgCfg, keys)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	random := rand.New(rand.NewSource(cfg.Seed))
	selections := make([]scenario, cfg.Streams)
	for i := range selections {
		selections[i] = scenarios[random.Intn(len(scenarios))]
	}

	var expectedBilled atomic.Int64
	failures := map[string]int{}
	var failuresMu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, cfg.Streams)

	for _, selected := range selections {
		selected := selected
		expectedBilled.Add(int64(selected.expectedBilled))
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := invokeStream(ctx, ts.URL, key, selected.model)
			if err != nil {
				errs <- err
				return
			}
			for _, failure := range []string{"dead_socket", "silent_hang", "malformed"} {
				if strings.Contains(body, `"reason":"`+failure+`"`) {
					failuresMu.Lock()
					failures[failure]++
					failuresMu.Unlock()
				}
			}
			if selected.truncated && !strings.Contains(body, "gateway_truncated") {
				errs <- errors.New("missing gateway_truncated for model " + selected.model)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return Result{}, err
		}
	}

	runner := reconcile.Runner{
		Job: reconcile.Job{
			Ledger:       srv.Ledger(),
			Calibration:  srv.Calibration(),
			TokenizerReg: srv.TokenizerRegistry(),
			ThresholdPct: sgCfg.Reconciliation.DriftThresholdPct,
			Fetcher: reconcile.HTTPUsageFetcher{
				Client: &http.Client{Timeout: 2 * time.Second},
			},
		},
		Providers: sgCfg.Providers,
	}
	for i := 0; i < cfg.ReconciliationRuns; i++ {
		if _, err := runner.RunOnce(ctx); err != nil {
			return Result{}, err
		}
	}

	sum := srv.Ledger().Summary(auth.HashKey(key), auth.Redact(key))
	silentHang, _ := srv.Calibration().SilentHangDeadline(5)
	drift, _ := srv.Calibration().DriftThreshold()
	return Result{
		Streams:              cfg.Streams,
		ExpectedBilled:       int(expectedBilled.Load()),
		ActualBilled:         sum.TokensBilled,
		TruncatedRequests:    sum.TruncatedRequests,
		Failures:             failures,
		InterTokenGapSamples: srv.Calibration().Count("inter_token_gap"),
		DriftSamples:         srv.Calibration().Count("drift"),
		SilentHangReady:      srv.Calibration().ReadyForSilentHangCalibration(),
		DriftReady:           srv.Calibration().ReadyForDriftCalibration(),
		SilentHangRawMS:      silentHang.Raw,
		SilentHangFinalMS:    silentHang.Final,
		SilentHangClamped:    silentHang.Clamped,
		DriftRawPct:          drift.Raw,
		DriftFinalPct:        drift.Final,
		DriftClamped:         drift.Clamped,
	}, nil
}

func invokeStream(ctx context.Context, baseURL, apiKey, model string) (string, error) {
	payload := map[string]any{
		"model":  model,
		"stream": true,
		"messages": []map[string]string{{
			"role": "user", "content": "chaos",
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/stream", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", errors.New(string(data))
	}
	return string(data), nil
}

func tokens(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "x "
	}
	return out
}
