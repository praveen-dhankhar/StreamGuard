//go:build chaos_enabled

package chaos

import (
	"context"
	"testing"
)

func TestHarnessRunsConcurrentChaosLoad(t *testing.T) {
	t.Setenv("STREAMGUARD_CHAOS_ENABLED", "true")

	result, err := Run(context.Background(), Config{
		Streams:            120,
		Seed:               7,
		ReconciliationRuns: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpectedBilled != result.ActualBilled {
		t.Fatalf("expected billed %d != actual billed %d", result.ExpectedBilled, result.ActualBilled)
	}
	if result.Failures["dead_socket"] == 0 || result.Failures["silent_hang"] == 0 || result.Failures["malformed"] == 0 {
		t.Fatalf("expected all failure modes to be exercised, got %+v", result.Failures)
	}
	if result.InterTokenGapSamples < 1000 {
		t.Fatalf("expected at least 1000 inter-token-gap samples, got %d", result.InterTokenGapSamples)
	}
	if result.DriftSamples < 100 {
		t.Fatalf("expected at least 100 drift samples, got %d", result.DriftSamples)
	}
	if !result.SilentHangReady || !result.DriftReady {
		t.Fatalf("expected calibration gates to be ready, got %+v", result)
	}
	t.Logf("chaos_result=%+v", result)
}
