package calibration

import "testing"

func TestSilentHangDeadlineUsesP99AndClamp(t *testing.T) {
	log := New()
	for i := 1; i <= 1000; i++ {
		log.Sample("inter_token_gap", float64(i))
	}

	got, ok := log.SilentHangDeadline(5)
	if !ok {
		t.Fatal("expected silent hang calibration result")
	}
	if !log.ReadyForSilentHangCalibration() {
		t.Fatal("expected silent hang calibration gate to be ready")
	}
	if got.Raw != 4950 {
		t.Fatalf("raw = %.0f, want 4950", got.Raw)
	}
	if got.Final != 4950 || got.Clamped {
		t.Fatalf("unexpected final=%v clamped=%v", got.Final, got.Clamped)
	}
}

func TestDriftThresholdUsesP95AndClamp(t *testing.T) {
	log := New()
	for i := 1; i <= 100; i++ {
		log.Sample("drift", float64(i))
	}

	got, ok := log.DriftThreshold()
	if !ok {
		t.Fatal("expected drift calibration result")
	}
	if !log.ReadyForDriftCalibration() {
		t.Fatal("expected drift calibration gate to be ready")
	}
	if got.Raw != 95 {
		t.Fatalf("raw = %.0f, want 95", got.Raw)
	}
	if got.Final != 25 || !got.Clamped {
		t.Fatalf("unexpected final=%v clamped=%v", got.Final, got.Clamped)
	}
}
