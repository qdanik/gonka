package limits

import (
	"context"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

func TestGatewayConfigFromLimits_MapsFieldsFromDefaults(t *testing.T) {
	t.Parallel()
	limits := config.Defaults().Limits

	got := GatewayConfigFromLimits(limits)

	if got.MaxConcurrent != limits.Concurrency.MaxRequests {
		t.Errorf("MaxConcurrent = %d, want %d", got.MaxConcurrent, limits.Concurrency.MaxRequests)
	}
	if got.MaxInputTokens != limits.MaxInputTokensInFlight {
		t.Errorf("MaxInputTokens = %d, want %d", got.MaxInputTokens, limits.MaxInputTokensInFlight)
	}
	if got.AcquireWait != 500*time.Millisecond {
		t.Errorf("AcquireWait = %v, want 500ms (default AcquireWaitMS=500)", got.AcquireWait)
	}
}

func TestGatewayConfigFromLimits_MapsPerModelOverrides(t *testing.T) {
	t.Parallel()
	maxConcurrent := int64(7)
	limits := config.Limits{
		ModelLimits: map[string]config.ModelLimits{
			"modelA": {MaxConcurrentRequests: &maxConcurrent},
			"modelB": {},
		},
	}

	got := GatewayConfigFromLimits(limits)

	overrideA, ok := got.ModelLimits["modelA"]
	if !ok {
		t.Fatal(`ModelLimits["modelA"] missing, want an entry`)
	}
	if overrideA.MaxConcurrent == nil || *overrideA.MaxConcurrent != 7 {
		t.Errorf("ModelLimits[modelA].MaxConcurrent = %v, want 7", overrideA.MaxConcurrent)
	}
	if overrideA.MaxInputTokens != nil {
		t.Errorf("ModelLimits[modelA].MaxInputTokens = %v, want nil (not configured)", overrideA.MaxInputTokens)
	}

	overrideB, ok := got.ModelLimits["modelB"]
	if !ok {
		t.Fatal(`ModelLimits["modelB"] missing, want an entry`)
	}
	if overrideB.MaxConcurrent != nil || overrideB.MaxInputTokens != nil {
		t.Errorf("ModelLimits[modelB] = %+v, want both nil (no overrides configured)", overrideB)
	}
}

func TestParticipantConfigFromLimits_MapsFieldsFromDefaults(t *testing.T) {
	t.Parallel()
	limits := config.Defaults().Limits

	got := ParticipantConfigFromLimits(limits)

	if got.InitialWindow != limits.AIMD.InitialWindow {
		t.Errorf("InitialWindow = %d, want %d", got.InitialWindow, limits.AIMD.InitialWindow)
	}
	if got.MaxWindow != 64 {
		t.Errorf("MaxWindow = %d, want 64", got.MaxWindow)
	}
	if got.TripThreshold != limits.Breaker.TripThreshold {
		t.Errorf("TripThreshold = %d, want %d", got.TripThreshold, limits.Breaker.TripThreshold)
	}
	if got.BaseOpen != 5*time.Second {
		t.Errorf("BaseOpen = %v, want 5s (default BaseOpenMS=5000)", got.BaseOpen)
	}
	if got.MaxOpen != 300*time.Second {
		t.Errorf("MaxOpen = %v, want 300s (default MaxOpenMS=300000)", got.MaxOpen)
	}
}

// Capacity's derived scale feeds GatewayLimiter's admission, and ParticipantLimiter (built from
// the same mapped config) independently gates a single host at its per-host window.
func TestCapacityGatewayParticipantLimiterComposeEndToEnd(t *testing.T) {
	t.Parallel()
	limits := config.Defaults().Limits
	gatewayLimiter := NewGatewayLimiter(GatewayConfigFromLimits(limits))
	participantLimiter := NewParticipantLimiter(ParticipantConfigFromLimits(limits), fixedNow(testEpoch))
	capacity := NewCapacity(nil)

	capacity.Update(chain.PhaseSnapshot{
		CurrentWeightsByModel: map[string]map[string]float64{
			"modelA": {"hostA": 60, "hostB": 20},
		},
		FullWeightsByModel: map[string]map[string]float64{
			"modelA": {"hostA": 80, "hostB": 20},
		},
	})

	scale := capacity.ScaleFactor("modelA", false)
	if scale != 0.8 {
		t.Fatalf("Capacity.ScaleFactor(modelA) = %v, want 0.8 (80 current / 100 full)", scale)
	}
	modelCapacity := ModelCapacity{ScaleFactor: scale}

	ctx := context.Background()
	if err := gatewayLimiter.AcquireForModel(ctx, "modelA", 128, modelCapacity); err != nil {
		t.Fatalf("GatewayLimiter.AcquireForModel under cap = %v, want nil", err)
	}
	gatewayLimiter.ReleaseForModel("modelA", 128)

	for i := int64(0); i < limits.AIMD.InitialWindow; i++ {
		if !participantLimiter.Acquire("hostA", "modelA") {
			t.Fatalf("ParticipantLimiter.Acquire call %d = false, want true (InitialWindow=%d)", i+1, limits.AIMD.InitialWindow)
		}
	}
	if participantLimiter.Acquire("hostA", "modelA") {
		t.Fatal("ParticipantLimiter.Acquire beyond InitialWindow = true, want false (per-host window must gate the host)")
	}
}
