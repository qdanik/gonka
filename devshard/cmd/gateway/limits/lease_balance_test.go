package limits

import (
	"testing"
)

func inflightOutput(l *ParticipantLimiter, participant, model string) int64 {
	for _, window := range l.Snapshot() {
		if window.Participant == participant && window.Model == model {
			return window.InflightOutputTokens
		}
	}
	return -1
}

// The window's in-flight is the sum of what admission charged, so the lease has to give back exactly that.
// Releasing a different number is how the counter drifts away from the requests it is supposed to be counting,
// and a drifted counter refuses a host that is idle.
func TestALeaseGivesBackExactlyWhatItTook(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(testConfig(), fixedNow(testEpoch))
	cost := TokenCost{Input: 1_024, Output: 4_096}

	release, admission := limiter.Overdraft("participant-1", "model-a", cost)

	if admission != AdmissionOpen {
		t.Fatalf("Overdraft = %q, want the tokens taken", admission)
	}
	if got := inflightInput(limiter, "participant-1", "model-a"); got != cost.Input {
		t.Fatalf("in-flight input = %d, want the %d it was charged", got, cost.Input)
	}
	if got := inflightOutput(limiter, "participant-1", "model-a"); got != cost.Output {
		t.Fatalf("in-flight output = %d, want the %d it was charged", got, cost.Output)
	}

	release()
	release()

	if got := inflightInput(limiter, "participant-1", "model-a"); got != 0 {
		t.Fatalf("in-flight input = %d after release, want 0", got)
	}
	if got := inflightOutput(limiter, "participant-1", "model-a"); got != 0 {
		t.Fatalf("in-flight output = %d after release, want 0", got)
	}
}

// A window admits in reservations, so it must grow in reservations. Crediting the tokens an answer actually
// produced would earn a rung several times more slowly than the admission arithmetic implies, and earn nothing
// at all against a host that reports no usage.
func TestAWindowGrowsInTheCurrencyItCharges(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(testConfig(), fixedNow(testEpoch))
	charged := TokenCost{Input: 2, Output: 2}

	release, admission := limiter.Overdraft("participant-1", "model-a", charged)
	if admission != AdmissionOpen {
		t.Fatalf("Overdraft = %q, want the tokens taken", admission)
	}
	before := outputWindow(t, limiter, "participant-1", "model-a")
	release()
	limiter.OnResult(Result{Participant: "participant-1", Model: "model-a", Verdict: Success, Carried: charged})
	grown := outputWindow(t, limiter, "participant-1", "model-a")

	if grown <= before {
		t.Fatalf("output window %v -> %v, want a rung earned by what the window was charged", before, grown)
	}
}

func outputWindow(t *testing.T, l *ParticipantLimiter, participant, model string) float64 {
	t.Helper()
	for _, window := range l.Snapshot() {
		if window.Participant == participant && window.Model == model {
			return window.OutputWindowTokens
		}
	}
	t.Fatalf("no window for %s/%s", participant, model)
	return 0
}
