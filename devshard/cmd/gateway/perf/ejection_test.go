package perf

import (
	"testing"
	"time"
)

func recordFailures(h *hostPerf, count int, at time.Time) {
	for range count {
		h.recordSample(Sample{Responsive: false}, at)
	}
}

func TestEjectionPolicyEvaluateConsecutiveFailThresholdEjects(t *testing.T) {
	policy := newEjectionPolicy(3, 0.99, 1e9, time.Minute, 10*time.Minute)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 3, testEpoch)

	policy.evaluate(h, state, testEpoch)

	if !state.ejected(testEpoch) {
		t.Fatalf("ejected() after %d consecutive failures (threshold 3) = false, want true", h.consecutiveFail)
	}
}

func TestEjectionPolicyEvaluateBelowConsecutiveFailThresholdDoesNotEject(t *testing.T) {
	policy := newEjectionPolicy(3, 0.99, 1e9, time.Minute, 10*time.Minute)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 2, testEpoch)

	policy.evaluate(h, state, testEpoch)

	if state.ejected(testEpoch) {
		t.Fatalf("ejected() after 2 consecutive failures (threshold 3) = true, want false")
	}
	if state.ejectionCount != 0 {
		t.Fatalf("ejectionCount after a non-trigger = %d, want 0", state.ejectionCount)
	}
}

// The key Envoy property: refuse to judge a host on a handful of requests,
// no matter how bad the rate looks.
func TestEjectionPolicyEvaluateFailureRateBelowMinVolumeNeverEjects(t *testing.T) {
	policy := newEjectionPolicy(1000, 0.5, 5, time.Minute, 10*time.Minute)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 3, testEpoch) // 100% failure, but volume 3 < minVolume 5

	policy.evaluate(h, state, testEpoch)

	if state.ejected(testEpoch) {
		t.Fatalf("ejected() at 100%% failure but volume 3 < minVolume 5 = true, want false")
	}
}

func TestEjectionPolicyEvaluateFailureRateAboveMinVolumeAndThresholdEjects(t *testing.T) {
	policy := newEjectionPolicy(1000, 0.5, 5, time.Minute, 10*time.Minute)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 5, testEpoch) // 100% failure, volume 5 >= minVolume 5

	policy.evaluate(h, state, testEpoch)

	if !state.ejected(testEpoch) {
		t.Fatalf("ejected() at 100%% failure with volume 5 >= minVolume 5 = false, want true")
	}
}

func TestEjectionPolicyEvaluateBackoffLadderDoublesThenCaps(t *testing.T) {
	const base = 100 * time.Second
	const max = 250 * time.Second
	policy := newEjectionPolicy(1, 0.99, 1e9, base, max)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 1, testEpoch) // consecutiveFail=1, never reset in this test

	step1 := testEpoch
	policy.evaluate(h, state, step1)
	if want := step1.Add(base); !state.ejectedUntil.Equal(want) || state.ejectionCount != 1 {
		t.Fatalf("after 1st ejection: ejectedUntil=%v count=%d, want %v count=1", state.ejectedUntil, state.ejectionCount, want)
	}

	step2 := state.ejectedUntil // exactly when the timer expires -> re-triggers
	policy.evaluate(h, state, step2)
	if want := step2.Add(2 * base); !state.ejectedUntil.Equal(want) || state.ejectionCount != 2 {
		t.Fatalf("after 2nd ejection: ejectedUntil=%v count=%d, want %v count=2", state.ejectedUntil, state.ejectionCount, want)
	}

	step3 := state.ejectedUntil
	policy.evaluate(h, state, step3)
	if want := step3.Add(max); !state.ejectedUntil.Equal(want) || state.ejectionCount != 3 { // 3*base=300s > max, so clamped
		t.Fatalf("after 3rd ejection: ejectedUntil=%v count=%d, want %v count=3 (capped at max)", state.ejectedUntil, state.ejectionCount, want)
	}
}

func TestEjectionPolicyEvaluateDoesNotReExtendWhileAlreadyEjected(t *testing.T) {
	const base = 100 * time.Second
	policy := newEjectionPolicy(1, 0.99, 1e9, base, 250*time.Second)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 1, testEpoch)
	policy.evaluate(h, state, testEpoch)
	firstEjectedUntil := state.ejectedUntil

	midWindow := testEpoch.Add(50 * time.Second) // still inside the ejection window, still triggering
	policy.evaluate(h, state, midWindow)

	if !state.ejectedUntil.Equal(firstEjectedUntil) || state.ejectionCount != 1 {
		t.Fatalf("re-evaluate mid-window: ejectedUntil=%v count=%d, want unchanged %v count=1", state.ejectedUntil, state.ejectionCount, firstEjectedUntil)
	}
}

func TestEjectionStateEjectedFalseBeforeAnyEjection(t *testing.T) {
	state := &ejectionState{}
	if state.ejected(testEpoch) {
		t.Fatalf("ejected() on a zero-value state = true, want false")
	}
}

func TestEjectionStateEjectedRecoversWhenTimerExpires(t *testing.T) {
	state := &ejectionState{ejectedUntil: testEpoch.Add(30 * time.Second)}

	if !state.ejected(testEpoch.Add(29 * time.Second)) {
		t.Fatalf("ejected() 1s before expiry = false, want true")
	}
	if state.ejected(testEpoch.Add(30 * time.Second)) {
		t.Fatalf("ejected() exactly at expiry = true, want false (rejoined)")
	}
	if state.ejected(testEpoch.Add(31 * time.Second)) {
		t.Fatalf("ejected() 1s after expiry = true, want false (rejoined)")
	}
}

func TestEjectionPolicyEvaluateDoesNotDecayBeforeHealthyWindowElapses(t *testing.T) {
	const base = 100 * time.Second
	const window = 200 * time.Second
	policy := newEjectionPolicy(1, 0.99, 1e9, base, window)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 1, testEpoch)
	policy.evaluate(h, state, testEpoch)

	recoverTime := state.ejectedUntil.Add(time.Millisecond)
	success := Sample{Responsive: true}
	h.recordSample(success, recoverTime) // resets consecutiveFail so evaluate stops re-triggering
	policy.evaluate(h, state, recoverTime)

	if state.ejected(recoverTime) {
		t.Fatalf("ejected() just after timer expiry = true, want false (host rejoins immediately)")
	}
	if state.ejectionCount != 1 {
		t.Fatalf("ejectionCount 1ms after recovery = %d, want 1 (healthy window not elapsed yet)", state.ejectionCount)
	}
}

func TestEjectionPolicyEvaluateDecaysEjectionCountAndResetsLadderAfterHealthyWindow(t *testing.T) {
	const base = 100 * time.Second
	const window = 200 * time.Second
	policy := newEjectionPolicy(1, 0.99, 1e9, base, window)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 1, testEpoch)
	policy.evaluate(h, state, testEpoch)
	firstEjectedUntil := state.ejectedUntil

	recoverTime := firstEjectedUntil.Add(time.Millisecond)
	success := Sample{Responsive: true}
	h.recordSample(success, recoverTime) // host healthy from here on

	pastWindow := firstEjectedUntil.Add(window + time.Second)
	policy.evaluate(h, state, pastWindow)
	if state.ejectionCount != 0 {
		t.Fatalf("ejectionCount after a full healthy window = %d, want 0 (relaxed)", state.ejectionCount)
	}

	recordFailures(h, 1, pastWindow) // a fresh single failure re-triggers
	policy.evaluate(h, state, pastWindow)
	if want := pastWindow.Add(base); !state.ejectedUntil.Equal(want) || state.ejectionCount != 1 {
		t.Fatalf("re-ejection after decay: ejectedUntil=%v count=%d, want %v count=1 (base, not punished at the old multiplier)", state.ejectedUntil, state.ejectionCount, want)
	}
}

func TestEjectionPolicyEvaluateDoesNotReEjectOnFirstSuccessAfterRateTrigger(t *testing.T) {
	policy := newEjectionPolicy(1000, 0.5, 20, 30*time.Second, 300*time.Second)
	h := newHostPerf(10 * time.Minute)
	state := &ejectionState{}
	recordFailures(h, 25, testEpoch) // volume 25 >= minVolume 20, rate 100% >= 0.5

	policy.evaluate(h, state, testEpoch)
	if !state.ejected(testEpoch) {
		t.Fatal("ejected() right after the rate trigger = false, want true")
	}

	rejoinTime := state.ejectedUntil.Add(time.Millisecond)
	success := Sample{Responsive: true}
	h.recordSample(success, rejoinTime)

	policy.evaluate(h, state, rejoinTime)
	if state.ejected(rejoinTime) {
		t.Fatal("ejected() after one success right after rejoin = true, want false (stale decayed failures must not re-trigger the rate check)")
	}
}
