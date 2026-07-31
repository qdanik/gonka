package perf

import (
	"math"
	"testing"
	"time"
)

// testEpoch is a fixed reference instant; every test time is derived from it via Add.
var testEpoch = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

func almostEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) <= epsilon
}

func TestDecayedCounterValueBeforeAnyAddIsZero(t *testing.T) {
	c := newDecayedCounter(10 * time.Minute)
	if got := c.value(testEpoch); got != 0 {
		t.Fatalf("value() before any add = %v, want 0", got)
	}
}

func TestDecayedCounterAddAccumulatesAtSameInstant(t *testing.T) {
	c := newDecayedCounter(10 * time.Minute)
	c.add(testEpoch)
	c.add(testEpoch)
	c.add(testEpoch)
	if got := c.value(testEpoch); got != 3 {
		t.Fatalf("value() after 3 adds at the same instant = %v, want 3", got)
	}
}

func TestDecayedCounterValueAfterOneHalfLifeIsHalved(t *testing.T) {
	const halfLife = 10 * time.Minute
	c := newDecayedCounter(halfLife)
	c.add(testEpoch)
	got := c.value(testEpoch.Add(halfLife))
	if !almostEqual(got, 0.5, 1e-9) {
		t.Fatalf("value() one half-life after a single add = %v, want 0.5", got)
	}
}

func TestHostPerfConsecutiveFailIncrementsOnEachNonResponsiveSample(t *testing.T) {
	h := newHostPerf(10 * time.Minute)
	fail := Sample{Responsive: false}

	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch.Add(time.Second))
	h.recordSample(fail, testEpoch.Add(2*time.Second))

	if h.consecutiveFail != 3 {
		t.Fatalf("consecutiveFail after 3 failures = %d, want 3", h.consecutiveFail)
	}
}

func TestHostPerfConsecutiveFailResetsToZeroOnResponsiveSample(t *testing.T) {
	h := newHostPerf(10 * time.Minute)
	fail := Sample{Responsive: false}
	success := Sample{Responsive: true}

	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch.Add(time.Second))
	h.recordSample(success, testEpoch.Add(2*time.Second))

	if h.consecutiveFail != 0 {
		t.Fatalf("consecutiveFail after a responsive sample = %d, want 0 (reset)", h.consecutiveFail)
	}
}

func TestHostPerfRecordSampleUpdatesLastSeenEvenOnFailure(t *testing.T) {
	h := newHostPerf(10 * time.Minute)
	failTime := testEpoch.Add(5 * time.Minute)

	h.recordSample(Sample{Responsive: false}, failTime)

	if !h.lastSeen.Equal(failTime) {
		t.Fatalf("lastSeen after a failing sample = %v, want %v", h.lastSeen, failTime)
	}
}

func TestHostPerfFailureRateZeroVolumeReturnsZeroRateNotNaN(t *testing.T) {
	h := newHostPerf(10 * time.Minute)

	rate, volume := h.failureRate(testEpoch)

	if rate != 0 || volume != 0 {
		t.Fatalf("failureRate() with no samples = (%v, %v), want (0, 0)", rate, volume)
	}
}

func TestHostPerfFailureRateAllFailuresAtSameInstant(t *testing.T) {
	h := newHostPerf(10 * time.Minute)
	fail := Sample{Responsive: false}

	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch)

	rate, volume := h.failureRate(testEpoch)
	if rate != 1 {
		t.Fatalf("failureRate() with only failures = %v, want 1", rate)
	}
	if volume != 3 {
		t.Fatalf("failureRate() decayed volume with only failures = %v, want 3", volume)
	}
}

// TestHostPerfFailureRateDecaysAsSuccessesFollowFailures pins that both the
// rate and the decayed volume move as time passes and new outcomes land,
// rather than the volume resetting or the rate ignoring decay.
func TestHostPerfFailureRateDecaysAsSuccessesFollowFailures(t *testing.T) {
	const halfLife = 10 * time.Minute
	h := newHostPerf(halfLife)
	fail := Sample{Responsive: false}
	success := Sample{Responsive: true}

	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch)

	later := testEpoch.Add(halfLife)
	h.recordSample(success, later)

	rate, volume := h.failureRate(later)
	// fail decays to 3*0.5=1.5, success is a fresh 1 -> rate=1.5/2.5=0.6, volume=2.5.
	if !almostEqual(rate, 0.6, 1e-9) {
		t.Fatalf("failureRate() rate after decay = %v, want 0.6", rate)
	}
	if !almostEqual(volume, 2.5, 1e-9) {
		t.Fatalf("failureRate() decayed volume = %v, want 2.5", volume)
	}
}
