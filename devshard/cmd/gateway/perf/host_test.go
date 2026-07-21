package perf

import (
	"testing"
	"time"
)

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

func TestHostPerfEstimateReturnsColdPriorBeforeAnySample(t *testing.T) {
	h := newHostPerf(10*time.Minute, 200, 3)
	got := h.estimate(100, testEpoch)
	want := 200 + 3*100.0
	if got != want {
		t.Fatalf("estimate() before any sample = %v, want %v (cold priors)", got, want)
	}
}

func TestHostPerfRecordSampleRaisesEstimateOnSlowSample(t *testing.T) {
	h := newHostPerf(10*time.Minute, 200, 0)
	slow := Sample{Responsive: true, SendTime: testEpoch, ReceiptTime: testEpoch.Add(900 * time.Millisecond)}

	h.recordSample(slow, testEpoch)

	if got := h.estimate(0, testEpoch); got != 900 {
		t.Fatalf("estimate() after a slow sample = %v, want 900 (peak snap-up from cold prior 200)", got)
	}
}

func TestHostPerfRecordSampleDecaysGraduallyAfterFastSample(t *testing.T) {
	h := newHostPerf(10*time.Minute, 200, 0)
	slow := Sample{Responsive: true, SendTime: testEpoch, ReceiptTime: testEpoch.Add(900 * time.Millisecond)}
	h.recordSample(slow, testEpoch)

	fastTime := testEpoch.Add(time.Second) // close in time relative to the 10-minute half-life
	fast := Sample{Responsive: true, SendTime: fastTime, ReceiptTime: fastTime.Add(100 * time.Millisecond)}
	h.recordSample(fast, fastTime)

	got := h.estimate(0, fastTime)
	if got <= 100 {
		t.Fatalf("estimate() after one fast sample = %v, want > 100 (must not drop straight to the fast value)", got)
	}
	if got >= 900 {
		t.Fatalf("estimate() after one fast sample = %v, want < 900 (must have moved down from the slow value)", got)
	}
}

func TestHostPerfEstimateCombinesReceiptAndCTTFLPerToken(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 0)
	s := Sample{
		Responsive:  true,
		SendTime:    testEpoch,
		ReceiptTime: testEpoch.Add(200 * time.Millisecond),
		FirstToken:  testEpoch.Add(240 * time.Millisecond), // 40ms gap
		InputTokens: 20,                                    // cttfl = 40/20 = 2ms/token
	}
	h.recordSample(s, testEpoch)

	got := h.estimate(10, testEpoch)
	want := 200 + 2*10.0
	if got != want {
		t.Fatalf("estimate(10, ...) = %v, want %v (receipt 200 + cttfl 2/token * 10 tokens)", got, want)
	}
}

func TestHostPerfRecordSampleIgnoresNonPositiveReceiptMs(t *testing.T) {
	h := newHostPerf(10*time.Minute, 500, 0)
	invalid := Sample{Responsive: true} // zero SendTime/ReceiptTime -> ReceiptMs() == 0

	h.recordSample(invalid, testEpoch)

	if got := h.estimate(0, testEpoch); got != 500 {
		t.Fatalf("estimate() after a zero-receipt sample = %v, want 500 (prior left untouched)", got)
	}
}

func TestHostPerfRecordSampleIgnoresNonPositiveCTTFL(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 7)
	// Valid receipt but no FirstToken -> CTTFL() == 0, must not touch ewmaCTTFL.
	s := Sample{Responsive: true, SendTime: testEpoch, ReceiptTime: testEpoch.Add(50 * time.Millisecond)}

	h.recordSample(s, testEpoch)

	got := h.estimate(10, testEpoch)
	want := 50 + 7*10.0
	if got != want {
		t.Fatalf("estimate(10, ...) = %v, want %v (cttfl prior 7 left untouched)", got, want)
	}
}

func TestHostPerfConsecutiveFailIncrementsOnEachNonResponsiveSample(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 0)
	fail := Sample{Responsive: false}

	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch.Add(time.Second))
	h.recordSample(fail, testEpoch.Add(2*time.Second))

	if h.consecutiveFail != 3 {
		t.Fatalf("consecutiveFail after 3 failures = %d, want 3", h.consecutiveFail)
	}
}

func TestHostPerfConsecutiveFailResetsToZeroOnResponsiveSample(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 0)
	fail := Sample{Responsive: false}
	success := Sample{Responsive: true, SendTime: testEpoch, ReceiptTime: testEpoch.Add(time.Millisecond)}

	h.recordSample(fail, testEpoch)
	h.recordSample(fail, testEpoch.Add(time.Second))
	h.recordSample(success, testEpoch.Add(2*time.Second))

	if h.consecutiveFail != 0 {
		t.Fatalf("consecutiveFail after a responsive sample = %d, want 0 (reset)", h.consecutiveFail)
	}
}

func TestHostPerfRecordSampleUpdatesLastSeenEvenOnFailure(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 0)
	failTime := testEpoch.Add(5 * time.Minute)

	h.recordSample(Sample{Responsive: false}, failTime)

	if !h.lastSeen.Equal(failTime) {
		t.Fatalf("lastSeen after a failing sample = %v, want %v", h.lastSeen, failTime)
	}
}

func TestHostPerfFailureRateZeroVolumeReturnsZeroRateNotNaN(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 0)

	rate, volume := h.failureRate(testEpoch)

	if rate != 0 || volume != 0 {
		t.Fatalf("failureRate() with no samples = (%v, %v), want (0, 0)", rate, volume)
	}
}

func TestHostPerfFailureRateAllFailuresAtSameInstant(t *testing.T) {
	h := newHostPerf(10*time.Minute, 0, 0)
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
	h := newHostPerf(halfLife, 0, 0)
	fail := Sample{Responsive: false}
	success := Sample{Responsive: true, SendTime: testEpoch, ReceiptTime: testEpoch.Add(time.Millisecond)}

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
