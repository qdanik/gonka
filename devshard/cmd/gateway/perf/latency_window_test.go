package perf

import (
	"testing"
	"time"
)

func TestLatencyWindow_SaysNothingUntilItHasAHistory(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for count := 1; count < latencyWindowMinimum; count++ {
		window.add(time.Second)
		if _, known := window.p75(latencyWindowMinimum); known {
			t.Fatalf("answered after %d samples, wants %d", count, latencyWindowMinimum)
		}
	}

	window.add(time.Second)
	if _, known := window.p75(latencyWindowMinimum); !known {
		t.Error("the window holds enough samples and must answer")
	}
}

func TestLatencyWindow_ReportsTheQuartileNotTheAverage(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for i := 1; i <= 20; i++ {
		window.add(time.Duration(i) * time.Second)
	}

	observed, known := window.p75(latencyWindowMinimum)
	if !known {
		t.Fatal("the window must answer")
	}
	if observed != 16*time.Second {
		t.Errorf("p75 = %v, want 16s", observed)
	}
}

func TestLatencyWindow_ForgetsWhatFellOutOfTheRing(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for range latencyWindowSize {
		window.add(5 * time.Minute)
	}
	for range latencyWindowSize {
		window.add(2 * time.Second)
	}

	observed, _ := window.p75(latencyWindowMinimum)
	if observed != 2*time.Second {
		t.Errorf("p75 = %v, want 2s once the slow history aged out", observed)
	}
}

func TestRecordSample_KeepsTheLatencyItWasGiven(t *testing.T) {
	t.Parallel()
	host := newHostPerf(time.Minute)

	for range latencyWindowMinimum {
		host.recordSample(Sample{Responsive: true, FirstContent: 4 * time.Second}, time.Now())
	}
	host.recordSample(Sample{Responsive: true}, time.Now())

	observed, known := host.firstContent.p75(latencyWindowMinimum)
	if !known || observed != 4*time.Second {
		t.Errorf("p75 = %v (known=%v), want 4s from the samples that carried one", observed, known)
	}
}

func TestLatencyWindow_SaysNothingAboutPressureUntilItHasAHistory(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for range latencyWindowMinimum - 1 {
		window.add(time.Second)
	}

	if pressure := window.pressure(); pressure != 0 {
		t.Errorf("pressure = %v, want 0: a window too young to name a quantile cannot name a baseline either", pressure)
	}
}

func TestLatencyWindow_ReportsAHostAtItsOwnBestAsUnpressured(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for range latencyWindowSize {
		window.add(time.Second)
	}

	if pressure := window.pressure(); pressure != 1 {
		t.Errorf("pressure = %v, want 1: a host holding the latency it always held is not congested", pressure)
	}
}

func TestLatencyWindow_ReportsSlowingDownAgainstTheBestItHeld(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for range latencyWindowSize {
		window.add(time.Second)
	}
	for range latencyWindowSize {
		window.add(2 * time.Second)
	}

	if pressure := window.pressure(); pressure <= 1.5 {
		t.Errorf("pressure = %v, want above 1.5: a host now twice as slow as its best is congested before it has failed anything", pressure)
	}
}

// The baseline falls to a new best at once, so a host that genuinely got faster is not reported as congested.
func TestLatencyWindow_TakesANewBestImmediately(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for range latencyWindowSize {
		window.add(10 * time.Second)
	}
	for range latencyWindowSize {
		window.add(time.Second)
	}

	if pressure := window.pressure(); pressure != 1 {
		t.Errorf("pressure = %v, want 1: the best a host has held is the one it is measured against", pressure)
	}
}

// A lucky minimum must be forgotten, or one fast answer condemns the host to permanent congestion.
func TestLatencyWindow_ForgetsALuckyMinimumOverTime(t *testing.T) {
	t.Parallel()
	var window latencyWindow

	for range latencyWindowSize {
		window.add(time.Second)
	}
	for range latencyWindowSize {
		window.add(2 * time.Second)
	}
	settled := window.pressure()

	for range 20 * latencyWindowSize {
		window.add(2 * time.Second)
	}

	if drifted := window.pressure(); !(drifted < settled) {
		t.Errorf("pressure = %v after a long run at the new latency, want below the %v it started at: the baseline rises towards what the host now holds", drifted, settled)
	}
}
