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
