package perf

import (
	"sync"
	"testing"
)

func TestInflightGaugeAcquireReleaseBalance(t *testing.T) {
	g := newInflightGauge()
	g.acquire("alice")
	g.acquire("alice")
	g.acquire("alice")

	g.release("alice")
	if got := g.count("alice"); got != 2 {
		t.Fatalf("count() after 3 acquires + 1 release = %d, want 2", got)
	}

	g.release("alice")
	if got := g.count("alice"); got != 1 {
		t.Fatalf("count() after 3 acquires + 2 releases = %d, want 1", got)
	}

	g.release("alice")
	if got := g.count("alice"); got != 0 {
		t.Fatalf("count() after 3 acquires + 3 releases = %d, want 0", got)
	}
	if got := len(g.counts); got != 0 {
		t.Fatalf("counts map len = %d, want 0 (key dropped once balanced)", got)
	}
}

func TestInflightGaugeReleaseNeverGoesNegative(t *testing.T) {
	tests := []struct {
		name     string
		acquires int
		releases int
	}{
		{name: "release with no prior acquire", acquires: 0, releases: 1},
		{name: "release past an already-balanced participant", acquires: 1, releases: 2},
		{name: "many extra releases past zero", acquires: 2, releases: 5},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			g := newInflightGauge()
			for range testCase.acquires {
				g.acquire("alice")
			}
			for range testCase.releases {
				g.release("alice")
			}
			if got := g.count("alice"); got != 0 {
				t.Fatalf("count() = %d, want 0 (never negative)", got)
			}
			if got := len(g.counts); got != 0 {
				t.Fatalf("counts map len = %d, want 0", got)
			}
		})
	}
}

func TestInflightGaugeCountUnknownParticipantIsZero(t *testing.T) {
	g := newInflightGauge()
	if got := g.count("nobody"); got != 0 {
		t.Fatalf("count() for unknown participant = %d, want 0", got)
	}
}

func TestInflightGaugeFreshParticipantMatchesDroppedParticipant(t *testing.T) {
	g := newInflightGauge()
	g.acquire("alice")
	g.release("alice") // back to 0, key must be dropped

	if got := g.count("alice"); got != g.count("bob") {
		t.Fatalf("count(alice, dropped) = %d, count(bob, never seen) = %d, want equal", got, g.count("bob"))
	}
}

func TestInflightGaugeTracksParticipantsIndependently(t *testing.T) {
	g := newInflightGauge()
	g.acquire("alice")
	g.acquire("alice")
	g.acquire("bob")

	if got := g.count("alice"); got != 2 {
		t.Fatalf("count(alice) = %d, want 2", got)
	}
	if got := g.count("bob"); got != 1 {
		t.Fatalf("count(bob) = %d, want 1", got)
	}

	g.release("alice")
	if got := g.count("bob"); got != 1 {
		t.Fatalf("count(bob) after releasing alice = %d, want unaffected 1", got)
	}
}

func TestInflightGaugeConcurrentAcquireReleaseSettlesAtZero(t *testing.T) {
	g := newInflightGauge()
	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			g.acquire("shared")
			g.release("shared")
		}()
	}
	wg.Wait()

	if got := g.count("shared"); got != 0 {
		t.Fatalf("count() after %d concurrent acquire+release pairs = %d, want 0", goroutines, got)
	}
	if got := len(g.counts); got != 0 {
		t.Fatalf("counts map len = %d, want 0 (key dropped)", got)
	}
}

// TestInflightGaugeConcurrentAcquireReachesExactTotalBeforeRelease uses a
// start/release barrier (no sleeps) to pin the exact in-flight peak under
// concurrency, not just the settled-to-zero end state.
func TestInflightGaugeConcurrentAcquireReachesExactTotalBeforeRelease(t *testing.T) {
	g := newInflightGauge()
	const goroutines = 200

	var acquired sync.WaitGroup
	acquired.Add(goroutines)
	release := make(chan struct{})

	var done sync.WaitGroup
	done.Add(goroutines)
	for range goroutines {
		go func() {
			defer done.Done()
			g.acquire("shared")
			acquired.Done()
			<-release
			g.release("shared")
		}()
	}

	acquired.Wait()
	if got := g.count("shared"); got != goroutines {
		t.Fatalf("count() after %d concurrent acquires (none released yet) = %d, want %d", goroutines, got, goroutines)
	}

	close(release)
	done.Wait()
	if got := g.count("shared"); got != 0 {
		t.Fatalf("count() after all releases settled = %d, want 0", got)
	}
}
