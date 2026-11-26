package share

import (
	"math"
	"testing"
	"time"
)

type testClock struct{ at time.Time }

func (c *testClock) now() time.Time { return c.at }

func newTestLedger(clock *testClock) *Ledger {
	return New(clock.now, func(model string) time.Duration { return 600 * time.Second })
}

// Test flow:
//  1. Count every kind once for two hosts of one model and for one host of another model, with a different number of events per kind.
//  2. Assert each host's totals and decayed counts hold exactly its own events and the other model's host is untouched.
func TestLedgerCountsEveryKindPerHostAndModel(t *testing.T) {
	clock := &testClock{at: time.Unix(1_000, 0)}
	ledger := newTestLedger(clock)
	for kind := range Kind(kinds) {
		for range int(kind) + 1 {
			ledger.Count("model-a", "host-a", kind)
		}
		for range int(kind) + 2 {
			ledger.Count("model-a", "host-b", kind)
		}
		ledger.Count("model-b", "host-a", kind)
	}
	for kind := range Kind(kinds) {
		if got, want := ledger.Totals("model-a", "host-a").Of(kind), uint64(kind)+1; got != want {
			t.Errorf("host-a total of kind %d = %d, want %d", kind, got, want)
		}
		if got, want := ledger.Totals("model-a", "host-b").Of(kind), uint64(kind)+2; got != want {
			t.Errorf("host-b total of kind %d = %d, want %d", kind, got, want)
		}
		if got := ledger.Totals("model-b", "host-a").Of(kind); got != 1 {
			t.Errorf("model-b host-a total of kind %d = %d, want 1", kind, got)
		}
	}
	decayed := ledger.Decayed("model-a", "host-a")
	if decayed != (Decayed{Assigned: 1, Offered: 2, Served: 3, Failed: 4, Passed: 5, Excused: 6}) {
		t.Errorf("host-a decayed = %+v, want 1 to 6 in kind order", decayed)
	}
	if hosts := ledger.Hosts("model-a"); len(hosts) != 2 {
		t.Errorf("Hosts(model-a) = %v, want both hosts", hosts)
	}
}

// Test flow:
//  1. Count six decayed kinds and one total-only kind, then advance the clock by one half-life.
//  2. Assert every decayed count halved, the totals did not move, and a count after the wait adds one to the halved value.
func TestLedgerDecaysByTheHalfLifeAndKeepsTotals(t *testing.T) {
	clock := &testClock{at: time.Unix(1_000, 0)}
	ledger := newTestLedger(clock)
	for kind := range Kind(decayedKinds) {
		for range 8 {
			ledger.Count("model-a", "host-a", kind)
		}
	}
	ledger.Count("model-a", "host-a", VotesOwed)
	clock.at = clock.at.Add(600 * time.Second)
	decayed := ledger.Decayed("model-a", "host-a")
	for name, got := range map[string]float64{"assigned": decayed.Assigned, "offered": decayed.Offered, "served": decayed.Served, "failed": decayed.Failed, "passed": decayed.Passed, "excused": decayed.Excused} {
		if math.Abs(got-4) > 1e-9 {
			t.Errorf("%s after one half-life = %v, want 4", name, got)
		}
	}
	ledger.Count("model-a", "host-a", Served)
	if got := ledger.Decayed("model-a", "host-a").Served; math.Abs(got-5) > 1e-9 {
		t.Errorf("served after one more = %v, want 5", got)
	}
	if got := ledger.Totals("model-a", "host-a").Of(Served); got != 9 {
		t.Errorf("served total = %d, want 9 undecayed", got)
	}
	if got := ledger.Totals("model-a", "host-a").Of(VotesOwed); got != 1 {
		t.Errorf("votes_owed total = %d, want 1", got)
	}
}

// Test flow:
//  1. Count into a host, reset, and read it back.
//  2. Assert totals, decayed counts and the host list are empty, and a later count starts from zero.
func TestLedgerResetForgetsEverything(t *testing.T) {
	clock := &testClock{at: time.Unix(1_000, 0)}
	ledger := newTestLedger(clock)
	ledger.Count("model-a", "host-a", Offered)
	ledger.Reset()
	if got := ledger.Totals("model-a", "host-a").Of(Offered); got != 0 {
		t.Errorf("total after reset = %d, want 0", got)
	}
	if got := ledger.Decayed("model-a", "host-a"); got != (Decayed{}) {
		t.Errorf("decayed after reset = %+v, want zero", got)
	}
	if hosts := ledger.Hosts("model-a"); len(hosts) != 0 {
		t.Errorf("hosts after reset = %v, want none", hosts)
	}
	ledger.Count("model-a", "host-a", Offered)
	if got := ledger.Totals("model-a", "host-a").Of(Offered); got != 1 {
		t.Errorf("total after a later count = %d, want 1", got)
	}
}
