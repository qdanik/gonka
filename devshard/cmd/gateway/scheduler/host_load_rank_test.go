package scheduler

import (
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/share"
)

const picksPerRank = 12

// rankFixture is two escrows of the hosts A and B whose cursors land on B and A, with the ledger of both hosts non-zero: A holds 80 of 100 offered nonces and serves a quarter of what it is offered, B holds 20 and serves all.
func rankFixture(t *testing.T, mode string) *Scheduler {
	t.Helper()
	scheduler, escrows, _ := newScheduler(
		candidate{id: "lands-on-b", weight: 10, latestNonce: 0, slots: []string{hostA, hostB, hostA, hostB}},
		candidate{id: "lands-on-a", weight: 10, latestNonce: 1, slots: []string{hostA, hostB, hostA, hostB}},
	)
	for _, escrow := range escrows.byModel[modelA] {
		session := escrow.Session.(*fakeSession)
		session.participants = []string{hostA, hostB}
	}
	clock := newTestClock()
	scheduler.now = clock.Now
	scheduler.snapshots = &fakeSnapshots{}
	scheduler.shares = newTestLedger(clock)
	weights := map[string]float64{hostA: 50, hostB: 50}
	scheduler.weights = func(participant, _ string) float64 { return weights[participant] }
	scheduler.rooms = func(string, string) float64 { return 100 }
	counts := map[string]map[share.Kind]int{
		hostA: {share.Assigned: 90, share.Offered: 80, share.Served: 10, share.Failed: 30, share.Passed: 5, share.Excused: 3},
		hostB: {share.Assigned: 25, share.Offered: 20, share.Served: 20, share.Failed: 4, share.Passed: 1, share.Excused: 2},
	}
	for host, byKind := range counts {
		for kind, count := range byKind {
			for range count {
				scheduler.shares.Count(modelA, host, kind)
			}
		}
	}
	settings := config.Defaults()
	settings.Scheduler.HostLoadMode = mode
	settings.Perf.FailureRateMinVolume = 20
	scheduler.settings = config.NewHolder(&settings)
	return scheduler
}

func pickedEscrows(t *testing.T, scheduler *Scheduler) map[string]int {
	t.Helper()
	picked := map[string]int{}
	for range picksPerRank {
		escrow, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})
		if err != nil {
			t.Fatalf("pickEscrow: %v", err)
		}
		picked[escrow.ID]++
	}
	return picked
}

// Test flow:
//  1. Pick twelve times over two escrows whose cursors land on a host below and a host above its weight target, under off, weight and balance.
//  2. Assert off alternates between both escrows as today, weight always takes the escrow whose walk crosses the host below its target, and balance, where the failing host's target falls further, does too.
func TestThePickRanksByTheSurplusOfTheHostsAWalkCrosses(t *testing.T) {
	t.Run("off keeps the tie rotation", func(t *testing.T) {
		picked := pickedEscrows(t, rankFixture(t, config.HostLoadModeOff))
		if picked["lands-on-b"] != picksPerRank/2 || picked["lands-on-a"] != picksPerRank/2 {
			t.Errorf("off picked %v, want an even rotation as before the setting", picked)
		}
	})
	for _, mode := range []string{config.HostLoadModeWeight, config.HostLoadModeBalance} {
		t.Run(mode, func(t *testing.T) {
			picked := pickedEscrows(t, rankFixture(t, mode))
			if picked["lands-on-b"] != picksPerRank {
				t.Errorf("%s picked %v, want every pick on the escrow whose walk crosses the host below its target", mode, picked)
			}
		})
	}
}

// Test flow:
//  1. Give hosts A and B equal weights and equal offered counts, and A a serve ratio of a quarter against B's full one.
//  2. Pick under weight and under balance.
//  3. Assert weight sees no surplus on either host and rotates, while balance lowers A's target and takes the escrow that crosses B.
func TestBalanceMovesTheRankOffAHostThatServesLessThanItIsOffered(t *testing.T) {
	for mode, wantEven := range map[string]bool{config.HostLoadModeWeight: true, config.HostLoadModeBalance: false} {
		scheduler := rankFixture(t, mode)
		scheduler.shares.Reset()
		for host, served := range map[string]int{hostA: 10, hostB: 40} {
			for range 40 {
				scheduler.shares.Count(modelA, host, share.Offered)
			}
			for range served {
				scheduler.shares.Count(modelA, host, share.Served)
			}
			for range 40 - served {
				scheduler.shares.Count(modelA, host, share.Failed)
			}
		}
		picked := pickedEscrows(t, scheduler)
		even := picked["lands-on-b"] == picksPerRank/2 && picked["lands-on-a"] == picksPerRank/2
		if even != wantEven {
			t.Errorf("%s picked %v, want an even rotation %v", mode, picked, wantEven)
		}
	}
}

// Test flow:
//  1. Walk a group of a shadow slot, a blocked host, a serving host and a host past the serving one, each with a surplus.
//  2. Assert the forecast sums the surplus of the blocked and the serving host, and none of the shadow host or the host past the walk.
//  3. Assert the same walk without a surplus function reads 0.
func TestTheForecastSumsTheSurplusOfTheHostsItCrosses(t *testing.T) {
	gates := forecastGates("full")
	gates.shadowed = func(participant string) bool { return participant == "shadow" }
	surplus := map[string]float64{"shadow": 1000, "full": 7, "serving": -3, "past": 500}
	escrow := forecastEscrow(3, "shadow", "full", "serving", "past")
	queued := newWaiter(RequestProfile{Model: modelA}, time.Time{})

	if got := expectedBurns(escrow, gates, queued, 0, 0).surplus; got != 0 {
		t.Errorf("forecast surplus without a surplus function = %v, want 0", got)
	}
	gates.surplus = func(participant string) float64 { return surplus[participant] }
	forecast := expectedBurns(escrow, gates, queued, 0, 0)
	if forecast.surplus != 4 || forecast.burns != 1 || !forecast.usable {
		t.Errorf("forecast = %+v, want surplus 7 + -3 = 4, one burn, usable", forecast)
	}
}
