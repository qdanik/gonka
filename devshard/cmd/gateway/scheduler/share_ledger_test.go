package scheduler

import (
	"math"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/share"
)

func newTestLedger(clock *testClock) *share.Ledger {
	return share.New(clock.Now, func(string) time.Duration { return 600 * time.Second })
}

// Test flow:
//  1. For each block of the host bound to nonce 1 (window full, cut-off, ejected, proof-of-compute required, state diverged), run a drain of one request with a counting ledger.
//  2. Assert the blocked host is booked assigned, offered and exactly one of passed or excused, and the host that serves nonce 2 is booked assigned and offered.
func TestTheLedgerBooksEachBurnAsPassedOrExcused(t *testing.T) {
	tests := []struct {
		name   string
		config func(*harnessConfig)
		want   share.Kind
	}{
		{"window full", func(cfg *harnessConfig) { cfg.windowFull = func(host string) bool { return host == hostB } }, share.Passed},
		{"cut-off", func(cfg *harnessConfig) { cfg.cutOff = func(host string) bool { return host == hostB } }, share.Passed},
		{"ejected", func(cfg *harnessConfig) { cfg.ejected = func(host string) bool { return host == hostB } }, share.Passed},
		{"proof-of-compute required", func(cfg *harnessConfig) { cfg.pocRequired = func(host string) bool { return host == hostB } }, share.Excused},
		{"state diverged", func(cfg *harnessConfig) { cfg.stateBlocked = func(host string) bool { return host == hostB } }, share.Excused},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newTestClock()
			ledger := newTestLedger(clock)
			cfg := harnessConfig{shares: ledger}
			testCase.config(&cfg)
			test := newHarness(t, cfg)

			wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now())), hostA, 2)
			test.dispatcher.stop()

			blocked := ledger.Totals(modelA, hostB)
			if blocked.Of(share.Assigned) != 1 || blocked.Of(share.Offered) != 1 || blocked.Of(testCase.want) != 1 {
				t.Errorf("blocked host totals = %v, want assigned 1, offered 1 and kind %d 1", blocked, testCase.want)
			}
			if other := share.Passed + share.Excused - testCase.want; blocked.Of(other) != 0 {
				t.Errorf("blocked host booked the other kind %d: %v", other, blocked)
			}
			if served := ledger.Totals(modelA, hostA); served.Of(share.Assigned) != 1 || served.Of(share.Offered) != 1 || served.Of(share.Passed) != 0 {
				t.Errorf("serving host totals = %v, want assigned 1 and offered 1", served)
			}
		})
	}
}

// Test flow:
//  1. Mark hostB a shadow member and run a drain of one request whose first nonce binds to it.
//  2. Assert the ledger holds nothing for the shadow member and one assigned and offered for the host that served.
func TestTheLedgerIgnoresShadowMembers(t *testing.T) {
	clock := newTestClock()
	ledger := newTestLedger(clock)
	test := newHarness(t, harnessConfig{shares: ledger, prober: &recordingProber{}, shadowed: func(host string) bool { return host == hostB }})

	wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now())), hostA, 2)
	test.dispatcher.stop()

	if got := ledger.Totals(modelA, hostB); got != (share.Totals{}) {
		t.Errorf("shadow member totals = %v, want none", got)
	}
	if served := ledger.Totals(modelA, hostA); served.Of(share.Assigned) != 1 || served.Of(share.Offered) != 1 {
		t.Errorf("serving host totals = %v, want assigned 1 and offered 1", served)
	}
}

// Test flow:
//  1. Submit a stale request excluding hostB, so the first nonce on hostB is burned for the request's exclusion.
//  2. Assert hostB is booked assigned, offered and excused, and passed stays zero.
func TestTheLedgerBooksAnExclusionBurnAsExcused(t *testing.T) {
	clock := newTestClock()
	ledger := newTestLedger(clock)
	test := newHarness(t, harnessConfig{shares: ledger})

	wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now().Add(-2*matchWaitWindow), hostB)), hostA, 2)
	test.dispatcher.stop()

	if got := ledger.Totals(modelA, hostB); got.Of(share.Excused) != 1 || got.Of(share.Passed) != 0 || got.Of(share.Offered) != 1 || got.Of(share.Assigned) != 1 {
		t.Errorf("excluded host totals = %v, want excused 1, offered 1, assigned 1", got)
	}
}

// Test flow:
//  1. Build a scheduler over one escrow of three hosts with weights 60, 30 and 10, a ledger holding all six counts for each, and a room per host.
//  2. Read each host's `HostShare` under `off` and under `balance`.
//  3. Assert weights, weight and slot shares, the weight targets under `off`, the scaled targets under `balance`, the totals and the mode.
func TestHostShareReportsTheTargetsOfTheModeInForce(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "escrow-1", slots: []string{hostA, hostB, hostB, "host-c"}, weight: 1})
	clock := newTestClock()
	scheduler.now = clock.Now
	scheduler.shares = newTestLedger(clock)
	scheduler.snapshots = &fakeSnapshots{}
	weights := map[string]float64{hostA: 60, hostB: 30, "host-c": 10}
	scheduler.weights = func(participant, _ string) float64 { return weights[participant] }
	scheduler.rooms = func(string, string) float64 { return 100 }
	for host, served := range map[string]int{hostA: 4, hostB: 6, "host-c": 2} {
		for _, kind := range []share.Kind{share.Assigned, share.Offered, share.Failed, share.Passed, share.Excused} {
			scheduler.shares.Count(modelA, host, kind)
		}
		for range served {
			scheduler.shares.Count(modelA, host, share.Served)
			scheduler.shares.Count(modelA, host, share.Offered)
		}
	}
	settings := config.Defaults()
	scheduler.settings = config.NewHolder(&settings)

	off, held := scheduler.HostShare(hostA, modelA)
	if !held || off.HostLoadMode != config.HostLoadModeOff || off.Weight != 60 || math.Abs(off.WeightShare-0.6) > 1e-9 || math.Abs(off.TargetShare-0.6) > 1e-9 || off.SlotShare != 0.25 {
		t.Fatalf("off share of hostA = %+v, held %v, want weight 60, weight share and target 0.6, slot share 0.25", off, held)
	}
	if off.Totals.Of(share.Served) != 4 || off.Totals.Of(share.Passed) != 1 || off.Totals.Of(share.Excused) != 1 {
		t.Errorf("hostA totals = %v, want served 4, passed 1, excused 1", off.Totals)
	}
	if other, held := scheduler.HostShare(hostB, modelA); !held || other.SlotShare != 0.5 {
		t.Errorf("hostB share = %+v, held %v, want slot share 0.5 for two of four slots", other, held)
	}
	if _, held := scheduler.HostShare("nobody", modelA); held {
		t.Error("HostShare of a host in no escrow = held, want none")
	}

	balanced := config.Defaults()
	balanced.Scheduler.HostLoadMode = config.HostLoadModeBalance
	balanced.Perf.FailureRateMinVolume = 1
	scheduler.settings = config.NewHolder(&balanced)
	clock.advance(2 * loadTableMaxAge)
	row, held := scheduler.HostShare(hostA, modelA)
	if !held || row.HostLoadMode != config.HostLoadModeBalance || row.CapacityFactor >= 1 || row.TargetShare >= 0.6 {
		t.Errorf("balance share of hostA = %+v, want a capacity factor and target under the weight share", row)
	}
}
