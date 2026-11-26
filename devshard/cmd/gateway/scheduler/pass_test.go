package scheduler

import (
	"errors"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/share"
)

type recordingPasser struct {
	mu       sync.Mutex
	requests []PassRequest
	refuse   bool
}

func (p *recordingPasser) Pass(request PassRequest) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refuse {
		return false
	}
	p.requests = append(p.requests, request)
	return true
}

func (p *recordingPasser) passed() []PassRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PassRequest(nil), p.requests...)
}

const hostC = "host-c"

func passHarness(t *testing.T, mode string, cfg harnessConfig) (*harness, *recordingPasser, *passBook, *share.Ledger) {
	t.Helper()
	passer := &recordingPasser{}
	book := &passBook{}
	cfg.passer, cfg.passes, cfg.hostLoadMode = passer, book, mode
	cfg.slots = []string{hostA, hostB, hostC}
	test := newHarness(t, cfg)
	return test, passer, book, nil
}

// Test flow:
//  1. For weight and balance, run one request over the group A, B, C where B's window is full and C is cut off, so the first two nonces are load blocks and the third serves A.
//  2. Assert both blocked nonces are committed as placeholders dated the same second and handed to the pass sender, B's with Send true and C's with Send false, and no ghost is recorded.
//  3. Assert the ledger books each as assigned, offered and passed, the escrow counts two unresolved passes, and resolving them frees the room and books the outcome.
func TestAPassReplacesTheGhostOfALoadBlockUnderWeightAndBalance(t *testing.T) {
	for _, mode := range []string{config.HostLoadModeWeight, config.HostLoadModeBalance} {
		t.Run(mode, func(t *testing.T) {
			clock := newTestClock()
			ledger := newTestLedger(clock)
			test, passer, book, _ := passHarness(t, mode, harnessConfig{
				shares:     ledger,
				windowFull: func(host string) bool { return host == hostB },
				cutOff:     func(host string) bool { return host == hostC },
			})

			wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now())), hostA, 3)
			test.dispatcher.stop()

			_, _, commits := test.session.report()
			if len(commits) != 3 || !commits[0].ghost || !commits[1].ghost || commits[2].ghost || commits[0].startedAt == 0 || commits[0].startedAt != commits[1].startedAt {
				t.Fatalf("commits = %+v, want two placeholders dated the same second, then the real dispatch", commits)
			}
			if burns := test.observer.burns(); len(burns) != 0 {
				t.Errorf("ghost burns = %v, want none: a pass is not a ghost", burns)
			}
			passes := passer.passed()
			if len(passes) != 2 || passes[0].Assignment.Host != hostB || !passes[0].Send || passes[1].Assignment.Host != hostC || passes[1].Send {
				t.Fatalf("passes = %+v, want B with Send and C without", passes)
			}
			if passes[0].Assignment.Nonce.Nonce() != 1 || passes[0].StartedAt != commits[0].startedAt || passes[0].Model != modelA {
				t.Errorf("first pass = %+v, want nonce 1 dated as committed on %s", passes[0], modelA)
			}
			if room := book.room(escrowA); room != maxUnresolvedPasses-2 {
				t.Errorf("room = %d, want %d with two unresolved passes", room, maxUnresolvedPasses-2)
			}
			for _, host := range []string{hostB, hostC} {
				if got := ledger.Totals(modelA, host); got.Of(share.Assigned) != 1 || got.Of(share.Offered) != 1 || got.Of(share.Passed) != 1 {
					t.Errorf("%s totals = %v, want assigned 1, offered 1, passed 1", host, got)
				}
			}
			passes[0].Resolved(PassAnswered)
			passes[1].Resolved(PassOwed)
			if room := book.room(escrowA); room != maxUnresolvedPasses {
				t.Errorf("room after both resolve = %d, want %d", room, maxUnresolvedPasses)
			}
			if got := ledger.Totals(modelA, hostB).Of(share.ProbesAnswered); got != 1 {
				t.Errorf("probes_answered of B = %d, want 1", got)
			}
			if got := ledger.Totals(modelA, hostC).Of(share.VotesOwed); got != 1 {
				t.Errorf("votes_owed of C = %d, want 1", got)
			}
		})
	}
}

// Test flow:
//  1. Run the same group under off, and under weight with a spent chat budget on B and an ejected C.
//  2. Assert off burns two ghosts with the window-full and cut-off reasons and sends nothing to the pass sender.
//  3. Assert under weight the spent chat budget yields a pass with Send false and the ejection a pass with Send true.
func TestOffBurnsTheGhostsAndWeightSendsOnlyWhereTheHostMayBe(t *testing.T) {
	test, passer, _, _ := passHarness(t, config.HostLoadModeOff, harnessConfig{
		windowFull: func(host string) bool { return host == hostB },
		cutOff:     func(host string) bool { return host == hostC },
	})
	wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now())), hostA, 3)
	test.dispatcher.stop()
	if burns := test.observer.burns(); len(burns) != 2 || burns[0] != ghostWindowFull.reason() || burns[1] != ghostCutOff.reason() {
		t.Errorf("off burns = %v, want a window-full and a cut-off ghost as before", burns)
	}
	if len(passer.passed()) != 0 {
		t.Errorf("off handed %d nonces to the pass sender, want none", len(passer.passed()))
	}

	weighted, weightedPasser, _, _ := passHarness(t, config.HostLoadModeWeight, harnessConfig{
		budgetSpent: map[string]bool{hostB: true},
		windowFull:  func(host string) bool { return host == hostB },
		ejected:     func(host string) bool { return host == hostC },
	})
	wantAssignment(t, awaitReply(t, weighted.submit(t, weighted.clock.Now())), hostA, 3)
	weighted.dispatcher.stop()
	passes := weightedPasser.passed()
	if len(passes) != 2 || passes[0].Send || !passes[1].Send {
		t.Errorf("weight passes = %+v, want B without Send for its spent chat budget and the ejected C with Send", passes)
	}
}

// Test flow:
//  1. Under weight, block B for proof-of-compute and C by state divergence, and run one request.
//  2. Assert both are ghosts with their own reasons, counted excused, and the pass sender is never called.
func TestAHostStateBlockStaysABurnUnderWeight(t *testing.T) {
	clock := newTestClock()
	ledger := newTestLedger(clock)
	test, passer, _, _ := passHarness(t, config.HostLoadModeWeight, harnessConfig{
		shares:       ledger,
		pocRequired:  func(host string) bool { return host == hostB },
		stateBlocked: func(host string) bool { return host == hostC },
	})
	wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now())), hostA, 3)
	test.dispatcher.stop()
	if burns := test.observer.burns(); len(burns) != 2 || burns[0] != ghostPoC.reason() || burns[1] != ghostStateDiverged.reason() {
		t.Errorf("burns = %v, want proof-of-compute and state-diverged ghosts", burns)
	}
	if len(passer.passed()) != 0 || ledger.Totals(modelA, hostB).Of(share.Excused) != 1 || ledger.Totals(modelA, hostC).Of(share.Excused) != 1 {
		t.Errorf("passes %d, excused %v %v, want none and one each", len(passer.passed()), ledger.Totals(modelA, hostB), ledger.Totals(modelA, hostC))
	}
}

// Test flow:
//  1. Fill an escrow's unresolved passes to the bound, then run a request whose first nonce binds to a host with a full window.
//  2. Assert the waiter is answered `ErrHostsBusy`, no nonce was committed and nothing was passed.
//  3. Open room for one pass and repeat with a request whose walk needs one pass, and assert it is served.
func TestAnEscrowAtItsPassBoundSpendsNoNonce(t *testing.T) {
	test, passer, book, _ := passHarness(t, config.HostLoadModeWeight, harnessConfig{windowFull: func(host string) bool { return host == hostB }})
	for range maxUnresolvedPasses {
		book.begin(escrowA)
	}

	reply := awaitReply(t, test.submit(t, test.clock.Now()))
	test.dispatcher.stop()

	if !errors.Is(reply.err, ErrHostsBusy) {
		t.Fatalf("reply = %+v, want ErrHostsBusy at the bound", reply)
	}
	if _, _, commits := test.session.report(); len(commits) != 0 || len(passer.passed()) != 0 {
		t.Errorf("commits %v, passes %d, want no nonce spent at the bound", commits, len(passer.passed()))
	}

	room, roomPasser, roomBook, _ := passHarness(t, config.HostLoadModeWeight, harnessConfig{windowFull: func(host string) bool { return host == hostB }})
	for range maxUnresolvedPasses - 1 {
		roomBook.begin(escrowA)
	}
	wantAssignment(t, awaitReply(t, room.submit(t, room.clock.Now())), hostC, 2)
	room.dispatcher.stop()
	if len(roomPasser.passed()) != 1 {
		t.Errorf("passes with room for one = %d, want 1", len(roomPasser.passed()))
	}
}

// Test flow:
//  1. Under weight, cap one request at one burn and block B and C by full windows with A open, so nonce 1 is a pass and nonce 2 meets the cap.
//  2. Assert the request is answered `ErrRequestCapped`, with exactly one pass made and a second nonce not spent.
func TestAPassCountsTowardTheBurnCapOfARequest(t *testing.T) {
	test, passer, _, _ := passHarness(t, config.HostLoadModeWeight, harnessConfig{
		maxBurnsPerRequest: 1,
		windowFull:         func(host string) bool { return host == hostB || host == hostC },
	})
	tally := &BurnTally{}
	reply := awaitReply(t, test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: tally}, test.clock.Now()))
	test.dispatcher.stop()
	if !errors.Is(reply.err, ErrRequestCapped) {
		t.Fatalf("reply = %+v, want ErrRequestCapped once the pass spent the request's one burn", reply)
	}
	if _, _, commits := test.session.report(); len(passer.passed()) != 1 || len(commits) != 1 {
		t.Errorf("passes %d, commits %d, want one pass and no second nonce", len(passer.passed()), len(commits))
	}
}

// Test flow:
//  1. Walk a group of three full hosts with pass mode on, once with room for three passes and once with room for two.
//  2. Assert the forecast counts three passes and three burns and is usable with the room, and unusable one pass short.
//  3. Assert without pass mode the same walk counts no pass.
func TestTheForecastPricesPassesAndRefusesAWalkPastTheBound(t *testing.T) {
	gates := forecastGates(hostA, hostB, hostC)
	escrow := forecastEscrow(3, hostA, hostB, hostC, "open")
	queued := newWaiter(RequestProfile{Model: modelA}, time.Time{})

	if got := expectedBurns(escrow, gates, queued, 0, 0); got.passes != 0 || got.burns != 3 {
		t.Errorf("forecast off = %+v, want three burns and no pass", got)
	}
	gates.passes, gates.passRoom = true, 3
	if got := expectedBurns(escrow, gates, queued, 0, 0); got.passes != 3 || got.burns != 3 || !got.usable {
		t.Errorf("forecast with room = %+v, want three passes, three burns, usable", got)
	}
	gates.passRoom = 2
	if got := expectedBurns(escrow, gates, queued, 0, 0); got.usable {
		t.Errorf("forecast one pass over the bound = %+v, want unusable", got)
	}
}

// Test flow:
//  1. Give two otherwise equal escrows whose walks both cross one full host, and fill the first escrow's passes to the bound.
//  2. Pick under weight and under off.
//  3. Assert weight takes the second escrow every time, and off still rotates over both.
func TestThePickStepsOverAnEscrowAtItsPassBound(t *testing.T) {
	for mode, wantAlways := range map[string]bool{config.HostLoadModeWeight: true, config.HostLoadModeOff: false} {
		scheduler, escrows, _ := newScheduler(
			candidate{id: "bounded", weight: 10, latestNonce: 0, slots: []string{"bounded-open", "bounded-full", "bounded-open", "bounded-full"}},
			candidate{id: "roomy", weight: 10, latestNonce: 0, slots: []string{"roomy-open", "roomy-full", "roomy-open", "roomy-full"}},
		)
		for _, escrow := range escrows.byModel[modelA] {
			escrow.Session.(*fakeSession).participants = escrow.Session.(*fakeSession).slots
		}
		scheduler.limiter.(*fakeLimiter).block("bounded-full")
		scheduler.limiter.(*fakeLimiter).block("roomy-full")
		clock := newTestClock()
		scheduler.now = clock.Now
		scheduler.snapshots = &fakeSnapshots{}
		scheduler.shares = newTestLedger(clock)
		settings := config.Defaults()
		settings.Scheduler.HostLoadMode = mode
		scheduler.settings = config.NewHolder(&settings)
		for range maxUnresolvedPasses {
			scheduler.passes.begin("bounded")
		}
		picked := pickedEscrows(t, scheduler)
		if wantAlways && picked["roomy"] != picksPerRank {
			t.Errorf("%s picked %v, want only the escrow with room", mode, picked)
		}
		if !wantAlways && (picked["roomy"] == 0 || picked["bounded"] == 0) {
			t.Errorf("%s picked %v, want the rotation over both escrows as before the setting", mode, picked)
		}
	}
}

// Test flow:
//  1. Under weight, run one request whose first nonce binds to a host with a full window and a pass sender that refuses the nonce.
//  2. Assert the nonce is recorded as a window-full ghost, the ledger books it pass_unvoted, and the escrow's room is whole again.
func TestARefusedPassIsRecordedAsAGhostAndFreesItsRoom(t *testing.T) {
	clock := newTestClock()
	ledger := newTestLedger(clock)
	test, passer, book, _ := passHarness(t, config.HostLoadModeWeight, harnessConfig{shares: ledger, windowFull: func(host string) bool { return host == hostB }})
	passer.refuse = true

	wantAssignment(t, awaitReply(t, test.submit(t, test.clock.Now())), hostC, 2)
	test.dispatcher.stop()

	if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostWindowFull.reason() {
		t.Errorf("burns = %v, want the refused pass recorded as a window-full ghost", burns)
	}
	if got := ledger.Totals(modelA, hostB).Of(share.PassUnvoted); got != 1 || book.room(escrowA) != maxUnresolvedPasses {
		t.Errorf("pass_unvoted %d, room %d, want 1 and %d", got, book.room(escrowA), maxUnresolvedPasses)
	}
}

// Test flow:
//  1. Begin two passes on an escrow and end one, then end twice more than were begun.
//  2. Assert the room follows the count and never exceeds the bound after a late end.
func TestThePassBookNeverCountsBelowZero(t *testing.T) {
	var book passBook
	book.begin(escrowA)
	book.begin(escrowA)
	book.end(escrowA)
	if got := book.room(escrowA); got != maxUnresolvedPasses-1 {
		t.Fatalf("room with one unresolved = %d, want %d", got, maxUnresolvedPasses-1)
	}
	book.end(escrowA)
	book.end(escrowA)
	if got := book.room(escrowA); got != maxUnresolvedPasses {
		t.Errorf("room after a late end = %d, want %d", got, maxUnresolvedPasses)
	}
}
