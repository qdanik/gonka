package scheduler

import (
	"context"
	"errors"
	"testing"

	"devshard/types"
)

// busyEscrowHarness stands two escrows side by side with groups of their own, weighted so the first one wins the pick.
func busyEscrowHarness(t *testing.T, busy, spare string) *schedulerHarness {
	t.Helper()
	test := newSchedulerHarness(t, schedulerConfig{
		escrows: []string{busy, spare},
		slotsByEscrow: map[string][]string{
			busy:  {busy + "-0", busy + "-1"},
			spare: {spare + "-0", spare + "-1"},
		},
	})
	test.weights.byEscrow[busy] = 1_000
	test.weights.byEscrow[spare] = 10
	test.loadEscrow(t, spare, 1)
	return test
}

// busyGroup fills every window of an escrow's group, so its drain answers ErrHostsBusy.
func (h *schedulerHarness) busyGroup(t *testing.T, escrowID string) {
	t.Helper()
	for _, participant := range h.session(t, escrowID).slots {
		h.limiter.block(participant)
	}
}

// rounds counts one snapshot per pick and one per drain reached.
func (h *schedulerHarness) rounds() int { return h.snapshots.fetches() }

// reached reports that a pick actually routed to this escrow.
func (h *schedulerHarness) reached(t *testing.T, escrowID string) bool {
	t.Helper()
	_, live := h.liveDispatchers()[escrowID]
	return live
}

// A drain that gives up answers "this escrow, right now", so another escrow may still serve on its next nonce.
func TestARequestAnEscrowGivesUpOnIsOfferedToAnotherEscrow(t *testing.T) {
	test := busyEscrowHarness(t, escrowA, escrowB)
	test.busyGroup(t, escrowA)

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	wantHost(t, assignment, err, escrowB, escrowB+"-1", 1)
	if !test.reached(t, escrowA) {
		t.Fatal("the pick never reached the busy escrow, so nothing was re-picked")
	}
	if advances, _, _ := test.session(t, escrowA).report(); advances != 0 {
		t.Fatalf("the escrow that gave up advanced %d nonces, want 0: its sweep answers without one", advances)
	}
}

// The escrow that gave up still scores best, so without the exclusion the second round is a second 503.
func TestTheSecondRoundLeavesOutTheEscrowThatGaveUp(t *testing.T) {
	test := busyEscrowHarness(t, escrowA, escrowB)
	test.busyGroup(t, escrowA)

	if _, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA}); err != nil {
		t.Fatalf("Pick: %v", err)
	}

	if advances, _, _ := test.session(t, escrowB).report(); advances != 1 {
		t.Fatalf("the spare escrow advanced %d nonces, want the one it served with", advances)
	}
}

// One retry, never a loop, and a busy shard still answers busy rather than out of capacity.
func TestABusyShardAnswersOnceAndStops(t *testing.T) {
	test := busyEscrowHarness(t, escrowA, escrowB)
	test.busyGroup(t, escrowA)
	test.busyGroup(t, escrowB)

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	if !errors.Is(err, ErrHostsBusy) {
		t.Fatalf("Pick = %v, want ErrHostsBusy", err)
	}
	if rounds := test.rounds(); rounds != 4 {
		t.Fatalf("a busy shard cost %d snapshot reads, want the two rounds' four", rounds)
	}
	for _, escrowID := range []string{escrowA, escrowB} {
		if !test.reached(t, escrowID) {
			t.Fatalf("escrow %s was never tried, so the one retry did not run", escrowID)
		}
		if advances, _, _ := test.session(t, escrowID).report(); advances != 0 {
			t.Fatalf("escrow %s advanced %d nonces answering a busy shard, want 0", escrowID, advances)
		}
	}
}

// ErrNoAvailableHost carries no Retry-After and reads as a broken gateway, so it may not replace a busy answer.
func TestAWorseRefusalFromTheSecondEscrowDoesNotReplaceTheBusyAnswer(t *testing.T) {
	test := busyEscrowHarness(t, escrowA, escrowB)
	test.busyGroup(t, escrowA)
	for _, participant := range test.session(t, escrowB).slots {
		test.scheduler.BlockHost(escrowB, participant)
	}

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	if !errors.Is(err, ErrHostsBusy) {
		t.Fatalf("Pick = %v, want the busy shard's own answer rather than the second escrow's worse one", err)
	}
	if errors.Is(err, ErrNoAvailableHost) {
		t.Fatalf("Pick = %v: a full shard must not answer as a broken one", err)
	}
}

// An escrow out of money is the one fact no other error reports, and the engine latches it to stop escalating.
func TestASecondRoundOutOfFundsReachesTheCaller(t *testing.T) {
	test := busyEscrowHarness(t, escrowA, escrowB)
	test.busyGroup(t, escrowA)
	test.session(t, escrowB).balance = 0

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, OutputTokens: 64})

	if !errors.Is(err, types.ErrInsufficientBalance) {
		t.Fatalf("Pick = %v, want the out-of-funds answer to survive the fold", err)
	}
}

// Nothing else was routable, so a shard that is merely busy must not answer as a missing one.
func TestTheOnlyEscrowsAnswerSurvivesARePickWithNowhereToGo(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{escrows: []string{escrowA}})
	for _, participant := range []string{hostA, hostB} {
		test.limiter.block(participant)
	}

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	if !errors.Is(err, ErrHostsBusy) {
		t.Fatalf("Pick = %v, want the busy escrow's own answer", err)
	}
	if errors.Is(err, ErrNoEscrowCapacity) {
		t.Fatalf("Pick = %v, want a busy shard not to read as a missing one", err)
	}
}

// An escalation races attempts inside one escrow's nonce stream, so a re-pick would hand it an escrow the race knows nothing about.
func TestAPinnedEscrowIsNeverRePicked(t *testing.T) {
	test := busyEscrowHarness(t, escrowA, escrowB)
	test.busyGroup(t, escrowA)

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, Escrow: escrowA})

	if !errors.Is(err, ErrHostsBusy) {
		t.Fatalf("Pick = %v, want the pinned escrow's own answer", err)
	}
	if advances, _, _ := test.session(t, escrowB).report(); advances != 0 {
		t.Fatalf("a pinned pick reached escrow %s, which advanced %d nonces", escrowB, advances)
	}
	if rounds := test.rounds(); rounds != 2 {
		t.Fatalf("a pinned pick cost %d snapshot reads, want the one round's two", rounds)
	}
}

// Only "busy" earns a second escrow; a host the chain has stopped is not a condition another escrow fixes.
func TestARefusalThatIsNotBusyIsAnsweredWhereItHappened(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{
		escrows: []string{escrowA, escrowB},
		slotsByEscrow: map[string][]string{
			escrowA: {escrowA + "-0", escrowA + "-1"},
			escrowB: {escrowB + "-0", escrowB + "-1"},
		},
	})
	test.weights.byEscrow[escrowA] = 1_000
	test.weights.byEscrow[escrowB] = 10
	test.loadEscrow(t, escrowB, 1)
	for _, participant := range test.session(t, escrowA).slots {
		test.scheduler.BlockHost(escrowA, participant)
	}

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	if !errors.Is(err, ErrNoAvailableHost) {
		t.Fatalf("Pick = %v, want ErrNoAvailableHost", err)
	}
	if advances, _, _ := test.session(t, escrowB).report(); advances != 0 {
		t.Fatalf("a state-blocked escrow re-picked onto %s, which advanced %d nonces", escrowB, advances)
	}
}
