package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
)

const (
	compositionRequestCap   = 2
	compositionPickCap      = 3
	compositionChargedOnce  = 1
	compositionChargedTwice = 2
	compositionTargetSlot   = 5
	compositionShadowSlot   = 1
)

// Test flow:
//  1. Build a 16-host harness with a cap of two burns per request and queue a waiter bound for host 5 that carries a burn tally.
//  2. Drain the queue.
//  3. Assert two nonces were burned as `affinity_skip` and charged to the request's tally.
//  4. Assert the waiter was then served unbound on host 3 at nonce 3 rather than answered overloaded, with the exception `max_burns`.
func TestAnAffinityWaiterWhoseRequestHasNoBurnLeftIsServedUnboundNotCapped(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, maxBurnsPerRequest: compositionRequestCap})
	tally := &BurnTally{}
	queued := newWaiter(RequestProfile{RequestID: "request-1", Model: modelA, Params: "payload", Burns: tally}, test.clock.Now())
	queued.homeTo(group[compositionTargetSlot])

	test.drainWaiters(queued)

	reply := awaitReply(t, queued)
	require.NotErrorIs(t, reply.err, ErrRequestCapped)
	wantAssignment(t, reply, group[compositionRequestCap+1], compositionRequestCap+1)
	require.Equal(t, []string{GhostReasonAffinitySkip, GhostReasonAffinitySkip}, test.observer.burns())
	require.True(t, tally.Reached(compositionRequestCap))
	_, exceptions := test.observer.affinity()
	require.Equal(t, []affinity.Exception{affinity.ExceptionMaxBurns}, exceptions)
}

// Test flow:
//  1. Build a 16-host harness whose slot 1 is a shadow host and queue a waiter bound for host 3, with no burn cap.
//  2. Drain the queue.
//  3. Assert the shadow slot was probed and not burned, so the only affinity burn is the one on host 2.
//  4. Assert the waiter landed on host 3 at nonce 3.
func TestAShadowSlotBeforeTheTargetIsProbedAndNeverAffinityBurned(t *testing.T) {
	group := groupOf(affinityGroupSize)
	prober := &recordingProber{}
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, shadowed: burnedSlots(group, compositionShadowSlot), prober: prober})
	queued := test.boundFor("request-1", group[3])

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[3], 3)
	require.Equal(t, []string{GhostReasonAffinitySkip}, test.observer.burns())
	require.Len(t, prober.probed(), 1)
	require.Equal(t, group[compositionShadowSlot], prober.probed()[0].Assignment.Host)
}

// Test flow:
//  1. Build a 16-host harness whose host 3 is a shadow host and queue a waiter bound for it.
//  2. Drain the queue.
//  3. Assert the waiter was served unbound on host 1 at nonce 1 with nothing burned and the exception `gate_blocked`.
func TestAWaiterBoundForAHostThatTurnedShadowIsServedUnbound(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{
		slots: group, holdStart: true, prober: &recordingProber{},
		shadowed: func(participant string) bool { return participant == group[3] },
	})
	queued := test.boundFor("request-1", group[3])

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[1], 1)
	require.Empty(t, test.observer.burns())
	_, exceptions := test.observer.affinity()
	require.Equal(t, []affinity.Exception{affinity.ExceptionGateBlocked}, exceptions)
}

// Test flow:
//  1. Build one escrow whose next nonce is two steps before an h2 slot, with a cap of three burns per request and a roomy affinity cap.
//  2. Pick for a request bound for h2 whose tally already holds two burns.
//  3. Assert the escrow is still chosen, but the waiter is unbound with a `max_burns` exception.
//  4. Pick for a request whose tally holds one burn and assert the waiter is bound to h2.
func TestAPickPricesTheStepsToTheTargetAgainstWhatTheRequestsCapLeaves(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "far", weight: 10, latestNonce: 3, slots: affinityHosts})
	withMaxBurns(scheduler, affinityGroupSize)
	withBurnCap(scheduler, compositionPickCap)

	_, queued, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2", Burns: chargedTally(compositionChargedTwice)})
	require.NoError(t, err)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)

	_, queued, err = pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2", Burns: chargedTally(compositionChargedOnce)})
	require.NoError(t, err)
	require.Equal(t, "h2", queued.target)
}

// Test flow:
//  1. Build an escrow that holds h2 behind two ejected hosts and one that does not hold h2.
//  2. Pick for a request bound for h2 whose tally holds two of three burns, so the first escrow is passed over for the cap.
//  3. Assert the escrow without h2 is picked and the waiter's exception is `max_burns`, not `not_in_group`.
func TestAnEscrowTheBurnCapPassesOverIsNotReportedAsLackingTheTarget(t *testing.T) {
	scheduler, _, _ := newScheduler(
		candidate{id: "holds", weight: 10, latestNonce: 3, slots: affinityHosts},
		candidate{id: "lacks", weight: 10, latestNonce: 0, slots: []string{"h5", "h6", "h7", "h8"}},
	)
	withMaxBurns(scheduler, affinityGroupSize)
	withBurnCap(scheduler, compositionPickCap)
	scheduler.perf.(*fakePerf).eject("h0")
	scheduler.perf.(*fakePerf).eject("h1")

	picked, queued, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2", Burns: chargedTally(compositionChargedTwice)})

	require.NoError(t, err)
	require.Equal(t, "lacks", picked.ID)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)
}

// Test flow:
//  1. Build an escrow that holds h2 behind two ejected hosts, with h2 itself ejected, and one that does not hold h2, under a cap of three burns per request.
//  2. Pick for a request bound for h2 whose tally holds two of three burns, so the first escrow is passed over for the cap.
//  3. Assert the escrow without h2 is picked and the waiter's exception is `gate_blocked`, since the host is gated rather than missing.
func TestAnEscrowTheBurnCapPassesOverStillReportsAGatedTarget(t *testing.T) {
	scheduler, _, _ := newScheduler(
		candidate{id: "holds", weight: 10, latestNonce: 3, slots: affinityHosts},
		candidate{id: "lacks", weight: 10, latestNonce: 0, slots: []string{"h5", "h6", "h7", "h8"}},
	)
	withMaxBurns(scheduler, affinityGroupSize)
	withBurnCap(scheduler, compositionPickCap)
	for _, host := range []string{"h0", "h1", "h2"} {
		scheduler.perf.(*fakePerf).eject(host)
	}

	picked, queued, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2", Burns: chargedTally(compositionChargedTwice)})

	require.NoError(t, err)
	require.Equal(t, "lacks", picked.ID)
	require.Equal(t, affinity.ExceptionGateBlocked, queued.exception)
}

// Test flow:
//  1. Build one escrow whose next nonce is the h2 slot, under an affinity cap of one burn, with one request already pending that will take that nonce.
//  2. Pick for a request bound for h2 and pinned to that escrow.
//  3. Assert the escrow is returned and the waiter is unbound with a `max_burns` exception, since the pending request moves the cursor three steps from h2.
func TestAPinnedEscrowIsPricedBehindTheRequestsAlreadyPending(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "busy", weight: 10, latestNonce: 1, slots: affinityHosts})
	withMaxBurns(scheduler, 1)
	scheduler.dispatchers["busy"] = &dispatcher{}
	scheduler.dispatchers["busy"].pendingSubmits.Store(1)

	picked, queued, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2", Escrow: "busy"})

	require.NoError(t, err)
	require.Equal(t, "busy", picked.ID)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)
}
