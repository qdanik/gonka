package scheduler

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

const affinityGroupSize = 16

func (h *harness) boundFor(requestID, host string) *waiter {
	queued := newWaiter(RequestProfile{RequestID: requestID, Model: modelA, Params: "payload"}, h.clock.Now())
	queued.homeTo(host)
	return queued
}

func (h *harness) drainWaiters(waiters ...*waiter) {
	h.dispatcher.waiting = append(h.dispatcher.waiting, waiters...)
	h.dispatcher.drain()
}

func affinityBurnsOf(burns []string) int {
	return len(slices.DeleteFunc(slices.Clone(burns), func(reason string) bool { return reason != GhostReasonAffinitySkip }))
}

// Test flow:
//  1. Build a 16-host harness and queue one waiter bound for host 0, whose slot is the last of the cycle.
//  2. Drain the queue.
//  3. Assert the waiter lands on host 0 at nonce 16 after 15 burns, every one an `affinity_skip`, the worst case for one request.
//  4. Assert no forced send, no burn-budget trip, no held nonce, and a hit counted with no exception.
func TestAWaiterBoundForAHostBurnsAtMostAGroupLessOneAndLandsOnIt(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, maxConsecutiveBurns: 2})
	queued := test.boundFor("request-1", group[0])

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[0], 16)
	require.Equal(t, slices.Repeat([]string{GhostReasonAffinitySkip}, affinityGroupSize-1), test.observer.burns())
	require.Empty(t, test.observer.forcedSends())
	holds, trips := test.observer.counts()
	require.Zero(t, holds)
	require.Zero(t, trips)
	landed, exceptions := test.observer.affinity()
	require.Equal(t, 1, landed)
	require.Empty(t, exceptions)
	require.Equal(t, slices.Repeat([]string{"request-1"}, affinityGroupSize-1), test.observer.burnRequests(), "each burn names the request it was spent for")
}

// Test flow:
//  1. Build a 16-host harness with a burn limit of 2 and a window-full host at slot 2, and queue a waiter bound for host 9.
//  2. Abandon the waiter once nonce 5 has been decided, so the drain ends with no serve to reset the run.
//  3. Assert the drain burned four affinity skips and one window-full burn.
//  4. Assert `burnsInARow` counts the window-full burn alone, so the affinity skips never move it.
func TestAffinityBurnsNeverCountTowardsBurnsInARow(t *testing.T) {
	group := groupOf(affinityGroupSize)
	var queued *waiter
	test := newHarness(t, harnessConfig{
		slots: group, holdStart: true, maxConsecutiveBurns: 2,
		windowFull: func(participant string) bool { return participant == group[2] },
		afterDecide: func(binding HostBinding) {
			if binding.Nonce == 5 {
				queued.abandoned.Store(true)
			}
		},
	})
	queued = test.boundFor("request-1", group[9])

	test.drainWaiters(queued)

	require.Equal(t, []string{
		GhostReasonAffinitySkip, GhostReasonWindowFull, GhostReasonAffinitySkip, GhostReasonAffinitySkip, GhostReasonAffinitySkip,
	}, test.observer.burns())
	require.Equal(t, int64(1), test.dispatcher.burnsInARow)
}

// Test flow:
//  1. Build a 16-host harness and queue a waiter bound for host 3 behind one with no target.
//  2. Drain the queue.
//  3. Assert the free waiter takes the first slot nobody is bound for, host 1, and the bound one lands on host 3 at nonce 3.
//  4. Assert exactly one nonce was burned, the slot of host 2 that nobody could use.
func TestAnIntervenedSlotGoesToAWaiterNobodyBoundItAwayFrom(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	free := newWaiter(RequestProfile{RequestID: "free", Model: modelA, Params: "payload"}, test.clock.Now())
	bound := test.boundFor("bound", group[3])

	test.drainWaiters(free, bound)

	wantAssignment(t, awaitReply(t, free), group[1], 1)
	wantAssignment(t, awaitReply(t, bound), group[3], 3)
	require.Equal(t, []string{GhostReasonAffinitySkip}, test.observer.burns())
}

// Test flow:
//  1. Build a 16-host harness and queue a waiter with no target ahead of one bound for host 1.
//  2. Drain the queue.
//  3. Assert the bound waiter lands on host 1 at nonce 1, ahead of the older free waiter, which lands on host 2 at nonce 2.
//  4. Assert nothing was burned.
func TestAWaiterBoundForTheOfferedHostOutranksAFreeOne(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	free := newWaiter(RequestProfile{RequestID: "free", Model: modelA, Params: "payload"}, test.clock.Now())
	bound := test.boundFor("bound", group[1])

	test.drainWaiters(free, bound)

	wantAssignment(t, awaitReply(t, bound), group[1], 1)
	wantAssignment(t, awaitReply(t, free), group[2], 2)
	require.Empty(t, test.observer.burns())
}

// Test flow:
//  1. Build a 16-host harness and queue an escalation and a waiter bound for host 1.
//  2. Drain the queue.
//  3. Assert the bound waiter takes the slot of host 1 at nonce 1, so it is served within its cycle.
//  4. Assert the escalation takes the next slot, host 2 at nonce 2, and nothing is burned.
func TestAWaiterBoundForTheOfferedHostOutranksAnEscalation(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	escalation := newWaiter(RequestProfile{RequestID: "escalation", Model: modelA, Params: "payload", Escalation: true}, test.clock.Now())
	bound := test.boundFor("bound", group[1])

	test.drainWaiters(escalation, bound)

	wantAssignment(t, awaitReply(t, bound), group[1], 1)
	wantAssignment(t, awaitReply(t, escalation), group[2], 2)
	require.Empty(t, test.observer.burns())
}

// Test flow:
//  1. Build a 16-host harness whose host 5 refuses admission although its window reads open, and queue a waiter bound for it.
//  2. Drain the queue.
//  3. Assert the refusal at nonce 5 burns that slot as a window-full burn, after four affinity skips.
//  4. Assert the waiter then lapses and lands on host 6 at nonce 6 with no further affinity burn.
//  5. Assert one `gate_blocked` exception was counted and no hit.
func TestATargetThatRefusesAdmissionLapsesToNormalDispatch(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, refused: []string{group[5]}})
	queued := test.boundFor("request-1", group[5])

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[6], 6)
	require.Equal(t, append(slices.Repeat([]string{GhostReasonAffinitySkip}, 4), GhostReasonWindowFull), test.observer.burns())
	landed, exceptions := test.observer.affinity()
	require.Zero(t, landed)
	require.Equal(t, []AffinityException{AffinityExceptionGateBlocked}, exceptions)
}

// Test flow:
//  1. Build a 16-host harness and queue a waiter bound for a host the escrow's group does not hold.
//  2. Drain the queue.
//  3. Assert the waiter lands on the first slot, burns nothing, and one `not_in_group` exception is counted.
func TestATargetOutsideTheGroupLapsesToNormalDispatch(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	queued := test.boundFor("request-1", "left-the-group")

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[1], 1)
	require.Empty(t, test.observer.burns())
	_, exceptions := test.observer.affinity()
	require.Equal(t, []AffinityException{AffinityExceptionNotInGroup}, exceptions)
}

// Test flow:
//  1. Build a 16-host harness and queue a waiter whose profile carries `new_key` and no target.
//  2. Drain the queue.
//  3. Assert it lands on the first slot, burns nothing, and one `new_key` exception is counted.
func TestAKeyWithoutAHostIsServedNormallyAndCounted(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	queued := newWaiter(RequestProfile{RequestID: "request-1", Model: modelA, Params: "payload", AffinityMiss: AffinityExceptionNewKey}, test.clock.Now())

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[1], 1)
	require.Empty(t, test.observer.burns())
	_, exceptions := test.observer.affinity()
	require.Equal(t, []AffinityException{AffinityExceptionNewKey}, exceptions)
}

// Test flow:
//  1. Build a 16-host harness and queue 16 waiters, one bound for each host, in an order unrelated to the group's.
//  2. Drain the queue.
//  3. Assert every waiter lands on its own host, within the first 16 nonces, and nothing is burned.
func TestSessionsBoundForDifferentHostsAreAllServedWithinOneCycle(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	waiters := make([]*waiter, 0, affinityGroupSize)
	for index := range affinityGroupSize {
		waiters = append(waiters, test.boundFor(group[(index*7)%affinityGroupSize], group[(index*7)%affinityGroupSize]))
	}

	test.drainWaiters(waiters...)

	for _, queued := range waiters {
		result := awaitReply(t, queued)
		require.NoError(t, result.err)
		require.Equal(t, queued.profile.RequestID, result.assignment.Host)
		require.LessOrEqual(t, result.assignment.Nonce.Nonce(), uint64(affinityGroupSize))
	}
	require.Empty(t, test.observer.burns())
}

// Test flow:
//  1. Build a 16-host harness and queue five waiters bound for the same host, whose slot recurs once per cycle.
//  2. Drain the queue.
//  3. Assert each waiter lands on that host, one per cycle, the first at nonce 7 and each later one 16 nonces after the last.
//  4. Assert no request is named by more than 15 burns, the drain's burn budget never tripped, and nothing was held.
func TestWaitersBoundForOneHostAreServedOnePerCycleWithBoundedBurns(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true})
	waiters := make([]*waiter, 0, 5)
	for index := range 5 {
		waiters = append(waiters, test.boundFor("request-"+string(rune('a'+index)), group[7]))
	}

	test.drainWaiters(waiters...)

	for index, queued := range waiters {
		wantAssignment(t, awaitReply(t, queued), group[7], uint64(7+affinityGroupSize*index))
	}
	perRequest := map[string]int{}
	for _, requestID := range test.observer.burnRequests() {
		perRequest[requestID]++
	}
	require.Len(t, perRequest, 5)
	for requestID, burns := range perRequest {
		require.LessOrEqual(t, burns, affinityGroupSize-1, "request %s was named by more burns than a group has other slots", requestID)
	}
	holds, trips := test.observer.counts()
	require.Zero(t, holds)
	require.Zero(t, trips)
}

func maxBurnsOf(burns int64) func() int64 { return func() int64 { return burns } }

// Test flow:
//  1. Build a 16-host harness with an affinity burn cap of one and queue a waiter bound for host 3.
//  2. Drain the queue.
//  3. Assert one nonce was burned as an `affinity_skip` for the request, and the waiter was then served unbound on host 2 at nonce 2.
//  4. Assert no hit was counted and the exception is `max_burns`.
func TestAWaiterThatHasCostItsBurnCapIsServedUnbound(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, affinityMaxBurns: maxBurnsOf(1)})
	queued := test.boundFor("request-1", group[3])

	test.drainWaiters(queued)

	wantAssignment(t, awaitReply(t, queued), group[2], 2)
	require.Equal(t, []string{GhostReasonAffinitySkip}, test.observer.burns())
	require.Equal(t, []string{"request-1"}, test.observer.burnRequests())
	landed, exceptions := test.observer.affinity()
	require.Zero(t, landed)
	require.Equal(t, []AffinityException{AffinityExceptionMaxBurns}, exceptions)
}

// Test flow:
//  1. Build a 16-host harness with a burn cap of zero and queue a waiter bound for host 3.
//  2. Drain the queue and assert it is served unbound on host 1 at nonce 1 with nothing burned and a `max_burns` exception.
//  3. Queue a waiter bound for host 2, whose slot is the next nonce, and drain.
//  4. Assert it lands on host 2 at nonce 2 with nothing burned and no exception.
func TestABurnCapOfZeroPinsOnlyAHostWhoseSlotIsNext(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, affinityMaxBurns: maxBurnsOf(0)})
	away := test.boundFor("away", group[3])

	test.drainWaiters(away)

	wantAssignment(t, awaitReply(t, away), group[1], 1)
	require.Empty(t, test.observer.burns())
	landed, exceptions := test.observer.affinity()
	require.Zero(t, landed)
	require.Equal(t, []AffinityException{AffinityExceptionMaxBurns}, exceptions)

	next := test.boundFor("next", group[2])
	test.drainWaiters(next)

	wantAssignment(t, awaitReply(t, next), group[2], 2)
	require.Empty(t, test.observer.burns())
	landed, exceptions = test.observer.affinity()
	require.Equal(t, 1, landed)
	require.Len(t, exceptions, 1)
}

// Test flow:
//  1. Build a 16-host harness with a burn cap of one and queue a waiter bound for host 5 ahead of one bound for host 7.
//  2. Drain the queue.
//  3. Assert two nonces were burned, the first charged to the older waiter and the second to the other, so neither paid more than the budget.
//  4. Assert the older waiter was served on host 2 at nonce 2 and the other on host 4 at nonce 4, both with a `max_burns` exception.
func TestEveryQueuedWaiterIsChargedAtMostTheBurnCap(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, affinityMaxBurns: maxBurnsOf(1)})
	older := test.boundFor("older", group[5])
	younger := test.boundFor("younger", group[7])

	test.drainWaiters(older, younger)

	wantAssignment(t, awaitReply(t, older), group[2], 2)
	wantAssignment(t, awaitReply(t, younger), group[4], 4)
	require.Equal(t, slices.Repeat([]string{GhostReasonAffinitySkip}, 2), test.observer.burns())
	require.Equal(t, []string{"older", "younger"}, test.observer.burnRequests())
	_, exceptions := test.observer.affinity()
	require.Equal(t, []AffinityException{AffinityExceptionMaxBurns, AffinityExceptionMaxBurns}, exceptions)
}

// Test flow:
//  1. Build a 16-host harness with a burn cap of zero and queue a waiter with no target ahead of one bound for host 2.
//  2. Drain the queue.
//  3. Assert the free waiter takes host 1 at nonce 1 and the bound one lands on host 2 at nonce 2, so no nonce is burned.
//  4. Assert one hit is counted and no exception, since the bound waiter was never released.
func TestAWaiterWhoseHostIsNextAfterAFreeOneIsNotReleasedByAZeroBurnCap(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, affinityMaxBurns: maxBurnsOf(0)})
	free := newWaiter(RequestProfile{RequestID: "free", Model: modelA, Params: "payload"}, test.clock.Now())
	bound := test.boundFor("bound", group[2])

	test.drainWaiters(free, bound)

	wantAssignment(t, awaitReply(t, free), group[1], 1)
	wantAssignment(t, awaitReply(t, bound), group[2], 2)
	require.Empty(t, test.observer.burns())
	landed, exceptions := test.observer.affinity()
	require.Equal(t, 1, landed)
	require.Empty(t, exceptions)
}

// Test flow:
//  1. Build a 16-host harness with a burn cap of one and queue a waiter bound for host 5 that has already been charged one burn, ahead of a free waiter.
//  2. Drain the queue.
//  3. Assert the free waiter takes host 1 at nonce 1, since that offer burns nothing for the bound waiter.
//  4. Assert the bound waiter is released at the next offer and served unbound on host 2 at nonce 2, with nothing burned and a `max_burns` exception.
func TestAWaiterAtItsBurnCapKeepsItsPlaceUntilAnOfferWouldBurnForIt(t *testing.T) {
	group := groupOf(affinityGroupSize)
	test := newHarness(t, harnessConfig{slots: group, holdStart: true, affinityMaxBurns: maxBurnsOf(1)})
	bound := test.boundFor("bound", group[5])
	bound.skipped = 1
	free := newWaiter(RequestProfile{RequestID: "free", Model: modelA, Params: "payload"}, test.clock.Now())

	test.drainWaiters(bound, free)

	wantAssignment(t, awaitReply(t, free), group[1], 1)
	wantAssignment(t, awaitReply(t, bound), group[2], 2)
	require.Empty(t, test.observer.burns())
	_, exceptions := test.observer.affinity()
	require.Equal(t, []AffinityException{AffinityExceptionMaxBurns}, exceptions)
}
