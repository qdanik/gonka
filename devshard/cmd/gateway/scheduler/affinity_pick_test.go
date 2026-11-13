package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

var affinityHosts = []string{"h0", "h1", "h2", "h3"}

func pickBound(scheduler *Scheduler, profile RequestProfile) (Escrow, *waiter, error) {
	queued := newWaiter(profile, time.Time{})
	escrow, err := scheduler.pickEscrow(profile, chain.PhaseSnapshot{}, queued, nil)
	return escrow, queued, err
}

// Test flow:
//  1. Build three escrows over hosts h0..h3 whose next nonces sit 1, 2 and 0 steps before an h2 slot.
//  2. Pick for a request bound for h2.
//  3. Assert the escrow that reaches h2 with no intervening nonce is chosen, and the waiter is bound to h2.
//  4. Load that escrow with a request already pending for h2, and pick again.
//  5. Assert the pick moves to the escrow with one intervening nonce, since the first now needs a full cycle.
func TestAPickReachesTheTargetThroughTheEscrowWithFewestInterveningNonces(t *testing.T) {
	scheduler, _, _ := newScheduler(
		candidate{id: "near", weight: 10, latestNonce: 0, slots: affinityHosts},
		candidate{id: "far", weight: 10, latestNonce: 3, slots: affinityHosts},
		candidate{id: "next", weight: 10, latestNonce: 1, slots: affinityHosts},
	)
	profile := RequestProfile{Model: modelA, Target: "h2"}

	picked, queued, err := pickBound(scheduler, profile)
	require.NoError(t, err)
	require.Equal(t, "next", picked.ID, "the escrow whose cursor is on an h2 slot has no intervening nonce")
	require.Equal(t, "h2", queued.target)
	require.Equal(t, "h2", queued.homedTo)

	scheduler.dispatchers["next"] = &dispatcher{}
	scheduler.dispatchers["next"].targets.add("h2")
	picked, _, err = pickBound(scheduler, profile)
	require.NoError(t, err)
	require.Equal(t, "near", picked.ID, "a pending request for h2 takes the slot, so the next one waits a cycle there")
}

// Test flow:
//  1. Build two escrows that reach h2 in the same number of steps, one carrying more active users.
//  2. Pick for a request bound for h2.
//  3. Assert the less loaded escrow wins the tie.
func TestEscrowsEqualInStepsAreTiedOnLoad(t *testing.T) {
	scheduler, _, _ := newScheduler(
		candidate{id: "busy", weight: 10, latestNonce: 1, activeUsers: 5, slots: affinityHosts},
		candidate{id: "idle", weight: 10, latestNonce: 1, activeUsers: 0, slots: affinityHosts},
	)

	picked, _, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2"})

	require.NoError(t, err)
	require.Equal(t, "idle", picked.ID)
}

// Test flow:
//  1. Build a thin escrow that affords one attempt and reaches h2 at once, beside a funded escrow that reaches it in three steps.
//  2. Pick for a request bound for h2 and assert the funded escrow serves it.
//  3. Build the thin escrow alone, pick again, and assert it serves and the waiter is bound.
func TestAThinEscrowReachesTheTargetOnlyWhenNothingBetterDoes(t *testing.T) {
	thin := candidate{id: "thin", weight: 10, latestNonce: 1, balance: 300, tokenPrice: 1, slots: affinityHosts}
	funded := candidate{id: "funded", weight: 10, latestNonce: 2, slots: affinityHosts}
	profile := RequestProfile{Model: modelA, Target: "h2", InputBytes: 100, OutputTokens: 100}

	both, _, _ := newScheduler(thin, funded)
	picked, _, err := pickBound(both, profile)
	require.NoError(t, err)
	require.Equal(t, "funded", picked.ID, "a request bound for a host is not worth a thin escrow's escalations")

	alone, _, _ := newScheduler(thin)
	picked, queued, err := pickBound(alone, profile)
	require.NoError(t, err)
	require.Equal(t, "thin", picked.ID)
	require.Equal(t, "h2", queued.target)
}

// Test flow:
//  1. Build a regular escrow whose group lacks h2 and a reserve escrow whose group has it.
//  2. Pick for a request bound for h2.
//  3. Assert the regular escrow serves it, the reserve stays closed, and the waiter records `not_in_group`.
func TestAffinityNeverOpensAReserveEscrow(t *testing.T) {
	scheduler, _, _ := newScheduler(
		candidate{id: "regular", weight: 10, slots: []string{"h0", "h1", "h3"}},
		candidate{id: "reserve", weight: 10, isReserve: true, slots: affinityHosts},
	)

	picked, queued, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2"})

	require.NoError(t, err)
	require.Equal(t, "regular", picked.ID)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, affinity.ExceptionNotInGroup, queued.exception)
}

// Test flow:
//  1. Build one escrow over h0..h3.
//  2. Pick for a request bound for a host in no group, and assert the escrow serves it with `not_in_group`.
//  3. Block h2 for the fleet and pick for a request bound for it, and assert the escrow serves it with `gate_blocked`.
//  4. Pick for an escalation that carries a target and assert it is not bound.
//  5. Pick for a request pinned to the escrow and bound for h0, and assert it is bound; pin it with a host the escrow lacks and assert `not_in_group`.
func TestARequestWhoseTargetCannotBeReachedIsDispatchedAsAnyOther(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "only", weight: 10, slots: affinityHosts})

	_, absent, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "elsewhere"})
	require.NoError(t, err)
	require.Equal(t, affinity.ExceptionNotInGroup, absent.exception)
	require.Equal(t, NoTarget, absent.homedTo)

	scheduler.limiter.(*fakeLimiter).block("h2")
	_, blocked, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2"})
	require.NoError(t, err)
	require.Equal(t, affinity.ExceptionGateBlocked, blocked.exception)
	require.Equal(t, NoTarget, blocked.target)

	_, escalation, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h0", Escalation: true})
	require.NoError(t, err)
	require.Equal(t, NoTarget, escalation.target, "an escalation wants another host, never its target")
	require.Equal(t, affinity.ExceptionNone, escalation.exception)

	_, pinned, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h0", Escrow: "only"})
	require.NoError(t, err)
	require.Equal(t, "h0", pinned.target, "a request pinned to an escrow is still bound for its host when the escrow holds it")

	_, pinnedAway, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "elsewhere", Escrow: "only"})
	require.NoError(t, err)
	require.Equal(t, affinity.ExceptionNotInGroup, pinnedAway.exception)
}

// Test flow:
//  1. Build a waiter from a profile that carries a miss, `new_key`, and no target.
//  2. Assert the waiter starts with that exception and no target.
//  3. Bind the waiter to a host and assert the exception clears.
func TestAWaiterStartsWithTheMissItsProfileCarries(t *testing.T) {
	queued := newWaiter(RequestProfile{Model: modelA, AffinityMiss: affinity.ExceptionNewKey}, time.Time{})

	require.Equal(t, affinity.ExceptionNewKey, queued.exception)
	require.Equal(t, NoTarget, queued.target)

	queued.homeTo("h1")
	require.Equal(t, affinity.ExceptionNone, queued.exception)
	require.Equal(t, "h1", queued.target)
}

func withMaxBurns(scheduler *Scheduler, burns int64) *Scheduler {
	settings := config.Defaults()
	scheduler.settings = config.NewHolder(&settings)
	scheduler.affinityBurnBudget = func() int64 { return burns }
	return scheduler
}

// Test flow:
//  1. Build one escrow whose next nonce is two steps before an h2 slot.
//  2. Pick for a request bound for h2 under a burn cap of one.
//  3. Assert the escrow is still chosen, but the waiter is unbound with a `max_burns` exception.
//  4. Raise the budget to two and pick again.
//  5. Assert the waiter is bound to h2 with no exception.
func TestARequestWhoseHostIsBeyondTheBurnCapIsPickedUnbound(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "far", weight: 10, latestNonce: 3, slots: affinityHosts})
	profile := RequestProfile{Model: modelA, Target: "h2"}

	picked, queued, err := pickBound(withMaxBurns(scheduler, 1), profile)
	require.NoError(t, err)
	require.Equal(t, "far", picked.ID)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, NoTarget, queued.homedTo)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)

	_, queued, err = pickBound(withMaxBurns(scheduler, 2), profile)
	require.NoError(t, err)
	require.Equal(t, "h2", queued.target)
	require.Equal(t, affinity.ExceptionNone, queued.exception)
}

// Test flow:
//  1. Build three escrows whose next nonces sit 1, 2 and 0 steps before an h2 slot.
//  2. Pick for a request bound for h2 under a burn cap of zero.
//  3. Assert the escrow with no intervening nonce is chosen and the waiter is bound.
//  4. Drop that escrow from the model and pick again.
//  5. Assert the waiter is unbound with a `max_burns` exception, then bound once the budget is one.
func TestABurnCapOfZeroReachesOnlyAHostWhoseSlotIsNext(t *testing.T) {
	scheduler, escrows, _ := newScheduler(
		candidate{id: "near", weight: 10, latestNonce: 0, slots: affinityHosts},
		candidate{id: "far", weight: 10, latestNonce: 3, slots: affinityHosts},
		candidate{id: "next", weight: 10, latestNonce: 1, slots: affinityHosts},
	)
	profile := RequestProfile{Model: modelA, Target: "h2"}

	picked, queued, err := pickBound(withMaxBurns(scheduler, 0), profile)
	require.NoError(t, err)
	require.Equal(t, "next", picked.ID)
	require.Equal(t, "h2", queued.target)

	escrows.byModel[modelA] = escrows.byModel[modelA][:2]
	_, queued, err = pickBound(scheduler, profile)
	require.NoError(t, err)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)

	_, queued, err = pickBound(withMaxBurns(scheduler, 1), profile)
	require.NoError(t, err)
	require.Equal(t, "h2", queued.target)
}

// Test flow:
//  1. Build one escrow whose next nonce is an h2 slot, under a burn cap of one.
//  2. Pick for h2 and assert the waiter is bound.
//  3. Load the escrow with a request already pending for h2 and pick again.
//  4. Assert the waiter is unbound with a `max_burns` exception, since the pending request takes the slot and a second would wait a full cycle.
func TestRequestsPendingForTheHostCountAgainstTheBurnCap(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "next", weight: 10, latestNonce: 1, slots: affinityHosts})
	withMaxBurns(scheduler, 1)
	profile := RequestProfile{Model: modelA, Target: "h2"}

	_, queued, err := pickBound(scheduler, profile)
	require.NoError(t, err)
	require.Equal(t, "h2", queued.target)

	scheduler.dispatchers["next"] = &dispatcher{}
	scheduler.dispatchers["next"].targets.add("h2")
	_, queued, err = pickBound(scheduler, profile)
	require.NoError(t, err)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)
}

// Test flow:
//  1. Build one escrow whose next nonce is two steps before an h2 slot, under a burn cap of one.
//  2. Pick for a request bound for h2 and pinned to that escrow.
//  3. Assert the escrow is returned, the waiter unbound and the exception `max_burns`.
func TestAPinnedEscrowIsHeldToTheBurnCapToo(t *testing.T) {
	scheduler, _, _ := newScheduler(candidate{id: "far", weight: 10, latestNonce: 3, slots: affinityHosts})
	withMaxBurns(scheduler, 1)

	picked, queued, err := pickBound(scheduler, RequestProfile{Model: modelA, Target: "h2", Escrow: "far"})

	require.NoError(t, err)
	require.Equal(t, "far", picked.ID)
	require.Equal(t, NoTarget, queued.target)
	require.Equal(t, affinity.ExceptionMaxBurns, queued.exception)
}
