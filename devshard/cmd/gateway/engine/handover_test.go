package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const handoverVoteDueIn = time.Hour

type laterPoster struct {
	failingPoster
	harness *queueHarness
}

func (p *laterPoster) VoteDeadline(uint64, time.Time) time.Time {
	return p.harness.now().Add(handoverVoteDueIn)
}

func executionAttempt(nonce uint64) AttemptOutcome {
	attempt := unsettledAttempt()
	attempt.Nonce = nonce
	attempt.ReceiptTime = testEpoch
	return attempt
}

func refusalAttempt(nonce uint64) AttemptOutcome {
	attempt := unsettledAttempt()
	attempt.Nonce = nonce
	return attempt
}

func handoverEngine(t *testing.T) (*Engine, *queueHarness, *laterPoster, *recordingVoteBook) {
	t.Helper()
	harness := newQueueHarness()
	poster := &laterPoster{harness: harness}
	races, _, book := bookedEngine(t, poster, nil)
	races.settles = harness.queue
	return races, harness, poster, book
}

func raceOf(attempts ...AttemptOutcome) RaceOutcome {
	outcome := race(attempts[0])
	outcome.Attempts = attempts
	return outcome
}

func requireStillHeld(t *testing.T, registration *raceRegistration, poster *laterPoster, why string) {
	t.Helper()
	require.Never(t, registration.released.Load, 100*time.Millisecond, time.Millisecond, why)
	require.Zero(t, poster.postCount(), "no vote may be posted before it is due")
}

// Test flow:
//  1. Settle a race that owes one execution vote due in an hour and assert its registration is held.
//  2. Hand its escrow over and assert the registration is released at once with no vote posted and nothing paid in the book.
func TestAHandOverReleasesAnExecutionVoteNotYetDue(t *testing.T) {
	races, _, poster, book := handoverEngine(t)
	registration := races.admit()

	races.settle(raceOf(executionAttempt(11)), nil, registration)
	requireStillHeld(t, registration, poster, "an execution vote not yet due holds the escrow")
	races.HandOver("escrow-1")

	awaitReleased(t, registration)
	require.Zero(t, poster.postCount(), "a handed-over vote is left to the next owner's sweep")
	_, paid := book.snapshot()
	require.Empty(t, paid)
	races.Stop()
}

// Test flow:
//  1. Hand an escrow over first, then settle a race that owes an execution vote on it.
//  2. Assert the registration is released at once and no vote is posted.
func TestARaceEndingAfterAHandOverReleasesItsExecutionVote(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	races.HandOver("escrow-1")
	registration := races.admit()

	races.settle(raceOf(executionAttempt(12)), nil, registration)

	awaitReleased(t, registration)
	require.Zero(t, poster.postCount())
	races.Stop()
}

// Test flow:
//  1. Settle a race that owes a refusal vote not yet due and hand its escrow over.
//  2. Assert the registration stays held and nothing is posted, because only this process holds the prompt.
func TestAHandOverKeepsWaitingForARefusalVote(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	registration := races.admit()

	races.settle(raceOf(refusalAttempt(13)), nil, registration)
	races.HandOver("escrow-1")

	requireStillHeld(t, registration, poster, "a refusal vote keeps its escrow held through a handover")
	races.Stop()
}

// Test flow:
//  1. Settle a race that owes a refusal and an execution vote due together, and hand its escrow over.
//  2. Assert the registration stays held and nothing is posted: a task is released only when all of its votes are execution votes.
func TestAHandOverKeepsWaitingForAGroupHoldingARefusalVote(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	registration := races.admit()

	races.settle(raceOf(executionAttempt(14), refusalAttempt(15)), nil, registration)
	races.HandOver("escrow-1")

	requireStillHeld(t, registration, poster, "a group with a refusal vote keeps its escrow held")
	races.Stop()
}

// Test flow:
//  1. Hand an escrow over and resume it, then settle a race that owes an execution vote on it.
//  2. Assert the registration stays held and nothing is posted: a resumed escrow waits for its votes again.
func TestAResumedEscrowWaitsForItsExecutionVotesAgain(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	races.HandOver("escrow-1")
	races.Resume("escrow-1")
	registration := races.admit()

	races.settle(raceOf(executionAttempt(16)), nil, registration)

	requireStillHeld(t, registration, poster, "a resumed escrow holds for an execution vote")
	races.Stop()
}

// Test flow:
//  1. Settle a race that owes an execution vote on one escrow and hand a different escrow over.
//  2. Assert the registration stays held and nothing is posted.
func TestAHandOverOfAnotherEscrowReleasesNothing(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	registration := races.admit()

	races.settle(raceOf(executionAttempt(17)), nil, registration)
	races.HandOver("escrow-2")

	requireStillHeld(t, registration, poster, "a handover releases only its own escrow")
	races.Stop()
}

type movingPoster struct {
	harness *queueHarness
	started chan struct{}

	once  sync.Once
	mu    sync.Mutex
	moved bool
}

func (p *movingPoster) VoteDeadline(uint64, time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.moved {
		return p.harness.now().Add(handoverVoteDueIn)
	}
	return p.harness.now()
}

func (p *movingPoster) SettleTimeout(ctx context.Context, _ TimeoutStep) (TimeoutVote, error) {
	p.mu.Lock()
	p.moved = true
	p.mu.Unlock()
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	return TimeoutVote{}, ctx.Err()
}

func holdWinnerOf(races *Engine, outcome RaceOutcome) heldWinnerKey {
	key := heldWinnerKey{escrowID: outcome.EscrowID, nonce: outcome.Attempts[0].Nonce}
	races.heldWinners.hold(key, heldWinner{outcome: outcome, attempt: outcome.Attempts[0]})
	return key
}

func requireWinnerDecided(t *testing.T, races *Engine, key heldWinnerKey, why string) {
	t.Helper()
	_, held := races.heldWinners.take(key)
	require.False(t, held, why)
}

// Test flow:
//  1. Hold a served winner's sample for the nonce of a race that owes an execution vote not yet due, and hand its escrow over.
//  2. Assert the registration is released at once, nothing is posted, and the held winner has been decided and is no longer held.
func TestAHandOverDecidesTheWinnerHeldForTheReleasedVote(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	registration := races.admit()
	outcome := raceOf(executionAttempt(19))
	key := holdWinnerOf(races, outcome)

	races.settle(outcome, nil, registration)
	races.HandOver("escrow-1")

	awaitReleased(t, registration)
	require.Zero(t, poster.postCount())
	requireWinnerDecided(t, races, key, "a handed-over vote must not leave its winner held")
	races.Stop()
}

// Test flow:
//  1. Hold a served winner's sample for the nonce of a race that owes an execution vote not yet due.
//  2. Stop the engine and assert Stop returns with nothing posted and the held winner decided and no longer held.
func TestAStopDecidesTheWinnerHeldForAnExecutionVoteNotYetDue(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	registration := races.admit()
	outcome := raceOf(executionAttempt(20))
	key := holdWinnerOf(races, outcome)

	races.settle(outcome, nil, registration)
	races.Stop()

	awaitReleased(t, registration)
	require.Zero(t, poster.postCount())
	requireWinnerDecided(t, races, key, "a vote left by Stop must not leave its winner held")
}

// Test flow:
//  1. Hold a served winner's sample for a race whose execution vote is due, and fire its timer so the post starts.
//  2. Wait until the poster is inside the post, then push the vote's deadline out and hand the escrow over, which cancels the post.
//  3. Assert the registration is released, the held winner is decided and no longer held.
func TestAHandOverCancellingARunningPostDecidesTheWinnerHeldForTheVote(t *testing.T) {
	harness := newQueueHarness()
	poster := &movingPoster{harness: harness, started: make(chan struct{})}
	races, _, _ := bookedEngine(t, poster, nil)
	races.settles = harness.queue
	registration := races.admit()
	outcome := raceOf(executionAttempt(21))
	key := holdWinnerOf(races, outcome)
	races.settle(outcome, nil, registration)
	harness.fire(0)

	select {
	case <-poster.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the poster was not reached within 2s of firing the vote's timer")
	}
	races.HandOver("escrow-1")

	awaitReleased(t, registration)
	requireWinnerDecided(t, races, key, "a cancelled post must not leave its winner held")
	races.Stop()
}

type splitPoster struct {
	failingPoster
	earlyDue   time.Time
	laterDue   time.Time
	laterNonce uint64
}

func (p *splitPoster) VoteDeadline(nonce uint64, _ time.Time) time.Time {
	if nonce == p.laterNonce {
		return p.laterDue
	}
	return p.earlyDue
}

func claimAttempt(nonce uint64, released *atomic.Int32) AttemptOutcome {
	attempt := unsettledAttempt()
	attempt.Nonce = nonce
	attempt.Terminal = TerminalErrorStream
	attempt.MissProof = &MissProof{ResponsePayload: []byte(`{}`), releaseFinish: func() { released.Add(1) }}
	return attempt
}

// Test flow:
//  1. Settle a race that owes an error-miss claim not yet due, whose Finish is held until the claim is posted.
//  2. Stop the engine and assert the registration is released and nothing is posted.
//  3. Assert the held Finish was not released, because Stop hands the claim on unposted.
func TestAStopKeepsTheFinishHeldForAClaimNotYetDue(t *testing.T) {
	races, _, poster, _ := handoverEngine(t)
	registration := races.admit()
	released := &atomic.Int32{}

	races.settle(raceOf(claimAttempt(22, released)), nil, registration)
	races.Stop()

	awaitReleased(t, registration)
	require.Zero(t, poster.postCount())
	require.Zero(t, released.Load(), "a claim Stop leaves unposted must keep its Finish held")
}

// Test flow:
//  1. Settle a race that owes an execution vote due in an hour and an error-miss claim due in two, whose Finish is held until the claim is posted.
//  2. Hand the escrow over and assert the execution vote is released with the registration still held and the Finish still held.
//  3. Advance the clock two hours and fire the claim's timer.
//  4. Assert the claim is posted, the registration is released and the held Finish is released exactly once.
func TestAHandedOverVoteCountsTowardReleasingTheFinishHeldForALaterClaim(t *testing.T) {
	harness := newQueueHarness()
	poster := &splitPoster{earlyDue: harness.now().Add(handoverVoteDueIn), laterDue: harness.now().Add(2 * handoverVoteDueIn), laterNonce: 24}
	races, _, _ := bookedEngine(t, poster, nil)
	races.settles = harness.queue
	registration := races.admit()
	released := &atomic.Int32{}

	races.settle(raceOf(executionAttempt(23), claimAttempt(24, released)), nil, registration)
	races.HandOver("escrow-1")
	require.Never(t, registration.released.Load, 100*time.Millisecond, time.Millisecond, "the claim is still owed")
	require.Zero(t, released.Load(), "the Finish stays held until the claim is posted")
	harness.advance(2 * handoverVoteDueIn)
	harness.fire(1)

	awaitReleased(t, registration)
	require.Equal(t, 1, poster.postCount(), "only the claim is posted")
	require.Equal(t, int32(1), released.Load(), "the handed-over vote and the posted claim together release the Finish")
	races.Stop()
}

// Test flow:
//  1. Settle a race whose error-miss claim is due, fire its timer so the post starts, and wait until the poster is inside it.
//  2. Push the claim's deadline out and Stop the engine, which cancels the post.
//  3. Assert the registration is released and the Finish held for the claim is still held.
func TestAStopCancellingARunningClaimPostKeepsTheFinishHeld(t *testing.T) {
	harness := newQueueHarness()
	poster := &movingPoster{harness: harness, started: make(chan struct{})}
	races, _, _ := bookedEngine(t, poster, nil)
	races.settles = harness.queue
	registration := races.admit()
	released := &atomic.Int32{}
	races.settle(raceOf(claimAttempt(25, released)), nil, registration)
	harness.fire(0)

	select {
	case <-poster.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the poster was not reached within 2s of firing the claim's timer")
	}
	races.Stop()

	awaitReleased(t, registration)
	require.Zero(t, released.Load(), "a claim post cut short by Stop must keep its Finish held")
}

// Test flow:
//  1. Resume an escrow that was never handed over and assert it reports false.
//  2. Hand the escrow over and resume it, and assert it reports true.
//  3. Resume it again and assert it reports false, since the handover was taken back.
func TestResumeReportsWhetherTheEscrowHadBeenHandedOver(t *testing.T) {
	races, _, _, _ := handoverEngine(t)

	require.False(t, races.Resume("escrow-1"))
	races.HandOver("escrow-1")
	require.True(t, races.Resume("escrow-1"))
	require.False(t, races.Resume("escrow-1"))
	races.Stop()
}
