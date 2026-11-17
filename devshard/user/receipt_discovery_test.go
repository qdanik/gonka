package user

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/types"
)

type askedLog struct {
	mu    sync.Mutex
	slots []int
}

func (a *askedLog) record(slot int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.slots = append(a.slots, slot)
}

func (a *askedLog) snapshot() []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int(nil), a.slots...)
}

func receiptFor(nonce uint64) []*types.DevshardTx {
	return []*types.DevshardTx{{Tx: &types.DevshardTx_ConfirmStart{ConfirmStart: &types.MsgConfirmStart{InferenceId: nonce}}}}
}

type discoveryRound struct {
	session   *Session
	nonce     uint64
	verifiers map[int]TimeoutVerifier
	asked     *askedLog
	slots     []int
}

func newDiscoveryRound(t *testing.T, accept bool, receiptAt map[int]bool) discoveryRound {
	t.Helper()
	session, hosts, _ := setupSession(t, 6, 100000, 10)
	nonce := uint64(1)
	executorIdx := int(nonce % uint64(len(session.group)))
	round := discoveryRound{session: session, nonce: nonce, verifiers: map[int]TimeoutVerifier{}, asked: &askedLog{}}
	for i, slot := range session.group {
		if i == executorIdx {
			continue
		}
		slotIdx := i
		verifier := &mockTimeoutVerifier{
			accept: accept, signer: signerForSlot(t, hosts, slot), group: session.group, slotIdx: slotIdx,
			onVerify: func() { round.asked.record(slotIdx) },
		}
		if receiptAt[i] {
			verifier.mempool = receiptFor(nonce)
		}
		round.verifiers[i] = verifier
		round.slots = append(round.slots, i)
	}
	return round
}

func (r discoveryRound) servingOnly(slots ...int) func() func(string) bool {
	serving := map[string]bool{}
	for _, slot := range slots {
		serving[r.session.group[slot].ValidatorAddress] = true
	}
	return func() func(string) bool { return func(participant string) bool { return serving[participant] } }
}

func (r discoveryRound) collect(t *testing.T, ctx context.Context, reason types.TimeoutReason) ([]*types.TimeoutVote, []*types.DevshardTx) {
	t.Helper()
	votes, recovery, _, _, err := r.session.collectTimeoutVotes(ctx, r.nonce, reason, timeoutVotePayload(), r.verifiers, nil)
	require.NoError(t, err)
	return votes, recovery
}

// Test flow:
// 1. Build a six-host session and one verifier per non-executor slot; only the first slot can serve and its verifier returns the host's receipt.
// 2. Run a refusal round under receipt discovery.
// 3. Assert the receipt is returned and no verifier but the serving one was asked.
func TestReceiptDiscovery_StopsAtFirstRecoveredReceipt(t *testing.T) {
	probe := newDiscoveryRound(t, false, nil)
	serving := probe.slots[0]
	round := newDiscoveryRound(t, false, map[int]bool{serving: true})

	ctx := WithReceiptDiscovery(context.Background(), round.servingOnly(serving))
	votes, recovery := round.collect(t, ctx, types.TimeoutReason_TIMEOUT_REASON_REFUSED)

	require.Empty(t, votes)
	require.Len(t, recovery, 1, "the recovered receipt is returned for publication")
	require.Equal(t, []int{serving}, round.asked.snapshot(), "no verifier past the first receipt was asked")
}

// Test flow:
// 1. Build a six-host session where two non-executor slots can serve and no verifier holds a receipt.
// 2. Run a refusal round under receipt discovery.
// 3. Assert every verifier was asked and both serving slots were asked before any non-serving one.
func TestReceiptDiscovery_AsksServingVerifiersFirst(t *testing.T) {
	round := newDiscoveryRound(t, false, nil)
	servingSlots := []int{round.slots[3], round.slots[1]}

	ctx := WithReceiptDiscovery(context.Background(), round.servingOnly(servingSlots...))
	round.collect(t, ctx, types.TimeoutReason_TIMEOUT_REASON_REFUSED)

	asked := round.asked.snapshot()
	require.Len(t, asked, len(round.slots))
	require.ElementsMatch(t, servingSlots, asked[:2], "serving verifiers come before the rest")
}

// Test flow:
// 1. Run the same all-accepting refusal round once without discovery and once under it, with no receipt anywhere.
// 2. Assert both asked every verifier and returned the same votes and quorum.
// 3. Repeat with rejecting verifiers and assert both return no votes and no recovery.
func TestReceiptDiscovery_NoReceiptAsksAllAndReachesTheSameOutcome(t *testing.T) {
	refused := types.TimeoutReason_TIMEOUT_REASON_REFUSED
	for _, accept := range []bool{true, false} {
		plain := newDiscoveryRound(t, accept, nil)
		plainVotes, plainRecovery := plain.collect(t, context.Background(), refused)

		discovered := newDiscoveryRound(t, accept, nil)
		ctx := WithReceiptDiscovery(context.Background(), discovered.servingOnly(discovered.slots[0]))
		votes, recovery := discovered.collect(t, ctx, refused)

		require.Len(t, plain.asked.snapshot(), len(plain.slots))
		require.Len(t, discovered.asked.snapshot(), len(discovered.slots))
		require.Len(t, votes, len(plainVotes))
		require.Equal(t, plain.session.HasSufficientTimeoutVotes(plainVotes), discovered.session.HasSufficientTimeoutVotes(votes))
		require.Len(t, recovery, len(plainRecovery))
	}
}

// Test flow:
// 1. Build a six-host session whose only serving verifier returns a receipt.
// 2. Run an execution round under receipt discovery.
// 3. Assert every verifier was asked, since only refusal rounds are discovery rounds.
func TestReceiptDiscovery_LeavesExecutionRoundsAlone(t *testing.T) {
	probe := newDiscoveryRound(t, false, nil)
	serving := probe.slots[0]
	round := newDiscoveryRound(t, false, map[int]bool{serving: true})

	ctx := WithReceiptDiscovery(context.Background(), round.servingOnly(serving))
	round.collect(t, ctx, types.TimeoutReason_TIMEOUT_REASON_EXECUTION)

	require.Len(t, round.asked.snapshot(), len(round.slots))
}

// Test flow:
// 1. Read the shipped poll and heartbeat intervals.
// 2. Assert the poll is 5 s and the heartbeat is 10 minutes.
func TestTimeoutWaitIntervals(t *testing.T) {
	require.Equal(t, 5*time.Second, timeoutClosePollInterval)
	require.Equal(t, 10*time.Minute, TimeoutHeartbeatInterval)
}

// Test flow:
// 1. Shrink the poll to 10 ms and set an hour-long heartbeat, with a deadline an hour away.
// 2. Start the wait with a close check that reports only from its fourth call, so only polling can reach it.
// 3. Assert the wait returns true within one second, long before the deadline, and logged no heartbeat.
func TestSleepUntilDeadline_NoticesDoneWithinOnePoll(t *testing.T) {
	prevPoll, prevBeat := timeoutClosePollInterval, TimeoutHeartbeatInterval
	timeoutClosePollInterval, TimeoutHeartbeatInterval = 10*time.Millisecond, time.Hour
	t.Cleanup(func() { timeoutClosePollInterval, TimeoutHeartbeatInterval = prevPoll, prevBeat })

	var checks, beats atomic.Int32
	returned := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		returned <- sleepUntilDeadlineWithHeartbeat(ctx, time.Now().Add(time.Hour), func() { beats.Add(1) }, func() bool { return checks.Add(1) > 3 })
	}()

	select {
	case ok := <-returned:
		require.True(t, ok)
		require.Zero(t, beats.Load())
	case <-time.After(time.Second):
		t.Fatal("the execution wait did not notice the closed record within a poll")
	}
}

// Test flow:
// 1. Set a 60 ms heartbeat, a 10 ms poll and a deadline 200 ms away.
// 2. Run the wait to its deadline with a close check that never reports.
// 3. Assert it returned true at the deadline having logged at most one heartbeat per interval.
func TestSleepUntilDeadline_HeartbeatNotMoreOftenThanItsInterval(t *testing.T) {
	prevPoll, prevBeat := timeoutClosePollInterval, TimeoutHeartbeatInterval
	timeoutClosePollInterval, TimeoutHeartbeatInterval = 10*time.Millisecond, 60*time.Millisecond
	t.Cleanup(func() { timeoutClosePollInterval, TimeoutHeartbeatInterval = prevPoll, prevBeat })

	var beats atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ok := sleepUntilDeadlineWithHeartbeat(ctx, time.Now().Add(200*time.Millisecond), func() { beats.Add(1) }, func() bool { return false })

	require.True(t, ok)
	require.LessOrEqual(t, beats.Load(), int32(3), "the poll must not drive the heartbeat")
}

// Test flow:
// 1. Pend a nonce in a session, set an hour-long heartbeat and a 10 ms poll, and start a refusal wait with an hour-away deadline.
// 2. Give the wait a close check that reports only from its fourth call, so only polling can reach it.
// 3. Assert the wait returns false within one second though no receipt arrived and the heartbeat is an hour away.
func TestSleepRefusal_NoticesClosedWithinOnePoll(t *testing.T) {
	prevPoll, prevBeat := timeoutClosePollInterval, TimeoutHeartbeatInterval
	timeoutClosePollInterval, TimeoutHeartbeatInterval = 10*time.Millisecond, time.Hour
	t.Cleanup(func() { timeoutClosePollInterval, TimeoutHeartbeatInterval = prevPoll, prevBeat })

	session, _, _ := setupSession(t, 3, 100000, 10)
	prepared, err := session.PrepareInference(context.Background(), InferenceParams{
		Model: "llama", Prompt: timeoutVotePayload().Prompt, InputLength: 100, MaxTokens: 100, StartedAt: 1000,
	})
	require.NoError(t, err)

	var checks atomic.Int32
	returned := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		returned <- session.sleepRefusal(ctx, prepared.Nonce(), time.Unix(1000, 0), time.Now().Add(time.Hour), func() {}, func() bool { return checks.Add(1) > 3 })
	}()

	select {
	case ok := <-returned:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("the refusal wait did not notice the closed record within a poll")
	}
}
