package scheduler

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// groupOf names a host per slot, so a nonce walks several of them before the sequence comes back round.
func groupOf(size int) []string {
	group := make([]string, 0, size)
	for index := range size {
		group = append(group, benchParticipant(index))
	}
	return group
}

// busyExcept marks every host but one as out of window, which is the shape a loaded fleet has: the nonce
// keeps landing on hosts that are working, and burning it is what the rung is there to stop.
func busyExcept(open string) func(string) bool {
	return func(participant string) bool { return participant != open }
}

// everyHostBut is what the fake limiter refuses at the acquire, where the real one weighs the request's size.
func everyHostBut(group []string, open string) []string {
	refused := make([]string, 0, len(group))
	for _, participant := range group {
		if participant != open {
			refused = append(refused, participant)
		}
	}
	return refused
}

// The rung's whole claim: a run of burns buys one send, and the run then starts again, so a busy fleet
// costs a bounded share of an escrow's nonces instead of all of them.
func TestABurnRunBuysOneSendAndThenStartsOver(t *testing.T) {
	group := groupOf(16)
	test := newHarness(t, harnessConfig{
		slots:               group,
		windowFull:          busyExcept(group[0]),
		refused:             group,
		maxConsecutiveBurns: 6,
	})

	first := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, first), group[7], 7)

	second := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, second), group[14], 14)
	test.dispatcher.stop()

	require.Equal(t, slices.Repeat([]string{GhostReasonWindowFull}, 12), test.observer.burns(),
		"six burns are the price of each of the two sends")
	require.Equal(t, []forcedSend{
		{participant: group[7], burnsInARow: 6},
		{participant: group[14], burnsInARow: 6},
	}, test.observer.forcedSends(), "each send must name the run it ended")
	require.Equal(t, 2, test.limiter.overdrafts(), "a forced send crosses the window rather than asking it")
}

// The run is reset by any serve, not only by a forced one. Without that, one long run latches the counter
// and every later full window is crossed -- the opposite of the bounded share the rung is for.
func TestAnOrdinaryServeStartsTheRunOver(t *testing.T) {
	group := groupOf(16)
	test := newHarness(t, harnessConfig{
		slots:               group,
		windowFull:          busyExcept(group[3]),
		refused:             everyHostBut(group, group[3]),
		maxConsecutiveBurns: 6,
	})

	first := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, first), group[3], 3)
	require.Empty(t, test.observer.forcedSends(), "a host with room is an ordinary serve, whatever the run behind it")

	second := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, second), group[10], 10)
	test.dispatcher.stop()

	require.Len(t, test.observer.burns(), 8, "two burns before the ordinary serve, then a full run of six")
	require.Equal(t, []forcedSend{{participant: group[10], burnsInARow: 6}}, test.observer.forcedSends(),
		"the second send must be bought by a run that started from zero")
}

// A host with room for one token and none for this request is the ordinary refusal: the peek says open and
// the acquire says no. The rung covers it too, and the send that ends a run is never weighed against the window.
func TestTheRungAlsoCoversARefusalThePeekDidNotPredict(t *testing.T) {
	group := groupOf(16)
	test := newHarness(t, harnessConfig{
		slots:               group,
		refused:             group,
		maxConsecutiveBurns: 3,
	})
	costly := RequestProfile{Model: modelA, InputTokens: 9_000, OutputTokens: 120, Params: "payload"}

	queued := test.submitCosting(t, costly, test.clock.Now())
	wantAssignment(t, awaitReply(t, queued), group[4], 4)
	test.dispatcher.stop()

	require.Len(t, test.observer.burns(), 3, "three burns are the whole price of this send")
	require.Equal(t, []forcedSend{{participant: group[4], burnsInARow: 3}}, test.observer.forcedSends())
	require.Equal(t, 1, test.limiter.overdrafts())
	require.Equal(t, slotCost(costly), test.limiter.chargedCosts()[len(test.limiter.chargedCosts())-1],
		"the tokens a forced send spends are the request's own, or the window it crossed means nothing")
}

// A cut-off host is broken rather than busy. Forcing work onto it spends the nonce the rung was meant to save.
func TestAForcedSendNeverCrossesACutOff(t *testing.T) {
	group := groupOf(16)
	test := newHarness(t, harnessConfig{
		slots:               group,
		cutOff:              busyExcept(group[0]),
		maxConsecutiveBurns: 6,
	})

	queued := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, queued), group[0], 16)
	test.dispatcher.stop()

	require.Equal(t, slices.Repeat([]string{GhostReasonCutOff}, 15), test.observer.burns(),
		"a cut-off host burns its nonce as it did before the rung existed")
	require.Empty(t, test.observer.forcedSends(), "no send may be forced onto a host the cut-off holds")
	require.Zero(t, test.limiter.overdrafts())
}

// Zero is the off switch an operator turns when the rung costs more than the burns it replaces, and the
// rung is read per drain, so moving it reaches an escrow that is already running.
func TestTheRunLengthIsReadAfreshOnEveryDrain(t *testing.T) {
	group := groupOf(16)
	test := newHarness(t, harnessConfig{
		slots:      group,
		windowFull: busyExcept(group[0]),
		refused:    everyHostBut(group, group[0]),
	})

	offTheRung := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, offTheRung), group[0], 16)
	require.Equal(t, slices.Repeat([]string{GhostReasonWindowFull}, 15), test.observer.burns(),
		"with the rung off the drain burns until the nonce reaches a host with room")
	require.Empty(t, test.observer.forcedSends())

	test.burnLimit.Store(3)

	onTheRung := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, onTheRung), group[4], 20)
	test.dispatcher.stop()

	require.Equal(t, []forcedSend{{participant: group[4], burnsInARow: 3}}, test.observer.forcedSends(),
		"the drain must pick up a run length set after it started")
}

// Crossing a full window is a licence for the bound host alone. Read as a licence for the whole fleet it
// hides the one host left that could serve an excluded waiter, and burns the nonce the rescue exists to spend.
func TestAForcedBindingStillRescuesAWaiterThatExcludedIt(t *testing.T) {
	test := newHarness(t, harnessConfig{
		slots:               []string{hostA, hostB},
		windowFull:          busyExcept(hostA),
		maxConsecutiveBurns: 1,
	})

	queued := test.submit(t, test.clock.Now().Add(-2*matchWaitWindow), hostA)
	wantAssignment(t, awaitReply(t, queued), hostA, 2)
	test.dispatcher.stop()

	require.Equal(t, []string{GhostReasonWindowFull}, test.observer.burns(),
		"only the binding on the full host burns; the next one serves rather than burning ghostExclude")
	require.Equal(t, []string{hostA}, test.observer.excludedServes())
	require.Empty(t, test.observer.forcedSends(), "the host had room, so nothing was crossed")
}
