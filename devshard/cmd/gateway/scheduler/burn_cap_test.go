package scheduler

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
)

const burnCapGroupSize = 8

const firstBurnSlot = 1

func burnedSlots(group []string, count int) func(string) bool {
	return func(participant string) bool {
		index := slices.Index(group, participant)
		return index >= firstBurnSlot && index < firstBurnSlot+count
	}
}

// Test flow:
//  1. For each state a slot can be burned in (proof-of-compute, full window, cut-off, ejected, diverged, excluded by the request) and for a shadow slot, build an eight-host group whose first three slots from the cursor are in that state.
//  2. Price the escrow with `expectedBurns`, then submit the same request to a dispatcher over the same predicates and await its serve.
//  3. Assert the estimate equals the burns the drain recorded, three for every burning state and none for the shadow one.
//  4. Assert the request was served by the first host past the three slots, after a probe for each shadow slot.
func TestTheEstimateEqualsTheBurnsTheDrainMakesForEachState(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	states := []struct {
		name    string
		burned  int
		config  func(blocked func(string) bool) harnessConfig
		exclude func() []string
	}{
		{name: "proof-of-compute", burned: 3, config: func(blocked func(string) bool) harnessConfig { return harnessConfig{pocRequired: blocked} }},
		{name: "full window", burned: 3, config: func(blocked func(string) bool) harnessConfig { return harnessConfig{windowFull: blocked} }},
		{name: "cut-off", burned: 3, config: func(blocked func(string) bool) harnessConfig { return harnessConfig{cutOff: blocked} }},
		{name: "ejected", burned: 3, config: func(blocked func(string) bool) harnessConfig { return harnessConfig{ejected: blocked} }},
		{name: "diverged", burned: 3, config: func(blocked func(string) bool) harnessConfig { return harnessConfig{stateBlocked: blocked} }},
		{name: "excluded by the request", burned: 3, config: func(blocked func(string) bool) harnessConfig { return harnessConfig{} }, exclude: func() []string { return group[firstBurnSlot : firstBurnSlot+3] }},
		{name: "shadow", burned: 0, config: func(blocked func(string) bool) harnessConfig {
			return harnessConfig{shadowed: blocked, prober: &recordingProber{}}
		}},
	}

	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			config := state.config(burnedSlots(group, 3))
			config.slots = group
			test := newHarness(t, config)
			var excluded []string
			if state.exclude != nil {
				excluded = state.exclude()
			}
			profile := RequestProfile{Model: modelA, Exclude: excluded, Params: "payload"}
			estimate := expectedBurns(Escrow{Session: test.session}, config.predicates(chain.PhaseSnapshot{}), newWaiter(profile, test.clock.Now()), 0)

			queued := test.submitCosting(t, profile, test.clock.Now().Add(-2*matchWaitWindow))
			wantAssignment(t, awaitReply(t, queued), group[firstBurnSlot+3], firstBurnSlot+3)
			test.dispatcher.stop()

			require.Equal(t, state.burned, estimate, "the estimate")
			require.Len(t, test.observer.burns(), estimate, "the burns the drain made")
		})
	}
}

// Test flow:
//  1. Build an eight-host group whose first five slots from the cursor are ejected, with a cap of three burns per request.
//  2. Submit a request carrying its own burn tally and await its reply.
//  3. Assert the reply is the overloaded error, exactly three burns were recorded, and no fourth nonce was committed.
//  4. Submit a second request with a tally of its own and assert it burns the two ejected slots left and is served by the next host, since the count belongs to the request.
func TestARequestIsAnsweredOverloadedAfterItsCapOfBurns(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	test := newHarness(t, harnessConfig{slots: group, ejected: burnedSlots(group, 5), maxBurnsPerRequest: 3})

	first := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}}, test.clock.Now())
	reply := awaitReply(t, first)
	require.ErrorIs(t, reply.err, ErrRequestCapped)
	require.Len(t, test.observer.burns(), 3)
	_, _, commits := test.session.report()
	require.Len(t, commits, 3, "the declined fourth nonce is not committed")

	second := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}}, test.clock.Now())
	wantAssignment(t, awaitReply(t, second), group[firstBurnSlot+5], firstBurnSlot+5)
	test.dispatcher.stop()
	require.Len(t, test.observer.burns(), 5, "the second request burned the two slots left of the group's five")
}

// Test flow:
//  1. Build an eight-host group whose first five slots from the cursor are ejected, with a cap of zero.
//  2. Submit a request carrying its own burn tally and await its reply.
//  3. Assert it is served by the first host past the ejected ones after five burns, since zero turns the cap off.
func TestAZeroCapNeverAnswersARequestOverloaded(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	test := newHarness(t, harnessConfig{slots: group, ejected: burnedSlots(group, 5), maxBurnsPerRequest: 0})

	queued := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}}, test.clock.Now())

	wantAssignment(t, awaitReply(t, queued), group[firstBurnSlot+5], firstBurnSlot+5)
	test.dispatcher.stop()
	require.Len(t, test.observer.burns(), 5)
}

// Test flow:
//  1. Build an eight-host group whose first two slots from the cursor are ejected, with a cap of three burns per request.
//  2. Submit two requests that share one burn tally, as two attempts of one request do.
//  3. Assert the first is served after two burns and the second, which needs two more, is answered overloaded after one.
func TestTheBurnsOfEveryAttemptOfARequestShareOneCap(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	test := newHarness(t, harnessConfig{slots: group, ejected: func(participant string) bool {
		return slices.Contains([]string{group[1], group[2], group[4], group[5]}, participant)
	}, maxBurnsPerRequest: 3})
	tally := &BurnTally{}

	first := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: tally}, test.clock.Now())
	wantAssignment(t, awaitReply(t, first), group[3], 3)
	second := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Escalation: true, Burns: tally}, test.clock.Now())
	reply := awaitReply(t, second)
	test.dispatcher.stop()

	require.ErrorIs(t, reply.err, ErrRequestCapped)
	require.Len(t, test.observer.burns(), 3, "two burns for the first attempt and the one that reached the cap for the second")
}

// Test flow:
//  1. Build an eight-host group whose first two slots from the cursor are ejected and whose third is one the request excluded, with a cap of two burns.
//  2. Submit a request that excludes that third host and has waited past the match window.
//  3. Assert it is answered capped after the two burns, with no third nonce committed and no send to the excluded host.
func TestAtTheCapAHostTheRequestExcludedIsNotSentTo(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	excluded := group[3]
	test := newHarness(t, harnessConfig{slots: group, ejected: burnedSlots(group, 2), maxBurnsPerRequest: 2})

	queued := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Exclude: []string{excluded}, Burns: &BurnTally{}}, test.clock.Now().Add(-2*matchWaitWindow))
	reply := awaitReply(t, queued)
	test.dispatcher.stop()

	require.ErrorIs(t, reply.err, ErrRequestCapped)
	require.Len(t, test.observer.burns(), 2)
	require.Empty(t, test.observer.excludedServes())
	_, _, commits := test.session.report()
	require.Len(t, commits, 2, "the nonce past the cap is not committed")
}

// Test flow:
//  1. Build an eight-host group whose first four slots from the cursor are shadow hosts and whose next two are ejected, with a cap of two burns and a prober that accepts every probe.
//  2. Submit a request and await its reply.
//  3. Assert it is served by the first host past them after two burns, and the four shadow nonces were probes, neither burned nor counted against the cap.
func TestAShadowSlotIsNeitherBurnedNorCountedAgainstTheCap(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	prober := &recordingProber{}
	test := newHarness(t, harnessConfig{
		slots: group, shadowed: burnedSlots(group, 4), prober: prober, maxBurnsPerRequest: 2,
		ejected: func(participant string) bool {
			index := slices.Index(group, participant)
			return index == firstBurnSlot+4 || index == firstBurnSlot+5
		},
	})

	queued := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}}, test.clock.Now())
	reply := awaitReply(t, queued)
	test.dispatcher.stop()

	require.False(t, errors.Is(reply.err, ErrRequestCapped), "the shadow probes must not have been counted")
	require.Len(t, prober.probed(), 4)
	require.Len(t, test.observer.burns(), 2)
}

// Test flow:
//  1. Build an eight-host group whose first three slots from the cursor are ejected, with the cap read from the live setting and set to two.
//  2. Submit a request and assert it is answered overloaded after two burns.
//  3. Change the setting to zero and submit another request.
//  4. Assert the next request is served after the three burns, since the cap is read when a drain starts and not when the dispatcher does.
func TestTheNextRequestReadsTheCapAsTheSettingStandsNow(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	test := newHarness(t, harnessConfig{slots: group, ejected: burnedSlots(group, 3), maxBurnsPerRequest: 2})

	capped := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}}, test.clock.Now())
	require.ErrorIs(t, awaitReply(t, capped).err, ErrRequestCapped)
	test.burnCap.Store(0)
	uncapped := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}}, test.clock.Now())
	reply := awaitReply(t, uncapped)
	test.dispatcher.stop()

	require.NoError(t, reply.err)
	require.Len(t, test.observer.burns(), 3, "two burns for the capped request, and the one slot left before a usable host for the next")
}

// Test flow:
//  1. Build a scheduler harness over one twelve-host escrow whose slots one to four and six to nine are ejected, with the default cap.
//  2. Pick for a request carrying a burn tally and assert it is served after four burns.
//  3. Apply `max_burns_per_request` of two the way the settings API does, by building the configuration from the override and swapping it in.
//  4. Pick for a second request and assert it is answered overloaded after two burns, then restore the default and assert a third request is served.
func TestAPickReadsTheBurnCapTheSettingsAPILastApplied(t *testing.T) {
	group := groupOf(12)
	test := newSchedulerHarness(t, schedulerConfig{slots: group})
	for _, index := range []int{1, 2, 3, 4, 6, 7, 8, 9} {
		test.perf.eject(group[index])
	}
	pick := func() (Assignment, error) {
		return test.scheduler.Pick(t.Context(), RequestProfile{Model: modelA, Params: "payload", Burns: &BurnTally{}})
	}
	apply := func(overrides env.Tunables) {
		next, err := config.Build(env.Values{}, config.Overrides{Tunables: overrides})
		require.NoError(t, err)
		test.settings.Swap(next)
	}

	first, err := pick()
	wantHost(t, first, err, escrowA, group[5], 5)
	require.Len(t, test.observer.burns(), 4)

	two := int64(2)
	apply(env.Tunables{MaxBurnsPerRequest: &two})
	_, err = pick()
	require.ErrorIs(t, err, ErrRequestCapped)
	require.Len(t, test.observer.burns(), 6, "two more burns, then the cap answered")

	apply(env.Tunables{})
	third, err := pick()
	wantHost(t, third, err, escrowA, group[10], 10)
}

// Test flow:
//  1. Build a harness whose session commits a nonce and then fails preparing it, once.
//  2. Submit a request carrying its own burn tally and await its reply.
//  3. Assert the request failed on the spent nonce and the tally reached a cap of one, since a nonce spent on a failed prepare is a burn of that request.
func TestANonceSpentOnAFailedPrepareIsChargedToItsRequest(t *testing.T) {
	test := newHarness(t, harnessConfig{failAfterSpend: errors.New("database is locked")})
	tally := &BurnTally{}

	queued := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: tally}, test.clock.Now())
	reply := awaitReply(t, queued)
	test.dispatcher.stop()

	require.Error(t, reply.err)
	require.True(t, tally.Reached(1), "the spent nonce must be counted")
}

// Test flow:
//  1. Build an eight-host group whose first slot from the cursor is a shadow host with no probe room and whose next one is ejected, with a cap of one burn and a prober that refuses every probe.
//  2. Submit a request and await its reply.
//  3. Assert the shadow host's burned nonce neither counted against the cap nor was answered overloaded, and the request was served after the ejected host's one burn.
func TestABurnOnAShadowHostIsNotCountedAgainstTheCap(t *testing.T) {
	group := groupOf(burnCapGroupSize)
	test := newHarness(t, harnessConfig{
		slots: group, shadowed: burnedSlots(group, 1), prober: &recordingProber{skipWith: ProbeSkipNoRoom}, maxBurnsPerRequest: 1,
		ejected: func(participant string) bool { return participant == group[firstBurnSlot+1] },
	})
	tally := &BurnTally{}

	queued := test.submitCosting(t, RequestProfile{Model: modelA, Params: "payload", Burns: tally}, test.clock.Now())
	reply := awaitReply(t, queued)
	test.dispatcher.stop()

	wantAssignment(t, reply, group[firstBurnSlot+2], firstBurnSlot+2)
	require.Equal(t, []string{GhostReasonProbeNoRoom, GhostReasonEjected}, test.observer.burns())
	require.True(t, tally.Reached(1), "only the ejected host's burn is counted")
	require.False(t, tally.Reached(2))
}
