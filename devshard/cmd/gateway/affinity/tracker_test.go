package affinity

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const unboundedPinAge = 1000 * time.Hour

type fakeClock struct{ now time.Time }

func (c *fakeClock) advance(by time.Duration) { c.now = c.now.Add(by) }

func newTestTracker(limits *Limits, weights Weights) (*Tracker, *fakeClock) {
	clock := &fakeClock{now: epoch}
	if limits.MaxPinAge == 0 {
		limits.MaxPinAge = unboundedPinAge
	}
	return NewTracker(func() Limits { return *limits }, weights, func() time.Time { return clock.now }), clock
}

func equalWeights(participants ...string) Weights {
	return func() map[string]int {
		slots := map[string]int{}
		for _, participant := range participants {
			slots[participant] = 1
		}
		return slots
	}
}

func recordMany(tracker *Tracker, prefix string, count int, participant string) {
	for index := range count {
		tracker.Record(Key(fmt.Sprintf("%s-%d", prefix, index)), participant, 0)
	}
}

// Test flow:
//  1. Derive keys for the same `prompt_cache_key` under two bearers, and for one bearer twice.
//  2. Assert two callers never share a key and one caller always does.
//  3. Assert the same text as a `prompt_cache_key` and as leading messages gives two keys, and a request with neither has no key.
//  4. Assert a `prompt_cache_key` wins over leading messages when both are present.
//  5. Assert one caller's `prompt_cache_key` or leading messages under two models give two keys.
//  6. Assert neither the namespace nor the model boundary can be shifted.
func TestAKeyIsNamespacedByTheCallerAndTheModel(t *testing.T) {
	mine := KeyFor("bearer-a", "qwen", "session-1", nil)

	require.NotEqual(t, mine, KeyFor("bearer-b", "qwen", "session-1", nil))
	require.Equal(t, mine, KeyFor("bearer-a", "qwen", "session-1", nil))
	require.NotEqual(t, KeyFor("bearer-a", "qwen", "text", nil), KeyFor("bearer-a", "qwen", "", []byte("text")))
	require.Equal(t, NoKey, KeyFor("bearer-a", "qwen", "", nil))
	require.Equal(t, mine, KeyFor("bearer-a", "qwen", "session-1", []byte("leading")))
	require.NotEqual(t, mine, KeyFor("bearer-a", "llama", "session-1", nil))
	require.NotEqual(t, KeyFor("bearer-a", "qwen", "", []byte("leading")), KeyFor("bearer-a", "llama", "", []byte("leading")))
	require.NotEqual(t, KeyFor("ab", "c", "session-1", nil), KeyFor("a", "bc", "session-1", nil), "the namespace and the model cannot trade bytes")
	require.NotEqual(t, KeyFor("a", "qwenk", "y", nil), KeyFor("a", "qwen", "ky", nil), "the model and the content cannot trade bytes")
}

// Test flow:
//  1. Look up a key never recorded and assert `StatusNew`.
//  2. Record a winner and assert a lookup returns it as `StatusHit`.
//  3. Advance the clock to the TTL and assert it is still a hit, then past it and assert `StatusExpired` with the entry gone.
//  4. Look it up again and assert `StatusNew`, since the expired entry was dropped.
func TestAnEntryIsHeldForItsTTL(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10}
	tracker, clock := newTestTracker(limits, nil)

	_, status := tracker.Lookup("key", 0)
	require.Equal(t, StatusNew, status)

	tracker.Record("key", "host-a", 0)
	participant, status := tracker.Lookup("key", 0)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)

	clock.advance(time.Minute)
	_, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusHit, status)

	clock.advance(time.Nanosecond)
	participant, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusExpired, status)
	require.Equal(t, NoParticipant, participant)
	require.Zero(t, tracker.Len())
	_, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusNew, status)
}

// Test flow:
//  1. Record three keys under a cap of three, refresh the oldest, and record a fourth.
//  2. Assert the least recently recorded key was evicted and the refreshed one kept.
//  3. Lower the cap to one while running and record again.
//  4. Assert only the newest entry remains.
func TestTheOldestEntriesGoPastTheCap(t *testing.T) {
	limits := &Limits{TTL: time.Hour, MaxEntries: 3}
	tracker, clock := newTestTracker(limits, nil)
	for _, key := range []Key{"a", "b", "c"} {
		tracker.Record(key, "host", 0)
		clock.advance(time.Second)
	}
	tracker.Record("a", "host-2", 0)
	clock.advance(time.Second)
	tracker.Record("d", "host", 0)

	_, evicted := tracker.Lookup("b", 0)
	require.Equal(t, StatusNew, evicted)
	refreshed, status := tracker.Lookup("a", 0)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-2", refreshed)
	require.Equal(t, 3, tracker.Len())

	limits.MaxEntries = 1
	clock.advance(time.Second)
	tracker.Record("e", "host", 0)
	require.Equal(t, 1, tracker.Len())
	_, status = tracker.Lookup("e", 0)
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Record two keys, advance the clock past the TTL, and record a third.
//  2. Assert the expired two were shed on the way, not left to a lookup.
func TestRecordingShedsWhatHasExpired(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("a", "host", 0)
	tracker.Record("b", "host", 0)
	clock.advance(2 * time.Minute)

	tracker.Record("c", "host", 0)

	require.Equal(t, 1, tracker.Len())
}

// Test flow:
//  1. Record a key, advance 30 minutes, record the same winner again, and advance 31 minutes more under a one-hour pin age and a two-hour TTL.
//  2. Assert the lookup answers `StatusPinAged` although the last win was 31 minutes ago, and the entry is gone.
//  3. Look up again and assert `StatusNew`, then record the key and assert it is a hit that outlives the first pin's age.
func TestAPinEndsAtItsMaximumAgeWhateverTheActivity(t *testing.T) {
	limits := &Limits{TTL: 2 * time.Hour, MaxEntries: 10, MaxPinAge: time.Hour}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a", 0)
	clock.advance(30 * time.Minute)
	tracker.Record("key", "host-a", 0)
	clock.advance(31 * time.Minute)

	participant, status := tracker.Lookup("key", 0)
	require.Equal(t, StatusPinAged, status)
	require.Equal(t, NoParticipant, participant)
	require.Zero(t, tracker.Len())
	_, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusNew, status)

	tracker.Record("key", "host-a", 0)
	clock.advance(45 * time.Minute)
	participant, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)
}

// Test flow:
//  1. Record a key for host-a, advance 50 minutes, and record the same key for host-b under a one-hour pin age.
//  2. Advance 50 minutes more, so the key is 100 minutes old but host-b's pin is 50.
//  3. Assert the lookup is a hit for host-b, since a new host starts a new pin.
//  4. Advance 11 minutes and assert `StatusPinAged`.
func TestAMovedConversationStartsANewPin(t *testing.T) {
	limits := &Limits{TTL: 2 * time.Hour, MaxEntries: 10, MaxPinAge: time.Hour}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a", 0)
	clock.advance(50 * time.Minute)
	tracker.Record("key", "host-b", 0)
	clock.advance(50 * time.Minute)

	participant, status := tracker.Lookup("key", 0)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-b", participant)

	clock.advance(11 * time.Minute)
	_, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusPinAged, status)
}

// Test flow:
//  1. Record a key, advance past the pin age with no lookup in between, and record the same winner again.
//  2. Advance 45 minutes under a one-hour pin age.
//  3. Assert the key is a hit: a win after the age is a new pin, not a refresh of the old one.
func TestAWinAfterTheMaximumAgeRepins(t *testing.T) {
	limits := &Limits{TTL: 2 * time.Hour, MaxEntries: 10, MaxPinAge: time.Hour}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a", 0)
	clock.advance(61 * time.Minute)
	tracker.Record("key", "host-a", 0)
	clock.advance(45 * time.Minute)

	_, status := tracker.Lookup("key", 0)

	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Under a cap of 200 percent and four equal participants, record eight keys for host-0, the floor every participant may always hold.
//  2. Record a ninth and assert its lookup answers `StatusShareCapped`, and the next lookup `StatusNew`.
//  3. Assert host-0 still holds eight pins and an existing key it wins again stays a hit.
//  4. Give each of the other three participants eight keys, so host-0's share of live entries has fallen below the cap.
//  5. Record the ninth key for host-0 again and assert it is a hit.
func TestAParticipantCannotHoldMoreThanItsShareOfTheEntries(t *testing.T) {
	limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 200}
	tracker, _ := newTestTracker(limits, equalWeights("host-0", "host-1", "host-2", "host-3"))
	recordMany(tracker, "k", 8, "host-0")

	tracker.Record("ninth", "host-0", 0)
	_, status := tracker.Lookup("ninth", 0)
	require.Equal(t, StatusShareCapped, status)
	_, status = tracker.Lookup("ninth", 0)
	require.Equal(t, StatusNew, status)
	require.Equal(t, 8, tracker.Len())
	tracker.Record("k-0", "host-0", 0)
	_, status = tracker.Lookup("k-0", 0)
	require.Equal(t, StatusHit, status)

	for _, other := range []string{"host-1", "host-2", "host-3"} {
		recordMany(tracker, other, 8, other)
	}
	tracker.Record("ninth", "host-0", 0)
	_, status = tracker.Lookup("ninth", 0)
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Under a cap of 200 percent, weigh a big participant nine slots and a small one a single slot, and record eight keys for the small one, its floor.
//  2. Record a ninth for the small one and assert `StatusShareCapped`.
//  3. Give the big participant 40 keys, so the small one holds 8 of 48 against a fair share of a tenth.
//  4. Record the ninth again and assert it is a hit, and that the big participant's 41st is a hit too.
func TestTheShareFollowsTheSlotsAParticipantHolds(t *testing.T) {
	weights := func() map[string]int { return map[string]int{"big": 9, "small": 1} }
	limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 200}
	tracker, _ := newTestTracker(limits, weights)
	recordMany(tracker, "small", 8, "small")

	tracker.Record("ninth", "small", 0)
	_, status := tracker.Lookup("ninth", 0)
	require.Equal(t, StatusShareCapped, status)

	recordMany(tracker, "big", 40, "big")
	tracker.Record("ninth", "small", 0)
	_, status = tracker.Lookup("ninth", 0)
	require.Equal(t, StatusHit, status)
	tracker.Record("big-last", "big", 0)
	_, status = tracker.Lookup("big-last", 0)
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Under a cap of 125 percent, weigh two participants three slots each, so each one's fair share is half the pins.
//  2. Give host-b eight keys and host-a eight keys, then record five more for host-a and assert each is a hit, since 13 of 21 is under 62.5 percent.
//  3. Record a 14th for host-a and assert `StatusShareCapped`, since 14 of 22 is over it.
//  4. Assert host-b can still pin, and that after the cap is raised to 200 percent host-a's next pin is a hit.
func TestACapOverFairShareBindsBetweenTwoParticipants(t *testing.T) {
	weights := func() map[string]int { return map[string]int{"host-a": 3, "host-b": 3} }
	limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 125}
	tracker, _ := newTestTracker(limits, weights)
	recordMany(tracker, "b", 8, "host-b")
	recordMany(tracker, "a", 8, "host-a")

	for _, key := range []Key{"a-8", "a-9", "a-10", "a-11", "a-12"} {
		tracker.Record(key, "host-a", 0)
		_, status := tracker.Lookup(key, 0)
		require.Equal(t, StatusHit, status)
	}
	tracker.Record("a-13", "host-a", 0)
	_, status := tracker.Lookup("a-13", 0)
	require.Equal(t, StatusShareCapped, status)

	tracker.Record("b-8", "host-b", 0)
	_, status = tracker.Lookup("b-8", 0)
	require.Equal(t, StatusHit, status)

	limits.ShareCap = 200
	tracker.Record("a-14", "host-a", 0)
	_, status = tracker.Lookup("a-14", 0)
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Build a tracker with no weights and record 50 keys for one participant.
//  2. Assert every key is a hit, since the share cap needs the fleet's weights.
//  3. Build one whose weights answer nothing and assert the same.
func TestWithoutWeightsNoShareCapApplies(t *testing.T) {
	for _, weights := range []Weights{nil, func() map[string]int { return nil }} {
		limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 100}
		tracker, _ := newTestTracker(limits, weights)

		recordMany(tracker, "k", 50, "host-0")

		for index := range 50 {
			_, status := tracker.Lookup(Key(fmt.Sprintf("k-%d", index)), 0)
			require.Equal(t, StatusHit, status)
		}
	}
}

// Test flow:
//  1. Fill host-0 with eight pins under a cap of 200 percent and four equal participants.
//  2. Move one of its keys to host-1, and record a new key for host-0.
//  3. Assert the new key is a hit, since the moved key freed one of host-0's pins.
//  4. Remove host-0's entries by the TTL and assert it can hold eight again.
func TestAPinThatMovesOrExpiresFreesTheShare(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 1000, ShareCap: 200}
	tracker, clock := newTestTracker(limits, equalWeights("host-0", "host-1", "host-2", "host-3"))
	recordMany(tracker, "k", 8, "host-0")
	tracker.Record("k-0", "host-1", 0)

	tracker.Record("fresh", "host-0", 0)
	_, status := tracker.Lookup("fresh", 0)
	require.Equal(t, StatusHit, status)

	clock.advance(2 * time.Minute)
	tracker.Record("later", "host-1", 0)
	recordMany(tracker, "again", 8, "host-0")
	for index := range 8 {
		_, status = tracker.Lookup(Key(fmt.Sprintf("again-%d", index)), 0)
		require.Equal(t, StatusHit, status)
	}
}

// Test flow:
//  1. Record a key won with 8000 prompt tokens under a minimum of 4096.
//  2. Look it up with 3000, 4095, 4096 and 20000 current tokens.
//  3. Assert the expected prefix is the smaller of the two counts: below 4096 answers `StatusBelowMinCached` and no host, 4096 and above is a `StatusHit`.
//  4. Assert the below-minimum lookups left the entry in place.
//  5. Record a key with 4095 and one with 4096 tokens and assert that a 50000-token lookup is below the minimum for the first and a hit for the second at 4096.
func TestAHitIsOnlyWorthAHostWhenTheSmallerPromptReachesTheMinimum(t *testing.T) {
	tracker, _ := newTestTracker(&Limits{TTL: time.Minute, MaxEntries: 10, MinCachedTokens: 4096}, nil)
	tracker.Record("key", "host-a", 8000)

	for _, current := range []uint64{3000, 4095} {
		participant, status := tracker.Lookup("key", current)
		require.Equal(t, StatusBelowMinCached, status, "current %d", current)
		require.Equal(t, NoParticipant, participant)
		require.Equal(t, 1, tracker.Len())
	}
	for _, current := range []uint64{4096, 20000} {
		participant, status := tracker.Lookup("key", current)
		require.Equal(t, StatusHit, status, "current %d", current)
		require.Equal(t, "host-a", participant)
	}

	tracker.Record("short", "host-a", 4095)
	_, status := tracker.Lookup("short", 50000)
	require.Equal(t, StatusBelowMinCached, status, "the last prompt bounds the expected prefix")
	tracker.Record("exact", "host-a", 4096)
	_, status = tracker.Lookup("exact", 4096)
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Record the same keys under a minimum of zero, with counts of zero and 10.
//  2. Look them up with counts of zero and 10.
//  3. Assert every lookup is a `StatusHit`, since 0 disables the minimum.
func TestAZeroMinimumDisablesTheThreshold(t *testing.T) {
	tracker, _ := newTestTracker(&Limits{TTL: time.Minute, MaxEntries: 10}, nil)
	tracker.Record("empty", "host-a", 0)
	tracker.Record("small", "host-a", 10)

	for _, key := range []Key{"empty", "small"} {
		for _, current := range []uint64{0, 10} {
			_, status := tracker.Lookup(key, current)
			require.Equal(t, StatusHit, status, "%s with %d", key, current)
		}
	}
}

// Test flow:
//  1. Record a key on a host with 2000 tokens and assert a 6000-token lookup is below the minimum.
//  2. Record the same host again with 6000 tokens, as a growing conversation does.
//  3. Assert the next lookup is a `StatusHit`.
func TestAGrowingConversationCrossesTheMinimumOnItsOwnHost(t *testing.T) {
	tracker, _ := newTestTracker(&Limits{TTL: time.Minute, MaxEntries: 10, MinCachedTokens: 4096}, nil)
	tracker.Record("key", "host-a", 2000)
	_, status := tracker.Lookup("key", 6000)
	require.Equal(t, StatusBelowMinCached, status)

	tracker.Record("key", "host-a", 6000)

	participant, status := tracker.Lookup("key", 7000)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)
}

// Test flow:
//  1. Record a key on host-a with 20000 tokens, then record a 500-token turn that won on host-b.
//  2. Assert the key still answers host-a for a long turn.
//  3. Record a 30000-token win on host-b and assert the key moves to host-b.
//  4. Let the pin outlive its TTL, record a short win on host-a, switch the minimum off and assert the short win is pinned to host-a, so an expired pin is not held by the long prompt it carried.
//  5. With the minimum off, record a long win on host-a and a short one on host-b and assert the short one moves the pin.
func TestAShortTurnDoesNotMoveAPinWorthRoutingTo(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10, MinCachedTokens: 4096}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a", 20000)

	tracker.Record("key", "host-b", 500)
	participant, status := tracker.Lookup("key", 20000)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)

	tracker.Record("key", "host-b", 30000)
	participant, _ = tracker.Lookup("key", 30000)
	require.Equal(t, "host-b", participant)

	clock.advance(2 * time.Minute)
	tracker.Record("key", "host-a", 500)
	limits.MinCachedTokens = 0
	participant, status = tracker.Lookup("key", 500)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)

	tracker.Record("key", "host-a", 20000)
	tracker.Record("key", "host-b", 100)
	participant, _ = tracker.Lookup("key", 100)
	require.Equal(t, "host-b", participant)
}

// Test flow:
//  1. Record a key on host-a with 20000 tokens, then record a 500-token turn that also won on host-a.
//  2. Assert a 21000-token lookup is a `StatusHit` on host-a, the long prefix still being held.
//  3. Record a 30000-token turn on host-a and assert the count follows it upward.
func TestAShortTurnOnThePinnedHostKeepsTheLongPrefix(t *testing.T) {
	tracker, _ := newTestTracker(&Limits{TTL: time.Minute, MaxEntries: 10, MinCachedTokens: 4096}, nil)
	tracker.Record("key", "host-a", 20000)

	tracker.Record("key", "host-a", 500)

	participant, status := tracker.Lookup("key", 21000)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)

	tracker.Record("key", "host-a", 30000)
	_, status = tracker.Lookup("key", 30000)
	require.Equal(t, StatusHit, status)
	tracker.Record("key", "host-a", 500)
	_, status = tracker.Lookup("key", 40000)
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Record a key on host-a with 3000 tokens, below the 4096 minimum, then record a 500-token win on host-b.
//  2. Switch the minimum off and assert the pin moved to host-b, because a pin below the minimum is not held.
func TestAPinBelowTheMinimumFollowsAShorterWin(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10, MinCachedTokens: 4096}
	tracker, _ := newTestTracker(limits, nil)
	tracker.Record("key", "host-a", 3000)

	tracker.Record("key", "host-b", 500)

	limits.MinCachedTokens = 0
	participant, status := tracker.Lookup("key", 500)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-b", participant)
}

// Test flow:
//  1. Record a key on host-a with 20000 tokens under a pin age of five minutes and let six minutes pass.
//  2. Record a 500-token win on host-b and switch the minimum off.
//  3. Assert the aged pin did not hold the key and it now answers host-b.
func TestAnAgedPinIsNotHeldByItsLongPrefix(t *testing.T) {
	limits := &Limits{TTL: time.Hour, MaxEntries: 10, MaxPinAge: 5 * time.Minute, MinCachedTokens: 4096}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a", 20000)
	clock.advance(6 * time.Minute)

	tracker.Record("key", "host-b", 500)

	limits.MinCachedTokens = 0
	participant, status := tracker.Lookup("key", 500)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-b", participant)
}

// Test flow:
//  1. Under a cap of 200 percent and four equal participants, record eight keys for host-0 and a ninth with 20000 tokens that is share capped.
//  2. Record a 500-token win for the ninth on host-1 and switch the minimum off.
//  3. Assert the capped placeholder did not hold the key and it now answers host-1.
func TestAShareCappedEntryIsNotHeldByItsLongPrefix(t *testing.T) {
	limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 200, MinCachedTokens: 4096}
	tracker, _ := newTestTracker(limits, equalWeights("host-0", "host-1", "host-2", "host-3"))
	recordMany(tracker, "k", 8, "host-0")
	tracker.Record("ninth", "host-0", 20000)

	tracker.Record("ninth", "host-1", 500)

	limits.MinCachedTokens = 0
	participant, status := tracker.Lookup("ninth", 500)
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-1", participant)
}
