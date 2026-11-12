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
		tracker.Record(Key(fmt.Sprintf("%s-%d", prefix, index)), participant)
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

	_, status := tracker.Lookup("key")
	require.Equal(t, StatusNew, status)

	tracker.Record("key", "host-a")
	participant, status := tracker.Lookup("key")
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-a", participant)

	clock.advance(time.Minute)
	_, status = tracker.Lookup("key")
	require.Equal(t, StatusHit, status)

	clock.advance(time.Nanosecond)
	participant, status = tracker.Lookup("key")
	require.Equal(t, StatusExpired, status)
	require.Equal(t, NoParticipant, participant)
	require.Zero(t, tracker.Len())
	_, status = tracker.Lookup("key")
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
		tracker.Record(key, "host")
		clock.advance(time.Second)
	}
	tracker.Record("a", "host-2")
	clock.advance(time.Second)
	tracker.Record("d", "host")

	_, evicted := tracker.Lookup("b")
	require.Equal(t, StatusNew, evicted)
	refreshed, status := tracker.Lookup("a")
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-2", refreshed)
	require.Equal(t, 3, tracker.Len())

	limits.MaxEntries = 1
	clock.advance(time.Second)
	tracker.Record("e", "host")
	require.Equal(t, 1, tracker.Len())
	_, status = tracker.Lookup("e")
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Record two keys, advance the clock past the TTL, and record a third.
//  2. Assert the expired two were shed on the way, not left to a lookup.
func TestRecordingShedsWhatHasExpired(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("a", "host")
	tracker.Record("b", "host")
	clock.advance(2 * time.Minute)

	tracker.Record("c", "host")

	require.Equal(t, 1, tracker.Len())
}

// Test flow:
//  1. Record a key, advance 30 minutes, record the same winner again, and advance 31 minutes more under a one-hour pin age and a two-hour TTL.
//  2. Assert the lookup answers `StatusPinAged` although the last win was 31 minutes ago, and the entry is gone.
//  3. Look up again and assert `StatusNew`, then record the key and assert it is a hit that outlives the first pin's age.
func TestAPinEndsAtItsMaximumAgeWhateverTheActivity(t *testing.T) {
	limits := &Limits{TTL: 2 * time.Hour, MaxEntries: 10, MaxPinAge: time.Hour}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a")
	clock.advance(30 * time.Minute)
	tracker.Record("key", "host-a")
	clock.advance(31 * time.Minute)

	participant, status := tracker.Lookup("key")
	require.Equal(t, StatusPinAged, status)
	require.Equal(t, NoParticipant, participant)
	require.Zero(t, tracker.Len())
	_, status = tracker.Lookup("key")
	require.Equal(t, StatusNew, status)

	tracker.Record("key", "host-a")
	clock.advance(45 * time.Minute)
	participant, status = tracker.Lookup("key")
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
	tracker.Record("key", "host-a")
	clock.advance(50 * time.Minute)
	tracker.Record("key", "host-b")
	clock.advance(50 * time.Minute)

	participant, status := tracker.Lookup("key")
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-b", participant)

	clock.advance(11 * time.Minute)
	_, status = tracker.Lookup("key")
	require.Equal(t, StatusPinAged, status)
}

// Test flow:
//  1. Record a key, advance past the pin age with no lookup in between, and record the same winner again.
//  2. Advance 45 minutes under a one-hour pin age.
//  3. Assert the key is a hit: a win after the age is a new pin, not a refresh of the old one.
func TestAWinAfterTheMaximumAgeRepins(t *testing.T) {
	limits := &Limits{TTL: 2 * time.Hour, MaxEntries: 10, MaxPinAge: time.Hour}
	tracker, clock := newTestTracker(limits, nil)
	tracker.Record("key", "host-a")
	clock.advance(61 * time.Minute)
	tracker.Record("key", "host-a")
	clock.advance(45 * time.Minute)

	_, status := tracker.Lookup("key")

	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Under a cap of two and four equal participants, record eight keys for host-0, the floor every participant may always hold.
//  2. Record a ninth and assert its lookup answers `StatusShareCapped`, and the next lookup `StatusNew`.
//  3. Assert host-0 still holds eight pins and an existing key it wins again stays a hit.
//  4. Give each of the other three participants eight keys, so host-0's share of live entries has fallen below the cap.
//  5. Record the ninth key for host-0 again and assert it is a hit.
func TestAParticipantCannotHoldMoreThanItsShareOfTheEntries(t *testing.T) {
	limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 2}
	tracker, _ := newTestTracker(limits, equalWeights("host-0", "host-1", "host-2", "host-3"))
	recordMany(tracker, "k", 8, "host-0")

	tracker.Record("ninth", "host-0")
	_, status := tracker.Lookup("ninth")
	require.Equal(t, StatusShareCapped, status)
	_, status = tracker.Lookup("ninth")
	require.Equal(t, StatusNew, status)
	require.Equal(t, 8, tracker.Len())
	tracker.Record("k-0", "host-0")
	_, status = tracker.Lookup("k-0")
	require.Equal(t, StatusHit, status)

	for _, other := range []string{"host-1", "host-2", "host-3"} {
		recordMany(tracker, other, 8, other)
	}
	tracker.Record("ninth", "host-0")
	_, status = tracker.Lookup("ninth")
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Under a cap of two, weigh a big participant nine slots and a small one a single slot, and record eight keys for the small one, its floor.
//  2. Record a ninth for the small one and assert `StatusShareCapped`.
//  3. Give the big participant 40 keys, so the small one holds 8 of 48 against a fair share of a tenth.
//  4. Record the ninth again and assert it is a hit, and that the big participant's 41st is a hit too.
func TestTheShareFollowsTheSlotsAParticipantHolds(t *testing.T) {
	weights := func() map[string]int { return map[string]int{"big": 9, "small": 1} }
	limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 2}
	tracker, _ := newTestTracker(limits, weights)
	recordMany(tracker, "small", 8, "small")

	tracker.Record("ninth", "small")
	_, status := tracker.Lookup("ninth")
	require.Equal(t, StatusShareCapped, status)

	recordMany(tracker, "big", 40, "big")
	tracker.Record("ninth", "small")
	_, status = tracker.Lookup("ninth")
	require.Equal(t, StatusHit, status)
	tracker.Record("big-last", "big")
	_, status = tracker.Lookup("big-last")
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Build a tracker with no weights and record 50 keys for one participant.
//  2. Assert every key is a hit, since the share cap needs the fleet's weights.
//  3. Build one whose weights answer nothing and assert the same.
func TestWithoutWeightsNoShareCapApplies(t *testing.T) {
	for _, weights := range []Weights{nil, func() map[string]int { return nil }} {
		limits := &Limits{TTL: time.Hour, MaxEntries: 1000, ShareCap: 1}
		tracker, _ := newTestTracker(limits, weights)

		recordMany(tracker, "k", 50, "host-0")

		for index := range 50 {
			_, status := tracker.Lookup(Key(fmt.Sprintf("k-%d", index)))
			require.Equal(t, StatusHit, status)
		}
	}
}

// Test flow:
//  1. Fill host-0 with eight pins under a cap of two and four equal participants.
//  2. Move one of its keys to host-1, and record a new key for host-0.
//  3. Assert the new key is a hit, since the moved key freed one of host-0's pins.
//  4. Remove host-0's entries by the TTL and assert it can hold eight again.
func TestAPinThatMovesOrExpiresFreesTheShare(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 1000, ShareCap: 2}
	tracker, clock := newTestTracker(limits, equalWeights("host-0", "host-1", "host-2", "host-3"))
	recordMany(tracker, "k", 8, "host-0")
	tracker.Record("k-0", "host-1")

	tracker.Record("fresh", "host-0")
	_, status := tracker.Lookup("fresh")
	require.Equal(t, StatusHit, status)

	clock.advance(2 * time.Minute)
	tracker.Record("later", "host-1")
	recordMany(tracker, "again", 8, "host-0")
	for index := range 8 {
		_, status = tracker.Lookup(Key(fmt.Sprintf("again-%d", index)))
		require.Equal(t, StatusHit, status)
	}
}
