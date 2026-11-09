package affinity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type fakeClock struct{ now time.Time }

func (c *fakeClock) advance(by time.Duration) { c.now = c.now.Add(by) }

func newTestTracker(limits *Limits) (*Tracker, *fakeClock) {
	clock := &fakeClock{now: epoch}
	return NewTracker(func() Limits { return *limits }, func() time.Time { return clock.now }), clock
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
	tracker, clock := newTestTracker(limits)

	_, status := tracker.Lookup("key")
	require.Equal(t, StatusNew, status)

	tracker.Record("key", "host-a", NoSession)
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
	tracker, clock := newTestTracker(limits)
	for _, key := range []Key{"a", "b", "c"} {
		tracker.Record(key, "host", NoSession)
		clock.advance(time.Second)
	}
	tracker.Record("a", "host-2", NoSession)
	clock.advance(time.Second)
	tracker.Record("d", "host", NoSession)

	_, evicted := tracker.Lookup("b")
	require.Equal(t, StatusNew, evicted)
	refreshed, status := tracker.Lookup("a")
	require.Equal(t, StatusHit, status)
	require.Equal(t, "host-2", refreshed)
	require.Equal(t, 3, tracker.Len())

	limits.MaxEntries = 1
	clock.advance(time.Second)
	tracker.Record("e", "host", NoSession)
	require.Equal(t, 1, tracker.Len())
	_, status = tracker.Lookup("e")
	require.Equal(t, StatusHit, status)
}

// Test flow:
//  1. Record two keys, advance the clock past the TTL, and record a third.
//  2. Assert the expired two were shed on the way, not left to a lookup.
func TestRecordingShedsWhatHasExpired(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10}
	tracker, clock := newTestTracker(limits)
	tracker.Record("a", "host", NoSession)
	tracker.Record("b", "host", NoSession)
	clock.advance(2 * time.Minute)

	tracker.Record("c", "host", NoSession)

	require.Equal(t, 1, tracker.Len())
}

// Test flow:
//  1. Record a winner under an issued session key and assert Session hands the key back.
//  2. Advance the clock past the TTL and assert Session answers NoSession.
//  3. Assert a Lookup after it still counts the entry `StatusExpired`, then `StatusNew`.
func TestSessionLeavesAnExpiredEntryForLookupToCount(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10}
	tracker, clock := newTestTracker(limits)

	tracker.Record("key", "host-a", "issued")
	require.Equal(t, "issued", tracker.Session("key"))

	clock.advance(time.Minute + time.Nanosecond)
	require.Equal(t, NoSession, tracker.Session("key"))

	_, status := tracker.Lookup("key")
	require.Equal(t, StatusExpired, status)
	_, status = tracker.Lookup("key")
	require.Equal(t, StatusNew, status)
}
