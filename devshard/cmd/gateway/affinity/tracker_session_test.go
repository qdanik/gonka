package affinity

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test flow:
//  1. Record a winner under an issued session key and assert Session hands the key back.
//  2. Advance the clock past the TTL and assert Session answers NoSession.
//  3. Assert a Lookup after it still counts the entry `StatusExpired`, then `StatusNew`.
func TestSessionLeavesAnExpiredEntryForLookupToCount(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 10}
	tracker, clock := newTestTracker(limits, nil)

	tracker.Record("key", "host-a", "issued", 0)
	require.Equal(t, "issued", tracker.Session("key"))

	clock.advance(time.Minute + time.Nanosecond)
	require.Equal(t, NoSession, tracker.Session("key"))

	_, status := tracker.Lookup("key", 0)
	require.Equal(t, StatusExpired, status)
	_, status = tracker.Lookup("key", 0)
	require.Equal(t, StatusNew, status)
}

// Test flow:
//  1. Record a key for host-a under "K1" with 8000 tokens, under a minimum of 4096.
//  2. Record a short turn for host-b under "K2" and assert the pin and its session key stay, since host-a holds the longer prefix.
//  3. Record the same key again for host-a under "K2" and assert Session hands back "K2".
func TestTheSessionFollowsTheRecordThatKeepsTheKey(t *testing.T) {
	tracker, _ := newTestTracker(&Limits{TTL: time.Minute, MaxEntries: 10, MinCachedTokens: 4096}, nil)
	tracker.Record("key", "host-a", "K1", 8000)

	tracker.Record("key", "host-b", "K2", 3000)
	participant, _ := tracker.Lookup("key", 8000)
	require.Equal(t, "host-a", participant)
	require.Equal(t, "K1", tracker.Session("key"))

	tracker.Record("key", "host-a", "K2", 3000)
	require.Equal(t, "K2", tracker.Session("key"))
}

// Test flow:
//  1. Under a cap of 200 percent and four equal participants, record four conversations for host-0 as a key and an alias each.
//  2. Assert every alias is a hit, then record a fifth key and assert it is a hit too, since the aliases took no share and eight pins would not have fit the floor.
//  3. Advance past the TTL, shed everything with a record for host-1, and assert host-0 can again hold eight keys.
func TestAnAliasTakesNoShareOfThePins(t *testing.T) {
	limits := &Limits{TTL: time.Minute, MaxEntries: 1000, ShareCap: 200}
	tracker, clock := newTestTracker(limits, equalWeights("host-0", "host-1", "host-2", "host-3"))
	for index := range 4 {
		tracker.Record(Key(fmt.Sprintf("key-%d", index)), "host-0", "K", 0)
		tracker.RecordAlias(Key(fmt.Sprintf("alias-%d", index)), "host-0", "K", 0)
	}

	for index := range 4 {
		_, status := tracker.Lookup(Key(fmt.Sprintf("alias-%d", index)), 0)
		require.Equal(t, StatusHit, status)
	}
	tracker.Record("fifth", "host-0", "K", 0)
	_, status := tracker.Lookup("fifth", 0)
	require.Equal(t, StatusHit, status)

	clock.advance(2 * time.Minute)
	tracker.Record("later", "host-1", NoSession, 0)
	recordMany(tracker, "again", 8, "host-0")
	for index := range 8 {
		_, status = tracker.Lookup(Key(fmt.Sprintf("again-%d", index)), 0)
		require.Equal(t, StatusHit, status)
	}
}
