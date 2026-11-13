package affinity

import (
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
