package registry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func registryWithOneEscrow(t *testing.T) *Registry {
	t.Helper()
	sessions := newSessions(map[string]*fakeSession{"1": newFakeSession("participant-a")})
	registry := New(Deps{ServingSessions: sessions.open, Membership: newRecordingMembership(), Now: fixedClock()})
	require.NoError(t, registry.Add(context.Background(), "1", "qwen"))
	return registry
}

func TestSweepWithoutABudgetTouchesNothing(t *testing.T) {
	registry := registryWithOneEscrow(t)

	due, applied, failed := registry.SweepExecutionTimeouts(context.Background(), 0, 0)

	require.Equal(t, [3]int{0, 0, 0}, [3]int{due, applied, failed})
}

// A session the registry holds without a user session behind it must be stepped over, not dereferenced.
func TestSweepStepsOverASessionWithNothingToSweep(t *testing.T) {
	registry := registryWithOneEscrow(t)

	require.NotPanics(t, func() {
		registry.SweepExecutionTimeouts(context.Background(), time.Minute, 4)
	})
}

// A cancelled context stops the walk before it holds the next escrow.
func TestSweepStopsWalkingOnACancelledContext(t *testing.T) {
	registry := registryWithOneEscrow(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	due, applied, failed := registry.SweepExecutionTimeouts(cancelled, time.Minute, 4)

	require.Equal(t, [3]int{0, 0, 0}, [3]int{due, applied, failed})
}
