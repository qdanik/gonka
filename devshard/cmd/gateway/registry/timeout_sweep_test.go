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

// Test flow:
//  1. Build a registry with one escrow via `registryWithOneEscrow`.
//  2. Sweep execution timeouts with a zero timeout and zero budget.
//  3. Assert due, applied, and failed counts are all zero.
func TestSweepWithoutABudgetTouchesNothing(t *testing.T) {
	registry := registryWithOneEscrow(t)

	due, applied, failed := registry.SweepExecutionTimeouts(context.Background(), 0, 0)

	require.Equal(t, [3]int{0, 0, 0}, [3]int{due, applied, failed})
}

// Test flow:
//  1. Build a registry with one escrow via `registryWithOneEscrow`.
//  2. Sweep execution timeouts with a nonzero timeout and budget.
//  3. Assert the sweep does not panic, even though the session behind that escrow has nothing to sweep.
func TestSweepStepsOverASessionWithNothingToSweep(t *testing.T) {
	registry := registryWithOneEscrow(t)

	require.NotPanics(t, func() {
		registry.SweepExecutionTimeouts(context.Background(), time.Minute, 4)
	})
}

// Test flow:
//  1. Build a registry with one escrow via `registryWithOneEscrow`.
//  2. Cancel the context before sweeping.
//  3. Sweep execution timeouts with a nonzero timeout and budget.
//  4. Assert due, applied, and failed counts are all zero: the cancelled context stops the walk before it reaches the escrow.
func TestSweepStopsWalkingOnACancelledContext(t *testing.T) {
	registry := registryWithOneEscrow(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	due, applied, failed := registry.SweepExecutionTimeouts(cancelled, time.Minute, 4)

	require.Equal(t, [3]int{0, 0, 0}, [3]int{due, applied, failed})
}
