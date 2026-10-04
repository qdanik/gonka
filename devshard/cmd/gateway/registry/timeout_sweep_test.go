package registry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/types"
	"devshard/user"
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

// Test flow:
//  1. Build a registry with one escrow whose session has one execution timeout due, in each table case's phase.
//  2. Sweep execution timeouts.
//  3. Assert the active escrow is swept and an escrow finalizing or settled is left alone.
func TestSweepVotesOnlyWhileTheSessionIsActive(t *testing.T) {
	cases := []struct {
		name      string
		phase     types.SessionPhase
		wantSwept [3]int
	}{
		{name: "active", phase: types.PhaseActive, wantSwept: [3]int{1, 1, 0}},
		{name: "finalizing", phase: types.PhaseFinalizing, wantSwept: [3]int{0, 0, 0}},
		{name: "settled", phase: types.PhaseSettlement, wantSwept: [3]int{0, 0, 0}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			session := newFakeSession("participant-a")
			session.sweepReport = user.SweepReport{Due: 1, Applied: 1}
			registry := New(Deps{ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open, Membership: newRecordingMembership(), Now: fixedClock()})
			require.NoError(t, registry.Add(context.Background(), "1", "qwen"))
			session.setPhase(testCase.phase)

			due, applied, failed := registry.SweepExecutionTimeouts(context.Background(), time.Minute, 4)

			require.Equal(t, testCase.wantSwept, [3]int{due, applied, failed})
		})
	}
}

// Test flow:
//  1. Build a registry with one active escrow whose session runs a probe while the sweep votes on it.
//  2. Sweep execution timeouts; inside the vote, read Routable, Candidates and IsBusy, then retire the escrow.
//  3. Assert the sweep counted no active user but did report the escrow busy, and the retired session stayed open during the vote.
//  4. Assert the session closes once the sweep releases its hold.
func TestSweepHoldsTheSessionOpenAndBusyWithoutCountingAsARequest(t *testing.T) {
	session := newFakeSession("participant-a")
	session.sweepReport = user.SweepReport{Due: 1, Applied: 1}
	registry := New(Deps{ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open, Membership: newRecordingMembership(), Now: fixedClock()})
	require.NoError(t, registry.Add(context.Background(), "1", "qwen"))
	session.setPhase(types.PhaseActive)
	var routableUsers, candidateUsers int
	var busyDuringSweep bool
	var closeCallsDuringSweep int64
	session.onSweep = func() {
		routable, _ := registry.Routable("1")
		routableUsers = routable.ActiveUsers
		candidates := registry.Candidates("qwen")
		require.Len(t, candidates, 1)
		candidateUsers = candidates[0].ActiveUsers
		busyDuringSweep = registry.IsBusy("1")
		require.NoError(t, registry.Retire("1"))
		closeCallsDuringSweep = session.closeCalls.Load()
	}

	registry.SweepExecutionTimeouts(context.Background(), time.Minute, 4)
	awaitDrainClose(t, registry, func() bool { return session.closeCalls.Load() == 1 })

	require.Equal(t, 0, routableUsers, "Routable(1).ActiveUsers during the sweep")
	require.Equal(t, 0, candidateUsers, "Candidates(qwen)[0].ActiveUsers during the sweep")
	require.True(t, busyDuringSweep, "IsBusy(1) during the sweep")
	require.Equal(t, int64(0), closeCallsDuringSweep, "Close calls while the sweep holds the retired escrow")
	require.Equal(t, int64(1), session.closeCalls.Load(), "Close calls after the sweep released")
}
