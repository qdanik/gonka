package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/scheduler"
)

// recordingJournal keeps the steps a coordinator reported.
type recordingJournal struct {
	mu    sync.Mutex
	steps []RaceStep
}

func (r *recordingJournal) RecordStep(step RaceStep) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

func (r *recordingJournal) HostDeniedCrown(string, string, int) {}

func (r *recordingJournal) HostCrownedAgain(string, string) {}

func (r *recordingJournal) recorded() []RaceStep {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RaceStep(nil), r.steps...)
}

// A step crosses to the journal's goroutine, so it is a copy the coordinator can go on writing.
func TestAFinishedAttemptIsReportedAsACopyOfWhatItDelivered(t *testing.T) {
	steps := &recordingJournal{}
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.deps.Journal = steps
	delivered := widestFinishedOutcome()
	delivered.Terminal = TerminalLost
	attempt := &liveAttempt{nonce: 77, participant: "host-3", cancel: func() {}}
	coordinator := pausedCoordinator(fixture, 1, attempt)
	coordinator.escrowID = "escrow-1"

	coordinator.complete(attempt, AttemptEvent{Kind: AttemptDone, Nonce: 77, At: testEpoch, Outcome: &delivered})
	attempt.outcome.ContentChunks = 0

	want := widestFinishedOutcome()
	want.Terminal = TerminalLost
	require.Equal(t, []RaceStep{{
		Kind: RaceStepAttemptFinished, RequestID: "request-1", EscrowID: "escrow-1", Nonce: 77,
		Participant: "host-3", Terminal: TerminalLost, HasOutcome: true, Outcome: want,
	}}, steps.recorded())
}

// A failed pick is traced by its error, not by the race's state: a refusal that answers after the served winner was handed off is still a refusal.
func TestAFailedPickIsTracedUnlessTheRaceCancelledIt(t *testing.T) {
	testCases := []struct {
		name    string
		pickErr error
		want    []RaceStep
	}{
		{
			name:    "a scheduler refusal after the hand-off",
			pickErr: scheduler.ErrNoAvailableHost,
			want: []RaceStep{{
				Kind: RaceStepEscalationUnfilled, RequestID: "request-1", EscrowID: "escrow-1",
				Reason: "receipt_timeout", Attempts: 2, Err: scheduler.ErrNoAvailableHost,
			}},
		},
		{name: "the race's own cancellation", pickErr: context.Canceled},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			steps := &recordingJournal{}
			fixture := newRaceFixture(settledPolicy(), 2)
			fixture.deps.Journal = steps
			winner := &liveAttempt{nonce: 90, participant: "host-0", done: true, cancel: func() {}}
			loser := &liveAttempt{nonce: 91, participant: "host-1", cancel: func() {}}
			coordinator := pausedCoordinator(fixture, 2, winner, loser)
			coordinator.escrowID, coordinator.winner = "escrow-1", winner
			require.True(t, coordinator.release(), "the served winner was not handed off")
			coordinator.pickCancel, coordinator.pickReason = func() {}, "receipt_timeout"

			coordinator.applyPick(pickedHost{err: testCase.pickErr})

			require.Equal(t, testCase.want, steps.recorded())
		})
	}
}
