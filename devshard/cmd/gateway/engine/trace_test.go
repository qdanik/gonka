package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
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
