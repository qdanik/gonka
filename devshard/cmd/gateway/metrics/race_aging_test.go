package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/engine"
)

const testHostStaleness = time.Hour

// handClock is moved by the test; the concurrent test reads and moves it from many goroutines.
type handClock struct {
	mu      sync.Mutex
	current time.Time
}

func (c *handClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *handClock) advance(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = c.current.Add(by)
}

func newAgingRecorder(telemetry *Metrics, clock *handClock, staleness time.Duration) *RaceRecorder {
	return NewRaceRecorder(telemetry, clock.now, func() time.Duration { return staleness })
}

// recordEveryParticipantSeries writes each of the thirteen participant-labelled families at least once for one participant.
func recordEveryParticipantSeries(recorder *RaceRecorder, participant string) {
	winner := winningAttempt()
	winner.Participant = participant
	winner.MaxChunkGap, winner.MeanChunkGap = 2*time.Second, 40*time.Millisecond
	failed := engine.AttemptOutcome{
		Participant: participant, Nonce: 12, Role: engine.RoleSpeculative, StartReason: engine.EscalationReasonFirstToken,
		SendTime: at(0), Terminal: engine.TerminalUpstreamServerError, UpstreamStatus: 502,
	}
	recorder.RecordRace(engine.RaceOutcome{
		Model: "qwen", InputTokens: 100, Decision: "primary", WinnerNonce: winner.Nonce,
		Succeeded: true, Attempts: []engine.AttemptOutcome{winner, failed},
	})
	recorder.RecordTimeout(engine.TimeoutEvent{
		Participant: participant, Model: "qwen",
		Kind: engine.TimeoutKindExecution, Action: engine.TimeoutActionCompleted, Reason: engine.TimeoutReasonNone,
	})
	recorder.RecordClassifyOverflow(participant, "qwen")
}

func TestAParticipantUnseenPastTheStalenessWindowLosesEverySeries(t *testing.T) {
	clock := &handClock{current: raceStart}
	telemetry := New()
	recorder := newAgingRecorder(telemetry, clock, testHostStaleness)
	recordEveryParticipantSeries(recorder, "gonka1gone")
	recordEveryParticipantSeries(recorder, "gonka1stays")
	require.Equal(t, 13, familiesCarrying(t, telemetry, "participant_key", "gonka1gone"))

	clock.advance(testHostStaleness + time.Minute)
	recordEveryParticipantSeries(recorder, "gonka1stays")

	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1gone"))
	require.Equal(t, 13, familiesCarrying(t, telemetry, "participant_key", "gonka1stays"))
}

// Pairs age out over an hour, so scanning them on every write would cost the response path for nothing.
func TestTheSweepRunsAtMostOncePerTenthOfTheStalenessWindow(t *testing.T) {
	clock := &handClock{current: raceStart}
	telemetry := New()
	recorder := newAgingRecorder(telemetry, clock, testHostStaleness)
	recordEveryParticipantSeries(recorder, "gonka1gone")
	clock.advance(58 * time.Minute)
	recordEveryParticipantSeries(recorder, "gonka1stays")

	clock.advance(3 * time.Minute)
	recordEveryParticipantSeries(recorder, "gonka1stays")
	require.Equal(t, 13, familiesCarrying(t, telemetry, "participant_key", "gonka1gone"),
		"three minutes after the last sweep is inside a tenth of the window, so nothing may be swept yet")

	clock.advance(3 * time.Minute)
	recordEveryParticipantSeries(recorder, "gonka1stays")
	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1gone"))
}

// A vote can settle after its host went quiet: the write brings the pair back, and the pair ages out again.
func TestALateTimeoutVoteRecreatesAForgottenParticipantUntilItAgesOutAgain(t *testing.T) {
	clock := &handClock{current: raceStart}
	telemetry := New()
	recorder := newAgingRecorder(telemetry, clock, testHostStaleness)
	recordEveryParticipantSeries(recorder, "gonka1late")
	clock.advance(testHostStaleness + time.Minute)
	recordEveryParticipantSeries(recorder, "gonka1stays")
	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1late"))

	recorder.RecordTimeout(engine.TimeoutEvent{
		Participant: "gonka1late", Model: "qwen",
		Kind: engine.TimeoutKindExecution, Action: engine.TimeoutActionCompleted, Reason: engine.TimeoutReasonNone,
	})
	require.Equal(t, 1, familiesCarrying(t, telemetry, "participant_key", "gonka1late"))

	clock.advance(testHostStaleness + time.Minute)
	recordEveryParticipantSeries(recorder, "gonka1stays")

	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1late"))
}

func TestAgingStaysConsistentUnderConcurrentWritesAndSweeps(t *testing.T) {
	clock := &handClock{current: raceStart}
	telemetry := New()
	recorder := newAgingRecorder(telemetry, clock, 10*time.Second)
	participants := []string{"gonka1a", "gonka1b", "gonka1c", "gonka1d"}

	// The writers move the clock 8 s in total, short of the 10 s window: sweeps run among the writes, yet no pair can age out before the last write.
	var writers sync.WaitGroup
	for _, participant := range participants {
		writers.Go(func() {
			for range 200 {
				clock.advance(10 * time.Millisecond)
				recordEveryParticipantSeries(recorder, participant)
			}
		})
	}
	writers.Wait()

	clock.advance(20 * time.Second)
	recorder.RecordClassifyOverflow("gonka1survivor", "qwen")

	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1a"))
	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1b"))
	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1c"))
	require.Zero(t, familiesCarrying(t, telemetry, "participant_key", "gonka1d"))
	require.Equal(t, 1, familiesCarrying(t, telemetry, "participant_key", "gonka1survivor"))
}
