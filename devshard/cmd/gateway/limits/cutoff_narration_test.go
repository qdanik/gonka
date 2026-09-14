package limits

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordedCutoff struct {
	participant  string
	model        string
	reason       string
	backoffCount int
	cutOffFor    time.Duration
}

type recordedLift struct {
	participant  string
	model        string
	backoffCount int
}

type recordingCutoffNarrator struct {
	cutoffs []recordedCutoff
	lifts   []recordedLift
}

func (n *recordingCutoffNarrator) HostCutOff(participant, model, reason string, backoffCount int, cutOffFor time.Duration) {
	n.cutoffs = append(n.cutoffs, recordedCutoff{
		participant: participant, model: model, reason: reason, backoffCount: backoffCount, cutOffFor: cutOffFor,
	})
}

func (n *recordingCutoffNarrator) HostCutOffLifted(participant, model string, backoffCount int) {
	n.lifts = append(n.lifts, recordedLift{participant: participant, model: model, backoffCount: backoffCount})
}

// The first cut-off lasts five seconds against a gauge sampled every fifteen.
func TestCutoffIsNarratedWhenItOpens(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	limiter.jitter = func(time.Duration) time.Duration { return 0 }
	narrator := &recordingCutoffNarrator{}
	limiter.SetNarrator(narrator)

	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}

	require.Equal(t, []recordedCutoff{{
		participant: "participant-a", model: "model-a", reason: "consecutive_transport_faults",
		backoffCount: 1, cutOffFor: 5 * time.Second,
	}}, narrator.cutoffs)
}

// A probe that fails reopens the breaker at once, and it is a different fact from a run of faults.
func TestAFailedProbeIsNarratedAsItsOwnReason(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}
	limiter.markHalfOpen("participant-a", "model-a")
	narrator := &recordingCutoffNarrator{}
	limiter.SetNarrator(narrator)

	limiter.OnResult("participant-a", "model-a", TransportFault)

	require.Len(t, narrator.cutoffs, 1)
	require.Equal(t, "half_open_probe_failed", narrator.cutoffs[0].reason)
}

// The close is what says the host came back, and it happens on a success the gauge may never sample.
func TestCutoffCloseIsNarrated(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}
	limiter.markHalfOpen("participant-a", "model-a")
	narrator := &recordingCutoffNarrator{}
	limiter.SetNarrator(narrator)

	limiter.OnResult("participant-a", "model-a", Success)

	require.Equal(t, []recordedLift{{participant: "participant-a", model: "model-a", backoffCount: 0}}, narrator.lifts)
}

// Ordinary traffic stays silent: a narration per result is a line per request.
func TestAnOrdinaryResultIsNotNarrated(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	narrator := &recordingCutoffNarrator{}
	limiter.SetNarrator(narrator)

	limiter.OnResult("participant-a", "model-a", Success)
	limiter.OnResult("participant-a", "model-a", Overload)
	limiter.OnResult("participant-a", "model-a", UpstreamFault)

	require.Empty(t, narrator.cutoffs)
	require.Empty(t, narrator.lifts)
}

func TestAnUnnarratedLimiterStillCutsOff(t *testing.T) {
	limiter := newTestParticipantLimiter(t)

	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}

	require.False(t, limiter.Available("participant-a", "model-a"))
}

func newTestParticipantLimiter(t *testing.T) *ParticipantLimiter {
	t.Helper()
	return NewParticipantLimiter(ParticipantConfig{
		Initial: 4, Max: 16, AfterFailures: 3,
		BaseOpen: 5 * time.Second, MaxOpen: time.Minute,
	}, time.Now)
}

// markHalfOpen puts the breaker in the state an expired cut-off leaves it in.
func (l *ParticipantLimiter) markHalfOpen(participant, model string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	state := l.stateLocked(key{participant: participant, model: model})
	state.halfOpen = true
	state.openUntil = time.Time{}
}
