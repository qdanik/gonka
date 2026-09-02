package limits

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/internal/logcapture"
)

// The first cut-off lasts five seconds against a gauge sampled every fifteen.
func TestCutoffIsLoggedWhenItOpens(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	logged := logcapture.Install(t)

	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}

	entry, found := logged.Find("host cut off after transport faults")
	require.True(t, found, "a cut-off nobody can see is a host that stopped serving for no stated reason")
	require.Equal(t, "model-a", logcapture.Field(entry, "model"))
	require.Equal(t, "consecutive_transport_faults", logcapture.Field(entry, "reason"))
	require.Equal(t, 1, logcapture.Field(entry, "backoff_count"))
}

// A probe that fails reopens the breaker at once, and it is a different fact from a run of faults.
func TestAFailedProbeIsLoggedAsItsOwnReason(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}
	limiter.markHalfOpen("participant-a", "model-a")
	logged := logcapture.Install(t)

	limiter.OnResult("participant-a", "model-a", TransportFault)

	entry, found := logged.Find("host cut off after transport faults")
	require.True(t, found)
	require.Equal(t, "half_open_probe_failed", logcapture.Field(entry, "reason"))
}

// The close is what says the host came back, and it happens on a success the gauge may never sample.
func TestCutoffCloseIsLogged(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	for range int(limiter.cfg.AfterFailures) {
		limiter.OnResult("participant-a", "model-a", TransportFault)
	}
	limiter.markHalfOpen("participant-a", "model-a")
	logged := logcapture.Install(t)

	limiter.OnResult("participant-a", "model-a", Success)

	_, found := logged.Find("host back after its cut-off")
	require.True(t, found, "a host that recovered leaves the operator reading a stale cut-off")
}

// Ordinary traffic must stay silent: a line per result is a line per request.
func TestAnOrdinaryResultIsSilent(t *testing.T) {
	limiter := newTestParticipantLimiter(t)
	logged := logcapture.Install(t)

	limiter.OnResult("participant-a", "model-a", Success)
	limiter.OnResult("participant-a", "model-a", Overload)
	limiter.OnResult("participant-a", "model-a", UpstreamFault)

	require.Empty(t, logged.All(), "nothing changed state, so nothing may be said")
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
