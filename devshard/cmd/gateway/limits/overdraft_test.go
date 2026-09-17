package limits

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// inflightInput reads what a host has taken on its input window the way a reader outside the package does.
func inflightInput(l *ParticipantLimiter, participant, model string) int64 {
	for _, window := range l.Snapshot() {
		if window.Participant == participant && window.Model == model {
			return window.InflightInputTokens
		}
	}
	return -1
}

// The forced send exists because a burned nonce costs more than a queued request, so the window is told
// rather than asked — and the tokens it spends are still counted, or the window it crossed means nothing.
func TestAnOverdraftCrossesAFullWindowAndStillCountsItsTokens(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	l := newTestLimiter(cfg, fixedNow(testEpoch))

	for range cfg.Pricing.Input.Initial {
		require.True(t, l.admits("p", "m"), "the window has to fill before an overdraft means anything")
	}
	require.Equal(t, AdmissionWindowFull, l.Admits("p", "m"))

	release, admitted := l.Overdraft("p", "m", oneToken)
	require.Equal(t, AdmissionOpen, admitted, "an overdraft crosses a full window rather than asking it")
	require.Equal(t, cfg.Pricing.Input.Initial+1, inflightInput(l, "p", "m"), "an overdraft is in flight like any other request")

	release()
	require.Equal(t, cfg.Pricing.Input.Initial, inflightInput(l, "p", "m"), "an overdraft gives its tokens back")
}

// A cut-off host is broken rather than busy, and forcing work onto it spends the nonce the rung was saving.
func TestAnOverdraftIsRefusedByAnOpenCutOff(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	l := newTestLimiter(cfg, newMovingClock(testEpoch).now)

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	require.Equal(t, AdmissionCutOff, l.Admits("p", "m"))

	_, admitted := l.Overdraft("p", "m", oneToken)
	require.Equal(t, AdmissionCutOff, admitted, "an overdraft is the window's business and never the cut-off's")
}

// A half-open host is spending its single probe, and a second request would answer the probe's question for it.
func TestAnOverdraftIsRefusedWhileAHalfOpenProbeIsInFlight(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	clock.advance(l.states[key{participant: "p", model: "m"}].openUntil.Sub(clock.now()) + time.Millisecond)

	probe, admitted := l.Acquire("p", "m", oneToken)
	require.Equal(t, AdmissionOpen, admitted, "an elapsed cut-off leaves one probe to take")
	require.Equal(t, AdmissionCutOff, l.Admits("p", "m"), "a probe in flight is the cut-off still deciding")

	_, second := l.Overdraft("p", "m", oneToken)
	require.Equal(t, AdmissionCutOff, second, "the probe is the whole traffic a half-open host gets")

	probe()
	_, afterTheProbe := l.Overdraft("p", "m", oneToken)
	require.Equal(t, AdmissionOpen, afterTheProbe, "a half-open host with nothing in flight takes work again")
}
