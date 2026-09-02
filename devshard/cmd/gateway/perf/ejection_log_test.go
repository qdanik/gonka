package perf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/internal/logcapture"
)

// The first rung lasts thirty seconds against a gauge sampled every fifteen, so the whole withholding
// can pass between two scrapes.
func TestEjectionIsLoggedWhenItStarts(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))
	logged := logcapture.Install(t)

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold)

	entry, found := logged.Find("host withheld from routing")
	require.True(t, found, "an ejection that leaves no line cannot be explained afterwards")
	require.Equal(t, "model-a", logcapture.Field(entry, "model"))
	require.Equal(t, "consecutive_failures", logcapture.Field(entry, "reason"))
	require.Equal(t, 1, logcapture.Field(entry, "ejection_count"))
	require.Equal(t, int(perf.ConsecutiveFailThreshold), logcapture.Field(entry, "consecutive_failures"))
}

// The return matters as much as the withholding: it is what says the fleet recovered.
func TestReturnToRoutingIsLogged(t *testing.T) {
	perf := noCap(testPerf())
	instant := testEpoch
	tracker := newTestTracker(perf, func() time.Time { return instant })
	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold)
	logged := logcapture.Install(t)

	instant = instant.Add(time.Duration(perf.EjectionBaseSeconds)*time.Second + time.Second)
	tracker.RecordSample(Sample{ParticipantKey: "participant-a", Model: "model-a", Responsive: true})

	entry, found := logged.Find("host back in routing")
	require.True(t, found, "a host that came back leaves the operator reading a stale ejection")
	require.Equal(t, "model-a", logcapture.Field(entry, "model"))
}

// A failure that changes nothing stays silent: a line per sample would be a line per request.
func TestASampleThatChangesNothingIsSilent(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))
	logged := logcapture.Install(t)

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold-1)

	_, found := logged.Find("host withheld from routing")
	require.False(t, found, "nothing was withheld yet, so nothing may be said")
}
