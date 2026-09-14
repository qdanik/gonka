package perf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordedReturn struct {
	participant string
	model       string
}

type recordingHostNarrator struct {
	withheld            []Withholding
	returned            []recordedReturn
	contextLimits       [][2]uint64
	toolsUnsupported    []string
	versionsUnsupported []string
}

func (n *recordingHostNarrator) HostWithheld(withheld Withholding) {
	n.withheld = append(n.withheld, withheld)
}

func (n *recordingHostNarrator) HostReturned(participant, model string, _ int) {
	n.returned = append(n.returned, recordedReturn{participant: participant, model: model})
}

func (n *recordingHostNarrator) HostContextLimit(_, _ string, contextLimit, previousContextLimit uint64) {
	n.contextLimits = append(n.contextLimits, [2]uint64{contextLimit, previousContextLimit})
}

func (n *recordingHostNarrator) HostToolsUnsupported(participant, model string) {
	n.toolsUnsupported = append(n.toolsUnsupported, participant+"/"+model)
}

func (n *recordingHostNarrator) HostVersionUnsupported(participant string) {
	n.versionsUnsupported = append(n.versionsUnsupported, participant)
}

// The first rung lasts thirty seconds against a gauge sampled every fifteen, so the whole withholding can pass between two scrapes.
func TestEjectionIsNarratedWhenItStarts(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))
	narrator := &recordingHostNarrator{}
	tracker.SetNarrator(narrator)

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold)

	require.Len(t, narrator.withheld, 1, "an ejection nobody hears about cannot be explained afterwards")
	withheld := narrator.withheld[0]
	require.Equal(t, "participant-a", withheld.Participant)
	require.Equal(t, "model-a", withheld.Model)
	require.Equal(t, "consecutive_failures", withheld.Reason)
	require.Equal(t, 1, withheld.EjectionCount)
	require.Equal(t, int(perf.ConsecutiveFailThreshold), withheld.ConsecutiveFailures)
	require.Equal(t, 30*time.Second, withheld.WithheldFor)
}

// The return matters as much as the withholding: it is what says the fleet recovered.
func TestReturnToRoutingIsNarrated(t *testing.T) {
	perf := noCap(testPerf())
	instant := testEpoch
	tracker := newTestTracker(perf, func() time.Time { return instant })
	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold)
	narrator := &recordingHostNarrator{}
	tracker.SetNarrator(narrator)

	instant = instant.Add(time.Duration(perf.EjectionBaseSeconds)*time.Second + time.Second)
	tracker.RecordSample(Sample{ParticipantKey: "participant-a", Model: "model-a", Responsive: true})

	require.Equal(t, []recordedReturn{{participant: "participant-a", model: "model-a"}}, narrator.returned)
}

// A failure that changes nothing says nothing: a narration per sample would be a line per request.
func TestASampleThatChangesNothingIsNotNarrated(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))
	narrator := &recordingHostNarrator{}
	tracker.SetNarrator(narrator)

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold-1)

	require.Empty(t, narrator.withheld)
}

// Only the smallest context a host admits is news; a larger refusal later does not lift the bound.
func TestACapabilityRefusalIsNarratedOnceAndOnlyWhenItTightens(t *testing.T) {
	tracker := newTestTracker(testPerf(), fixedNow(testEpoch))
	narrator := &recordingHostNarrator{}
	tracker.SetNarrator(narrator)

	tracker.RecordContextLimit("participant-a", "model-a", 8192)
	tracker.RecordContextLimit("participant-a", "model-a", 16384)
	tracker.RecordContextLimit("participant-a", "model-a", 4096)
	tracker.RecordToolUnsupported("participant-a", "model-a")
	tracker.RecordToolUnsupported("participant-a", "model-a")
	tracker.RecordVersionUnsupported("participant-a")
	tracker.RecordVersionUnsupported("participant-a")

	require.Equal(t, [][2]uint64{{8192, 0}, {4096, 8192}}, narrator.contextLimits)
	require.Equal(t, []string{"participant-a/model-a"}, narrator.toolsUnsupported)
	require.Equal(t, []string{"participant-a"}, narrator.versionsUnsupported)
}

// Most tests build a tracker with no journal; routing must still honour the ejection.
func TestAnUnnarratedTrackerStillWithholds(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold)

	require.True(t, tracker.Ejected("participant-a", "model-a"))
}
