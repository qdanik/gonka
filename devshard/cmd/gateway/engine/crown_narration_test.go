package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type recordedCrownDenial struct {
	participant string
	model       string
	strikes     int
}

type recordingCrownNarrator struct {
	denials      []recordedCrownDenial
	restorations []string
}

func (n *recordingCrownNarrator) HostDeniedCrown(participant, model string, strikes int) {
	n.denials = append(n.denials, recordedCrownDenial{participant: participant, model: model, strikes: strikes})
}

func (n *recordingCrownNarrator) HostCrownedAgain(participant, _ string) {
	n.restorations = append(n.restorations, participant)
}

// A denied host keeps drawing nonces and starts a second attempt beside itself, so the denial is a spend nothing else reports.
func TestCrownDenialIsNarratedWhenItStarts(t *testing.T) {
	narrator := &recordingCrownNarrator{}
	strikes := newCrownStrikes(narrator)

	for range crownDenialStrikes {
		strikes.Observe("participant-a", "model-a", true)
	}

	require.Equal(t, []recordedCrownDenial{{participant: "participant-a", model: "model-a", strikes: crownDenialStrikes}}, narrator.denials)
	require.True(t, strikes.Denied("participant-a", "model-a"))
}

// The narration belongs to the crossing, not to every strike after it.
func TestFurtherStrikesAreNotNarrated(t *testing.T) {
	narrator := &recordingCrownNarrator{}
	strikes := newCrownStrikes(narrator)

	for range crownDenialStrikes + 2 {
		strikes.Observe("participant-a", "model-a", true)
	}

	require.Len(t, narrator.denials, 1, "a host already denied the crown is not news twice")
}

// One answer with content restores the crown, and that is the recovery an operator waits for.
func TestCrownRestoredIsNarrated(t *testing.T) {
	narrator := &recordingCrownNarrator{}
	strikes := newCrownStrikes(narrator)
	for range crownDenialStrikes {
		strikes.Observe("participant-a", "model-a", true)
	}

	strikes.Observe("participant-a", "model-a", false)

	require.Equal(t, []string{"participant-a"}, narrator.restorations)
	require.False(t, strikes.Denied("participant-a", "model-a"))
}

// A host that never lost the crown says nothing when it answers with content, which is every request.
func TestAnOrdinaryAnswerIsNotNarrated(t *testing.T) {
	narrator := &recordingCrownNarrator{}
	strikes := newCrownStrikes(narrator)

	strikes.Observe("participant-a", "model-a", false)
	strikes.Observe("participant-a", "model-a", true)

	require.Empty(t, narrator.denials)
	require.Empty(t, narrator.restorations)
}

// An engine built without a journal still denies the crown at the threshold.
func TestStrikesWithoutANarratorStillDeny(t *testing.T) {
	strikes := newCrownStrikes(nil)

	for range crownDenialStrikes {
		strikes.Observe("participant-a", "model-a", true)
	}

	require.True(t, strikes.Denied("participant-a", "model-a"))
}
