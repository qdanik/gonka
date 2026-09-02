package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/internal/logcapture"
)

// A denied host keeps drawing nonces and starts a second attempt beside itself, so the denial is a
// spend nothing else reports.
func TestCrownDenialIsLoggedWhenItStarts(t *testing.T) {
	strikes := newCrownStrikes()
	logged := logcapture.Install(t)

	for range crownDenialStrikes {
		strikes.Observe("participant-a", "model-a", true)
	}

	entry, found := logged.Find("host denied the crown")
	require.True(t, found, "a host answering nothing must not lose the crown silently")
	require.Equal(t, "model-a", logcapture.Field(entry, "model"))
	require.Equal(t, crownDenialStrikes, logcapture.Field(entry, "strikes"))
	require.True(t, strikes.Denied("participant-a", "model-a"))
}

// The line belongs to the crossing, not to every strike after it.
func TestFurtherStrikesAreSilent(t *testing.T) {
	strikes := newCrownStrikes()
	for range crownDenialStrikes {
		strikes.Observe("participant-a", "model-a", true)
	}
	logged := logcapture.Install(t)

	strikes.Observe("participant-a", "model-a", true)
	strikes.Observe("participant-a", "model-a", true)

	require.Empty(t, logged.All(), "a host already denied the crown is not news twice")
}

// One answer with content restores the crown, and that is the recovery an operator waits for.
func TestCrownRestoredIsLogged(t *testing.T) {
	strikes := newCrownStrikes()
	for range crownDenialStrikes {
		strikes.Observe("participant-a", "model-a", true)
	}
	logged := logcapture.Install(t)

	strikes.Observe("participant-a", "model-a", false)

	_, found := logged.Find("host crowned again")
	require.True(t, found)
	require.False(t, strikes.Denied("participant-a", "model-a"))
}

// A host that never lost the crown says nothing when it answers with content, which is every request.
func TestAnOrdinaryAnswerIsSilent(t *testing.T) {
	strikes := newCrownStrikes()
	logged := logcapture.Install(t)

	strikes.Observe("participant-a", "model-a", false)
	strikes.Observe("participant-a", "model-a", true)

	require.Empty(t, logged.All())
}
