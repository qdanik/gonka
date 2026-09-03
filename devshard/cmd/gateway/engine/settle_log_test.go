package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/internal/logcapture"
)

// A vote is the only thing that undoes a charge, and the counter beside it cannot name the nonce.
func TestAFailedTimeoutVoteIsLogged(t *testing.T) {
	logged := logcapture.Install(t)

	logTimeoutVote(TimeoutEvent{
		EscrowID: "7", Participant: "gonka1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Model: "qwen",
		Nonce: 42, Kind: TimeoutKindExecution, Action: TimeoutActionFailed, Reason: TimeoutReasonNotApplied,
	})

	entry, found := logged.Find("timeout vote failed")
	require.True(t, found, "a vote that never reached the chain must name the nonce it left unpaid")
	require.Equal(t, "7", logcapture.Field(entry, "escrow"))
	require.Equal(t, uint64(42), logcapture.Field(entry, "nonce"))
	require.Equal(t, TimeoutKindExecution, logcapture.Field(entry, "kind"))
	require.Equal(t, TimeoutReasonNotApplied, logcapture.Field(entry, "reason"))
}

// An escrow gone from the chain fails every vote it owed at once, and has its own line already.
func TestAVoteLostWithItsEscrowIsSilent(t *testing.T) {
	logged := logcapture.Install(t)

	logTimeoutVote(TimeoutEvent{
		EscrowID: "7", Nonce: 42, Kind: TimeoutKindExecution,
		Action: TimeoutActionFailed, Reason: TimeoutReasonEscrowGone,
	})

	require.Empty(t, logged.All(), "the escrow's own line already says this, once instead of per nonce")
}

func TestAPostedVoteIsSilent(t *testing.T) {
	logged := logcapture.Install(t)

	logTimeoutVote(TimeoutEvent{EscrowID: "7", Nonce: 42, Action: TimeoutActionCompleted})
	logTimeoutVote(TimeoutEvent{EscrowID: "7", Nonce: 43, Action: TimeoutActionStarted})
	logTimeoutVote(TimeoutEvent{EscrowID: "7", Nonce: 44, Action: TimeoutActionSkipped})

	require.Empty(t, logged.All())
}
