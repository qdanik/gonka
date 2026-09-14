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

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "timeout vote failed", Fields: []any{
		"escrow", "7", "nonce", uint64(42), "host", "aaaaaaaa", "model", "qwen",
		"kind", "execution", "reason", "timeout_not_applied",
	}})
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
