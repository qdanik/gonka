package journal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logcapture"
)

// A vote is the only thing that undoes a charge, and the counter beside it cannot name the nonce.
func TestAFailedTimeoutVoteIsLogged(t *testing.T) {
	logged := logcapture.Install(t)
	events := newJournal(t, Settings{})

	events.RecordTimeout(engine.TimeoutEvent{
		EscrowID: "7", Participant: "gonka1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Model: "qwen",
		Nonce: 42, Kind: engine.TimeoutKindExecution, Action: engine.TimeoutActionFailed, Reason: engine.TimeoutReasonNotApplied,
	})
	events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "timeout vote failed", Fields: []any{
		"escrow", "7", "nonce", uint64(42), "host", "aaaaaaaa", "model", "qwen",
		"kind", "execution", "reason", "timeout_not_applied",
	}})
}

// An escrow gone from the chain fails every vote it owed at once, and has its own line already.
func TestAVoteLostWithItsEscrowIsSilent(t *testing.T) {
	logged := logcapture.Install(t)
	events := newJournal(t, Settings{})

	events.RecordTimeout(engine.TimeoutEvent{
		EscrowID: "7", Nonce: 42, Kind: engine.TimeoutKindExecution,
		Action: engine.TimeoutActionFailed, Reason: engine.TimeoutReasonEscrowGone,
	})
	events.Flush()

	require.Empty(t, logged.All(), "the escrow's own line already says this, once instead of per nonce")
}

func TestAPostedVoteIsSilent(t *testing.T) {
	logged := logcapture.Install(t)
	events := newJournal(t, Settings{})

	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "7", Nonce: 42, Action: engine.TimeoutActionCompleted})
	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "7", Nonce: 43, Action: engine.TimeoutActionStarted})
	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "7", Nonce: 44, Action: engine.TimeoutActionSkipped})
	events.Flush()

	require.Empty(t, logged.All())
}

// A warmup's vote reaches the ledger like a race's and writes no race line: the warmup writes its own.
func TestAFailedProbeVoteReachesTheLedgerWithoutARaceLine(t *testing.T) {
	logged := logcapture.Install(t)
	ledger := &ledgerSpy{}
	events := newJournal(t, Settings{Ledger: ledger})

	events.RecordProbeTimeout(engine.TimeoutEvent{
		EscrowID: "7", Model: "qwen", Nonce: 42, Kind: engine.TimeoutKindRefused,
		Action: engine.TimeoutActionFailed, Reason: engine.TimeoutReasonNotApplied,
	})
	events.Flush()

	require.Empty(t, logged.All())
	require.Equal(t, []string{"timeout 7 42 failed"}, ledger.arrived())
}
