package journal

import (
	"testing"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logcapture"
	"devshard/cmd/gateway/scheduler"
)

// A burn costs the escrow a nonce; the request whose drain spent it is where an operator starts.
func TestABurnNamesTheRequestItWasSpentDuring(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.GhostBurned("escrow-1", scheduler.Burn{
		Nonce: 42, Participant: hostAlpha, Reason: scheduler.GhostReasonAbandoned, RequestID: "request-7",
	})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{
		"escrow", "escrow-1", "nonce", uint64(42), "host", "aaaaaaaa", "reason", scheduler.GhostReasonAbandoned,
		"burned_during_request", "request-7",
	}})
}

// A failed vote leaves a charge standing; the request it belonged to is where an operator starts.
func TestAFailedTimeoutVoteNamesItsRequest(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.RecordTimeout(engine.TimeoutEvent{
		RequestID: "req-9", EscrowID: "7", Participant: hostAlpha, Model: "qwen", Nonce: 42,
		Kind: engine.TimeoutKindExecution, Action: engine.TimeoutActionFailed, Reason: engine.TimeoutReasonNotApplied,
	})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "timeout vote failed", Fields: []any{
		"request", "req-9", "escrow", "7", "nonce", uint64(42), "host", "aaaaaaaa", "model", "qwen",
		"kind", "execution", "reason", "timeout_not_applied",
	}})
}
