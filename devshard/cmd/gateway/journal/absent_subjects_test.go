package journal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logcapture"
	"devshard/cmd/gateway/scheduler"
)

// The last eight characters of each are what logkey.ShortHost renders.
const (
	hostAlpha = "gonka1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hostBravo = "gonka1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// A race whose first pick failed ran on no escrow and asked no host; an empty value reads as a subject with no name.
func TestARequestThatReachedNoEscrowNamesNoEscrowAndNoHost(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.RequestFinished(RequestLine{
		RequestID: "request-1", Model: "qwen", ClientStream: true,
		Outcome: engine.RaceOutcome{Model: "qwen"},
		Verdict: "failed_before_first_byte", Elapsed: time.Second, RaceErr: engine.ErrAllAttemptsFailed,
	})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "request finished", Fields: []any{
		"request", "request-1", "model", "qwen", "stream", true,
		"input_tokens", uint64(0), "output_tokens", int64(0),
		"outcome", "failed_before_first_byte", "bytes", int64(0), "terminated", false, "duration_ms", int64(1000),
		"error", "every attempt failed",
	}})
	require.Len(t, lines.All(), 1)
}

// With nobody crowned there is no winner to name, only the hosts that were asked.
func TestARequestNobodyWonNamesEveryHostItTried(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.RequestFinished(RequestLine{
		RequestID: "request-2", Model: "qwen", EscrowID: "escrow-1", ClientStream: true,
		Outcome: engine.RaceOutcome{Model: "qwen", InputTokens: 12, Attempts: []engine.AttemptOutcome{
			{Participant: hostAlpha, Nonce: 5},
			{Participant: hostBravo, Nonce: 6},
		}},
		Verdict: "failed_before_first_byte", Elapsed: 2 * time.Second, RaceErr: engine.ErrAllAttemptsFailed,
	})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "request finished", Fields: []any{
		"request", "request-2", "model", "qwen", "escrow", "escrow-1", "stream", true,
		"input_tokens", uint64(12), "output_tokens", int64(0), "hosts", "aaaaaaaa,bbbbbbbb",
		"outcome", "failed_before_first_byte", "bytes", int64(0), "terminated", false, "duration_ms", int64(2000),
		"error", "every attempt failed",
	}})
}

// host names the crowned attempt only; the loser beside it is not the answer the client got.
func TestARequestWithAWinnerNamesOnlyTheWinner(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.RequestFinished(RequestLine{
		RequestID: "request-3", Model: "qwen", EscrowID: "escrow-1", ClientStream: true,
		Outcome: engine.RaceOutcome{Model: "qwen", InputTokens: 12, WinnerNonce: 6, Succeeded: true, Attempts: []engine.AttemptOutcome{
			{Participant: hostAlpha, Nonce: 5},
			{Participant: hostBravo, Nonce: 6, NonceFinished: true, UsageCompletionTokens: 40},
		}},
		Verdict: "served", Bytes: 512, Terminated: true, Elapsed: 3 * time.Second,
	})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "info", Msg: "request finished", Fields: []any{
		"request", "request-3", "model", "qwen", "escrow", "escrow-1", "stream", true,
		"input_tokens", uint64(12), "output_tokens", int64(40), "host", "bbbbbbbb",
		"outcome", "served", "bytes", int64(512), "terminated", true, "duration_ms", int64(3000),
		"nonce_finished", true,
	}})
}

// Nonce 0 names no record; writing it sends an operator looking for a nonce that was never committed.
func TestABurnWithoutANonceNamesNone(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.GhostBurned("escrow-1", scheduler.Burn{Participant: hostAlpha, Reason: scheduler.GhostReasonThrottled})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{
		"escrow", "escrow-1", "host", "aaaaaaaa", "reason", scheduler.GhostReasonThrottled,
	}})
}

func TestABurnWithANonceNamesIt(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.GhostBurned("escrow-1", scheduler.Burn{Nonce: 42, Participant: hostAlpha, Reason: scheduler.GhostReasonThrottled})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{
		"escrow", "escrow-1", "nonce", uint64(42), "host", "aaaaaaaa", "reason", scheduler.GhostReasonThrottled,
	}})
}
