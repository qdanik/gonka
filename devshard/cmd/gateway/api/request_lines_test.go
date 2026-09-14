package api

import (
	"net/http"
	"testing"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logcapture"
	"devshard/cmd/gateway/limits"
)

func TestAServedRequestWritesItsRecord(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)

	response := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "info", Msg: "request finished", Fields: []any{
		"request", "request-1", "model", "qwen", "escrow", "7", "stream", false,
		"input_tokens", uint64(0), "output_tokens", int64(0),
		"outcome", "served", "bytes", int64(response.Body.Len()), "terminated", false, "duration_ms", int64(0),
	}})
}

// A crowned winner adds whether its nonce closed and how far its host's clock sits from the gateway's.
func TestAServedRequestWithAWinnerWritesTheWinnersFacts(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)
	live.inference.outcome = benchOutcome()
	live.inference.outcome.EscrowID = "7"

	response := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "info", Msg: "request finished", Fields: []any{
		"request", "request-1", "model", "qwen", "escrow", "7", "stream", false,
		"input_tokens", uint64(1024), "output_tokens", int64(384), "host", "qqqqqqqq",
		"outcome", "served", "bytes", int64(response.Body.Len()), "terminated", false, "duration_ms", int64(0),
		"nonce_finished", false, "host_clock_offset_ms", int64(400), "host_receipt_ms", int64(200),
	}})
}

func TestARaceThatFailedBeforeItsFirstByteWritesItsRecordAtWarn(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)
	live.inference.reply = ""
	live.inference.outcome = engine.RaceOutcome{EscrowID: "7"}
	live.inference.err = engine.ErrAllAttemptsFailed

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "request finished", Fields: []any{
		"request", "request-1", "model", "qwen", "escrow", "7", "stream", false,
		"input_tokens", uint64(0), "output_tokens", int64(0),
		"outcome", "failed_before_first_byte", "bytes", int64(0), "terminated", false, "duration_ms", int64(0),
		"error", "every attempt failed",
	}})
}

func TestAStreamThatFailedMidAnswerWritesItsRecordAtWarn(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)
	live.inference.chunks = []string{`data: {"choices":[{"index":0,"delta":{"content":"half"}}]}` + "\n\n"}
	live.inference.outcome = engine.RaceOutcome{EscrowID: "7"}
	live.inference.err = engine.ErrAllAttemptsFailed

	response := live.request(t, http.MethodPost, "/v1/chat/completions", streamChatBody, nil)
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "request finished", Fields: []any{
		"request", "request-1", "model", "qwen", "escrow", "7", "stream", true,
		"input_tokens", uint64(0), "output_tokens", int64(0),
		"outcome", "failed_mid_stream", "bytes", int64(response.Body.Len()), "terminated", true, "duration_ms", int64(0),
		"error", "every attempt failed",
	}})
}

func TestACacheHitWritesTheShortRecord(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	replay := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "info", Msg: "request finished", Fields: []any{
		"request", "request-1", "model", "qwen", "escrow", "7", "stream", false,
		"outcome", "cache_hit", "bytes", int64(replay.Body.Len()),
	}})
}

func TestALimiterRefusalIsLoggedWithTheCapItHit(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)
	live.limiter.err = &limits.RateLimitError{Reason: "too many concurrent requests"}

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "gateway limiter turned a request away", Fields: []any{
		"request", "request-1", "model", "qwen", "reason", "concurrent_requests",
	}})
}

// The refusal is the only place a truncated answer is named, so this line is the operator's contract.
func TestAHostThatStoppedMidAnswerIsLogged(t *testing.T) {
	logged := logcapture.Install(t)
	live := newHarness(t)
	live.inference.chunks = []string{`data: {"choices":[{"index":0,"delta":{"reasoning":"still working"}}]}` + "\n\n"}

	live.request(t, http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))
	live.events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "a host stopped mid-answer: reply served, not cached", Fields: []any{
		"request", "request-1", "model", "qwen", "escrow", "7",
	}})
}
