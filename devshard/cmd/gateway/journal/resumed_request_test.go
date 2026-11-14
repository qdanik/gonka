package journal

import (
	"testing"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logcapture"
)

// Test flow:
//  1. Finish a request that was served from a live stream another request began, then one served from the cache by an explicit resume.
//  2. Flush the journal.
//  3. Assert each logs the short shape with the stream it resumed, the first as `resumed` and the second as `cache_hit`.
func TestAResumedRequestNamesTheStreamItContinued(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})
	outcome := engine.RaceOutcome{InputTokens: 12}

	events.RequestFinished(RequestLine{RequestID: "request-2", Model: "qwen", EscrowID: "escrow-1", ClientStream: true, Outcome: outcome, Bytes: 64, Resumed: true, ResumeOf: "request-1"})
	events.RequestFinished(RequestLine{RequestID: "request-3", Model: "qwen", EscrowID: "escrow-1", ClientStream: true, Outcome: outcome, Bytes: 32, CacheHit: true, Resumed: true, ResumeOf: "request-1"})
	events.Flush()

	lines.RequireLine(t, logcapture.Entry{Level: "info", Msg: "request finished", Fields: []any{
		"request", "request-2", "model", "qwen", "api_key_id", "", "escrow", "escrow-1", "stream", true,
		"input_tokens", uint64(12), "input_tokens_source", "", "outcome", "resumed", "bytes", int64(64),
		"resumed", true, "resume_of", "request-1",
	}})
	lines.RequireLine(t, logcapture.Entry{Level: "info", Msg: "request finished", Fields: []any{
		"request", "request-3", "model", "qwen", "api_key_id", "", "escrow", "escrow-1", "stream", true,
		"input_tokens", uint64(12), "input_tokens_source", "", "outcome", "cache_hit", "bytes", int64(32),
		"resumed", true, "resume_of", "request-1",
	}})
}
