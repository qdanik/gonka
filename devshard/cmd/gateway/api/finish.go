package api

import (
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/journal"
)

// finishRequest hands the journal what a finished request can no longer be asked. See README.md, "What a finished request records".
func (s *Server) finishRequest(requestID string, normalized filters.Result, outcome engine.RaceOutcome, verdict string, stream *clientStream, elapsed time.Duration, raceErr, deliverErr error) {
	written, terminated := stream.delivered()
	s.events.RequestFinished(journal.RequestLine{
		RequestID:    requestID,
		Model:        normalized.Model,
		EscrowID:     outcome.EscrowID,
		ClientStream: normalized.ClientStream,
		Outcome:      outcome,
		Verdict:      verdict,
		Bytes:        written,
		Terminated:   terminated,
		Elapsed:      elapsed,
		RaceErr:      raceErr,
		DeliverErr:   deliverErr,
	})
}

// estimatePromptTokens is an input size, not a tokenizer call. See README.md, "What the boundary hands the engine".
func estimatePromptTokens(body []byte) uint64 {
	if estimated := (len(body) + 3) / 4; estimated > 0 {
		return uint64(estimated)
	}
	return 1
}

func outputTokenBudget(normalized filters.Result) uint64 {
	if normalized.MaxTokens > 0 {
		return normalized.MaxTokens
	}
	return normalized.MaxCompletionTokens
}
