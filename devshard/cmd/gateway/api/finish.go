package api

import (
	"strings"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

func hostClockOffset(outcome engine.RaceOutcome) (offsetMS, roundTripMS int64, stamped bool) {
	for _, attempt := range outcome.Attempts {
		if !outcome.IsWinner(attempt) {
			continue
		}
		offset, measured := engine.ClockOffset(attempt)
		if !measured {
			continue
		}
		return offset.Milliseconds(), attempt.ReceiptTime.Sub(attempt.SendTime).Milliseconds(), true
	}
	return 0, 0, false
}

func loggedHosts(outcome engine.RaceOutcome) string {
	for _, attempt := range outcome.Attempts {
		if outcome.IsWinner(attempt) {
			return logkey.ShortHost(attempt.Participant)
		}
	}
	tried := make([]string, 0, len(outcome.Attempts))
	for _, attempt := range outcome.Attempts {
		if short := logkey.ShortHost(attempt.Participant); short != "" {
			tried = append(tried, short)
		}
	}
	return strings.Join(tried, ",")
}

// winnerOutputTokens is what the client actually received, which with the line's own timestamp is what
// a measured output rate is computed from.
func winnerOutputTokens(outcome engine.RaceOutcome) int64 {
	for _, attempt := range outcome.Attempts {
		if outcome.IsWinner(attempt) {
			return attempt.UsageCompletionTokens
		}
	}
	return 0
}

// logRequestFinished answers the one question a finished request can no longer be asked: how much
// reached the client and whether the terminator went with it. A delivery error is the difference
// between a reply the client read and one it is still waiting out its own timeout for.
// Input tokens ride along because every escalation deadline is computed from them.
func logRequestFinished(requestID string, normalized filters.Result, outcome engine.RaceOutcome, verdict string, stream *clientStream, elapsed time.Duration, raceErr, deliverErr error) {
	written, terminated := stream.delivered()
	fields := []any{
		logkey.Request, requestID,
		logkey.Model, normalized.Model,
		logkey.Escrow, outcome.EscrowID,
		logkey.Stream, normalized.ClientStream,
		logkey.InputTokens, outcome.InputTokens,
		logkey.OutputTokens, winnerOutputTokens(outcome),
		logkey.Host, loggedHosts(outcome),
		logkey.Outcome, verdict,
		logkey.Bytes, written,
		logkey.Terminated, terminated,
		logkey.DurationMS, elapsed.Milliseconds(),
	}
	if offsetMS, roundTripMS, stamped := hostClockOffset(outcome); stamped {
		fields = append(fields, logkey.HostClockOffsetMS, offsetMS, logkey.HostReceiptMS, roundTripMS)
	}
	if raceErr != nil {
		fields = append(fields, logkey.Error, loggedError(raceErr))
	}
	if deliverErr != nil {
		fields = append(fields, logkey.DeliverError, loggedError(deliverErr))
	}
	if raceErr != nil || deliverErr != nil {
		logging.Warn("request finished", fields...)
		return
	}
	logging.Info("request finished", fields...)
}

// estimatePromptTokens is the limiter's and the perf buckets' input size, not a tokenizer call. See
// gateway-request-lifecycle.md, "6. Admission to capacity".
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
