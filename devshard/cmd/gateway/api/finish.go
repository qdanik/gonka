package api

import (
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

// executorStampTruncation is the second the executor's Unix() stamp rounds away, always downward, so
// half of it is added back before the comparison: without it a correct clock reads half a second slow.
const executorStampTruncation = time.Second

// hostClockOffset reads the receipt, which the executor signs before inference: `created` would carry
// a busy host's admission and prefill delay too. The stamp landed somewhere inside the round trip, so
// it is compared against that window's midpoint rather than against the dispatch, which would charge
// the host for the outbound leg. The executor stamps whole seconds, leaving under a second of it.
func hostClockOffset(outcome engine.RaceOutcome) (offsetMS, roundTripMS int64, stamped bool) {
	for _, attempt := range outcome.Attempts {
		if !outcome.IsWinner(attempt) || attempt.ConfirmedAt == 0 {
			continue
		}
		if attempt.SendTime.IsZero() || !attempt.ReceiptTime.After(attempt.SendTime) {
			continue
		}
		roundTrip := attempt.ReceiptTime.Sub(attempt.SendTime)
		midpoint := attempt.SendTime.Add(roundTrip / 2)
		stamped := time.Unix(attempt.ConfirmedAt, 0).Add(executorStampTruncation / 2)
		return stamped.Sub(midpoint).Milliseconds(),
			roundTrip.Milliseconds(), true
	}
	return 0, 0, false
}

func winnerParticipant(outcome engine.RaceOutcome) string {
	for _, attempt := range outcome.Attempts {
		if outcome.IsWinner(attempt) {
			return attempt.Participant
		}
	}
	return ""
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
		logkey.Host, winnerParticipant(outcome),
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
