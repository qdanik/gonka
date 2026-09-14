package journal

import (
	"strings"
	"unicode/utf8"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
)

const (
	// loggedFieldSlots is every keyval a finished request can carry, so the line is never copied to grow.
	loggedFieldSlots = 32

	// maxLoggedErrorBytes bounds host-controlled error text. See docs/operations.md, "The request record".
	maxLoggedErrorBytes = 256

	cacheHitOutcome = "cache_hit"
)

// renderRequestFinished writes both shapes: the short one of a cache hit, and the record of a raced request.
func renderRequestFinished(lines logSink, line *RequestLine) {
	if line.CacheHit {
		lines.Info("request finished", logkey.Request, line.RequestID, logkey.Model, line.Model,
			logkey.Escrow, line.EscrowID, logkey.Stream, line.ClientStream, logkey.Outcome, cacheHitOutcome, logkey.Bytes, line.Bytes)
		return
	}
	fields := requestFinishedFields(line)
	if line.RaceErr != nil || line.DeliverErr != nil {
		lines.Warn("request finished", fields...)
		return
	}
	lines.Info("request finished", fields...)
}

func renderRequestThrottled(lines logSink, line *RequestLine) {
	lines.Warn("gateway limiter turned a request away", logkey.Request, line.RequestID,
		logkey.Model, line.Model, logkey.Reason, line.LimiterReason)
}

// renderReplyNotCached names a truncated answer, which nothing else does. See docs/operations.md, "Lines that mean something happened".
func renderReplyNotCached(lines logSink, line *RequestLine) {
	lines.Warn("a host stopped mid-answer: reply served, not cached", logkey.Request, line.RequestID,
		logkey.Model, line.Model, logkey.Escrow, line.EscrowID)
}

// requestFinishedFields builds the line in a slice reserved for the widest one a finished request can carry.
func requestFinishedFields(line *RequestLine) []any {
	outcome := line.Outcome
	fields := make([]any, 0, loggedFieldSlots)
	fields = append(fields,
		logkey.Request, line.RequestID,
		logkey.Model, line.Model,
		logkey.Escrow, line.EscrowID,
		logkey.Stream, line.ClientStream,
		logkey.InputTokens, outcome.InputTokens,
		logkey.OutputTokens, winnerOutputTokens(outcome),
		logkey.Host, loggedHosts(outcome),
		logkey.Outcome, line.Verdict,
		logkey.Bytes, line.Bytes,
		logkey.Terminated, line.Terminated,
		logkey.DurationMS, line.Elapsed.Milliseconds(),
	)
	if nonceFinished, crowned := outcome.WinnerNonceFinished(); crowned {
		fields = append(fields, logkey.NonceFinished, nonceFinished)
	}
	if offsetMS, roundTripMS, stamped := hostClockOffset(outcome); stamped {
		fields = append(fields, logkey.HostClockOffsetMS, offsetMS, logkey.HostReceiptMS, roundTripMS)
	}
	if line.RaceErr != nil {
		fields = append(fields, logkey.Error, loggedError(line.RaceErr))
	}
	if line.DeliverErr != nil {
		fields = append(fields, logkey.DeliverError, loggedError(line.DeliverErr))
	}
	return fields
}

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

// winnerOutputTokens is what the client actually received, which a measured output rate is computed from.
func winnerOutputTokens(outcome engine.RaceOutcome) int64 {
	for _, attempt := range outcome.Attempts {
		if outcome.IsWinner(attempt) {
			return attempt.UsageCompletionTokens
		}
	}
	return 0
}

// loggedError cuts host-controlled text on a rune boundary. See docs/operations.md, "The request record".
func loggedError(err error) string {
	text := err.Error()
	if len(text) <= maxLoggedErrorBytes {
		return text
	}
	cut := maxLoggedErrorBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…(truncated)"
}
