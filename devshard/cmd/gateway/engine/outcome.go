// Package engine races escalated attempts for one inference request and reports what happened
// through a single RaceOutcome.
package engine

import (
	"net/http"
	"time"

	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/types"
)

const (
	// Label values the engine emits. metrics reads these rather than restating them. See
	// gateway-invariants.md, "10. Labels, ordering and determinism".
	AttemptOutcomeSuccess = "success"
	AttemptOutcomeFailed  = "failed"

	VisibilityWinner            = "user_visible_winner"
	VisibilityWinnerClientGone  = "winner_client_gone"
	VisibilityNoWinner          = "no_winner"
	VisibilitySuppressedLoser   = "suppressed_loser"
	VisibilityFailedNotFinished = "failed_not_finished"

	// reasonCrownDenied names an answer that arrived complete and was given to nobody.
	reasonCrownDenied = "crown_denied"

	longResponseExemption = 280 * time.Second

	// Below this an empty stream is the model's output; at or above it the host held the request past the
	// refusal point and returned nothing.
	emptyStreamHeldTooLong = types.DefaultRefusalTimeoutSeconds * time.Second
)

// Terminal is an attempt's classified end state. Every downstream vocabulary — limiter verdict,
// perf sample, metric label — is a total function of it. See gateway-speculative-race.md, "The outcome".
type Terminal int

const (
	TerminalUnclassified Terminal = iota
	TerminalWon
	TerminalLost
	TerminalThrottled
	TerminalUnavailable
	TerminalForbidden
	TerminalNotFound
	TerminalTimestampDrift
	// TerminalRejected is every other upstream status, 400 and 500 included. See
	// gateway-speculative-race.md, "The outcome".
	TerminalRejected
	TerminalOffPath
	TerminalDialFailure
	TerminalStreamTruncated
	TerminalUnexpectedEOF
	TerminalClientCancelled
	TerminalNoReceipt
	TerminalEmptyStream
	TerminalBurnEmpty
	TerminalErrorStream
	TerminalCapabilityRefused
	TerminalStalled
	TerminalHardTimeout
)

var (
	// terminalStatuses is the one table linking an upstream status to the terminal it produces.
	// StatusFor reads it forward for metrics and classifyDispatchError reads the derived inverse, so
	// the two cannot disagree about which status a label describes.
	terminalStatuses = map[Terminal]int{
		TerminalThrottled:      http.StatusTooManyRequests,
		TerminalUnavailable:    http.StatusServiceUnavailable,
		TerminalForbidden:      http.StatusForbidden,
		TerminalNotFound:       http.StatusNotFound,
		TerminalTimestampDrift: http.StatusUnauthorized,
	}

	// terminalForStatus is the inverse, built once so a status can never map to one terminal going out
	// and a different one coming back.
	terminalForStatus = func() map[int]Terminal {
		inverse := make(map[int]Terminal, len(terminalStatuses))
		for terminal, status := range terminalStatuses {
			inverse[status] = terminal
		}
		return inverse
	}()
)

// StatusFor reports the upstream status a terminal was recovered from, and false for the terminals
// that never carried one.
func StatusFor(terminal Terminal) (int, bool) {
	status, known := terminalStatuses[terminal]
	return status, known
}

type SampleExemption int

const (
	SampleRecorded SampleExemption = iota
	ExemptPhaseAborted
	ExemptErrorStream
	ExemptStateDivergent
	ExemptLongResponse
	ExemptPoCSuppressed
	ExemptEmptyStreamNoWinner
	ExemptNeverDispatched
	ExemptClientCancelled
	ExemptNeverReported
)

// Lifecycle carries escrow facts the engine observes and must not act on.
type Lifecycle struct {
	EscrowMissing    bool
	BalanceExhausted bool
	ClientGone       bool
}

type RaceOutcome struct {
	RequestID    string
	EscrowID     string
	Model        string
	InputTokens  uint64
	Decision     string
	ClientStream bool

	WinnerNonce uint64
	Succeeded   bool
	Attempts    []AttemptOutcome

	PoCBypassActive bool
	Lifecycle       Lifecycle
}

// AttemptOutcome is what one attempt ended up doing. ErrorPayload holds the host's own error event
// verbatim, so a refusal reaches the client unrewritten. StartedAt is the race's, not the attempt's:
// verifiers recompute a refusal deadline from the committed record, so every nonce a request commits
// must carry the one stamp, dispatched or stranded.
type AttemptOutcome struct {
	Participant string
	HostIdx     int
	HostLabel   string
	Nonce       uint64
	Role        string
	StartReason string
	Suspicious  bool

	StartedAt    time.Time
	SendTime     time.Time
	ReceiptTime  time.Time
	FirstToken   time.Time
	FirstContent time.Time
	LastChunk    time.Time
	Completed    time.Time

	ContentChunks         int64
	StreamChunks          int64
	OutputBytes           int64
	UsageCompletionTokens int64
	HostCreated           int64
	MaxChunkGap           time.Duration
	MaxChunkGapAt         int64
	MeanChunkGap          time.Duration
	DroppedEvents         int64

	Terminal            Terminal
	Confirmed           bool
	ConfirmedAt         int64
	NonceFinished       bool
	FailureRateExceeded bool

	// Capability is a refusal read off the dispatch error, where the SSE error fields stay empty.
	Capability     CapabilitySignal
	ContentSource  string
	UpstreamStatus int
	UpstreamBody   string

	ErrorSource  string
	ErrorCode    string
	ErrorType    string
	ErrorMessage string
	ErrorPayload string

	PhaseTransitionAborted bool
	StateDivergent         bool
}

const executorStampTruncation = time.Second

// The stamp landed somewhere inside the round trip, so it is compared against that window's midpoint
// rather than the dispatch, which would charge the host for the outbound leg; half a second is added
// back because the executor stamps whole seconds downward.
func ClockOffset(attempt AttemptOutcome) (time.Duration, bool) {
	if attempt.ConfirmedAt == 0 || attempt.SendTime.IsZero() || !attempt.ReceiptTime.After(attempt.SendTime) {
		return 0, false
	}
	midpoint := attempt.SendTime.Add(attempt.ReceiptTime.Sub(attempt.SendTime) / 2)
	stamped := time.Unix(attempt.ConfirmedAt, 0).Add(executorStampTruncation / 2)
	return stamped.Sub(midpoint), true
}

type AttemptLabels struct {
	Participant string
	Model       string
	Role        string
	Outcome     string
	Visibility  string
	Reason      string
}

func (t Terminal) verdict() (limits.Verdict, bool) {
	switch t {
	case TerminalWon, TerminalLost:
		return limits.Success, true
	case TerminalThrottled, TerminalUnavailable, TerminalHardTimeout:
		return limits.Overload, true
	case TerminalForbidden, TerminalNotFound, TerminalTimestampDrift,
		TerminalDialFailure, TerminalStreamTruncated, TerminalUnexpectedEOF, TerminalStalled:
		return limits.TransportFault, true
	case TerminalEmptyStream, TerminalBurnEmpty, TerminalErrorStream, TerminalCapabilityRefused:
		return limits.ModelOutcome, true
	}
	return limits.ModelOutcome, false
}

// String names the terminal. reason() answers a different question -- why an attempt failed -- and is
// empty for the two outcomes that are not failures, which failureReason relies on to fall through.
// The failure names come from reason() rather than a second list, so the two cannot drift apart.
func (t Terminal) String() string {
	switch t {
	case TerminalWon:
		return "won"
	case TerminalLost:
		return "lost"
	case TerminalUnclassified:
		return "unclassified"
	}
	if reason := t.reason(); reason != "" {
		return reason
	}
	return "unnamed"
}

func (t Terminal) reason() string {
	switch t {
	case TerminalWon, TerminalLost:
		return ""
	case TerminalThrottled:
		return "http_429"
	case TerminalUnavailable:
		return "http_503"
	case TerminalForbidden:
		return "http_forbidden"
	case TerminalNotFound:
		return "http_not_found"
	case TerminalTimestampDrift:
		return "http_timestamp_drift"
	case TerminalRejected:
		return "http_error"
	case TerminalOffPath:
		return "off_path"
	case TerminalDialFailure:
		return "transport_error"
	case TerminalStreamTruncated:
		return "sse_truncated"
	case TerminalUnexpectedEOF:
		return "eof_transport"
	case TerminalClientCancelled:
		return "client_cancelled"
	case TerminalNoReceipt:
		return "no_receipt"
	case TerminalEmptyStream, TerminalBurnEmpty:
		return "empty_stream"
	case TerminalErrorStream, TerminalCapabilityRefused:
		return "error_stream"
	case TerminalStalled:
		return "stalled"
	case TerminalHardTimeout:
		return "hard_timeout"
	}
	return "unknown"
}

func (a AttemptOutcome) emptyStream() bool {
	return a.Terminal == TerminalEmptyStream || a.Terminal == TerminalBurnEmpty
}

func (a AttemptOutcome) elapsed() time.Duration {
	if a.SendTime.IsZero() || a.Completed.IsZero() {
		return 0
	}
	return a.Completed.Sub(a.SendTime)
}

// longResponseExempt gates on ContentSource, not ContentChunks, which counts error events too. See
// gateway-invariants.md, "1. A committed nonce is always settled".
func (o RaceOutcome) longResponseExempt(a AttemptOutcome) bool {
	if a.Terminal == TerminalHardTimeout {
		return false
	}
	return !a.NonceFinished && a.ContentSource != "" && a.elapsed() >= longResponseExemption
}

// responsive decides whether a host earns a positive perf sample. A long non-stream reply is exempt
// from being judged slow, but an empty one earns nothing: crediting a host that held the request for
// the whole window and returned no content teaches the router to prefer it. See
// gateway-speculative-race.md, "The exemption ladder".
func (o RaceOutcome) responsive(a AttemptOutcome) bool {
	return a.Confirmed && a.NonceFinished && !a.emptyStream()
}

// sampleExemption is the whole ladder: the ordered reasons an attempt contributes no perf sample,
// and therefore never counts toward its host's ejection.
func (o RaceOutcome) sampleExemption(a AttemptOutcome) SampleExemption {
	switch {
	case a.SendTime.IsZero():
		return ExemptNeverDispatched
	// An attempt that never reported is one the race stopped listening for, so it says nothing about
	// the host: judging it would charge a host for our own cancellation.
	case a.Terminal == TerminalUnclassified:
		return ExemptNeverReported
	case a.PhaseTransitionAborted:
		return ExemptPhaseAborted
	case a.Terminal == TerminalErrorStream || a.Terminal == TerminalCapabilityRefused:
		return ExemptErrorStream
	case a.StateDivergent:
		return ExemptStateDivergent
	case o.longResponseExempt(a):
		return ExemptLongResponse
	case a.emptyStream() && o.PoCBypassActive:
		return ExemptPoCSuppressed
	case a.emptyStream() && !o.Succeeded:
		return ExemptEmptyStreamNoWinner
	// The race cancels its own losers; the sample and verdict ladders deliberately disagree here. See
	// gateway-speculative-race.md, "The exemption ladder".
	case a.Terminal == TerminalClientCancelled:
		return ExemptClientCancelled
	}
	return SampleRecorded
}

func (o RaceOutcome) Sample(a AttemptOutcome) (perf.Sample, SampleExemption) {
	if exemption := o.sampleExemption(a); exemption != SampleRecorded {
		return perf.Sample{}, exemption
	}
	return perf.Sample{
		ParticipantKey: a.Participant,
		Model:          o.Model,
		Responsive:     o.responsive(a),
		FirstContent:   a.firstContentDelay(),

		TimePerOutputToken: TimePerOutputToken(a),
	}, SampleRecorded
}

func (o RaceOutcome) Verdict(a AttemptOutcome) (limits.Verdict, bool) {
	switch {
	case a.PhaseTransitionAborted || a.StateDivergent:
		return limits.ModelOutcome, false
	case o.longResponseExempt(a):
		return limits.ModelOutcome, false
	case a.emptyStream() && o.PoCBypassActive:
		return limits.ModelOutcome, false
	case a.emptyStream() && a.elapsed() >= emptyStreamHeldTooLong:
		return limits.Overload, true
	case a.Terminal == TerminalStalled && !a.FailureRateExceeded:
		return limits.ModelOutcome, false
	case (a.Terminal == TerminalWon || a.Terminal == TerminalLost) && !o.responsive(a):
		return limits.ModelOutcome, false
	}
	return a.Terminal.verdict()
}

// DeniesCrowning reports that a host produced nothing while claiming to serve. See
// gateway-speculative-race.md, "Crown denial".
func (o RaceOutcome) DeniesCrowning(a AttemptOutcome) bool {
	if a.PhaseTransitionAborted || o.PoCBypassActive {
		return false
	}
	return a.Terminal == TerminalEmptyStream
}

func (o RaceOutcome) Labels(a AttemptOutcome) AttemptLabels {
	role := a.Role
	if role == "" {
		role = RolePrimary
	}
	served := (a.Terminal == TerminalWon || a.Terminal == TerminalLost) && o.responsive(a)
	labels := AttemptLabels{
		Participant: a.Participant,
		Model:       o.Model,
		Role:        role,
		Outcome:     AttemptOutcomeFailed,
		Visibility:  o.visibility(a, served),
		Reason:      o.failureReason(a),
	}
	if served {
		labels.Outcome = AttemptOutcomeSuccess
		labels.Reason = ""
		// Blanking this reason rendered the panel's own headline case as "unknown".
		if labels.Visibility == VisibilityNoWinner {
			labels.Reason = reasonCrownDenied
		}
	}
	return labels
}

func (o RaceOutcome) visibility(a AttemptOutcome, served bool) string {
	switch {
	case served && o.IsWinner(a) && o.Lifecycle.ClientGone:
		return VisibilityWinnerClientGone
	case served && o.IsWinner(a):
		return VisibilityWinner
	case a.Suspicious:
		return VisibilityNoWinner
	case served:
		return VisibilitySuppressedLoser
	}
	return VisibilityFailedNotFinished
}

// IsWinner is the one test for "this attempt won the race". Whether a client was still there to receive
// it is Lifecycle.ClientGone, which visibility reads: the race outlives the client on purpose.
func (o RaceOutcome) IsWinner(a AttemptOutcome) bool {
	return o.WinnerNonce != 0 && a.Nonce == o.WinnerNonce
}

func (o RaceOutcome) failureReason(a AttemptOutcome) string {
	if a.PhaseTransitionAborted {
		return timeoutReasonPhaseAborted
	}
	if reason := a.Terminal.reason(); reason != "" {
		return reason
	}
	if !a.NonceFinished {
		return "not_finished"
	}
	return "unknown"
}

// TimePerOutputToken starts at the first content chunk, so prefill is not charged to decode speed.
func TimePerOutputToken(a AttemptOutcome) time.Duration {
	if a.UsageCompletionTokens <= 0 || a.FirstContent.IsZero() || !a.LastChunk.After(a.FirstContent) {
		return 0
	}
	return a.LastChunk.Sub(a.FirstContent) / time.Duration(a.UsageCompletionTokens)
}

func (a AttemptOutcome) firstContentDelay() time.Duration {
	if a.SendTime.IsZero() || !a.FirstContent.After(a.SendTime) {
		return 0
	}
	return a.FirstContent.Sub(a.SendTime)
}
