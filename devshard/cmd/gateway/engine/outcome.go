// Package engine races escalated attempts for one inference request and reports what happened
// through a single RaceOutcome.
package engine

import (
	"time"

	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
)

const longResponseExemption = 280 * time.Second

// Terminal is an attempt's classified end state. Every downstream vocabulary — limiter verdict,
// perf sample, metric label — is a total function of it, so the HTTP status recovery and SSE
// inspection that decide it happen once, where the error and the bytes are.
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
	// TerminalRejected is every other upstream status, 400 and 500 included: those describe the
	// request or the model, not the host's ability to serve, so they move nothing.
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
)

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
)

// Lifecycle carries escrow facts the engine observes and must not act on.
type Lifecycle struct {
	EscrowMissing    bool
	BalanceExhausted bool
}

type RaceOutcome struct {
	RequestID   string
	EscrowID    string
	Model       string
	InputTokens uint64
	Stream      bool
	Decision    string

	WinnerNonce uint64
	Succeeded   bool
	Attempts    []AttemptOutcome

	PoCBypassActive bool
	Lifecycle       Lifecycle
}

// AttemptOutcome is what one attempt ended up doing. ErrorPayload holds the host's own error event
// verbatim, so a refusal reaches the client unrewritten.
type AttemptOutcome struct {
	Participant string
	HostIdx     int
	HostLabel   string
	Nonce       uint64
	Role        string
	StartReason string
	Suspicious  bool

	SendTime    time.Time
	ReceiptTime time.Time
	FirstToken  time.Time
	Completed   time.Time

	ContentChunks         int64
	UsageCompletionTokens int64

	Terminal            Terminal
	Confirmed           bool
	NonceFinished       bool
	FailureRateExceeded bool

	ContentSource string
	ErrorSource   string
	ErrorCode     string
	ErrorType     string
	ErrorMessage  string
	ErrorPayload  string

	PhaseTransitionAborted bool
	StateDivergent         bool
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
	case TerminalThrottled, TerminalUnavailable:
		return limits.Overload, true
	case TerminalForbidden, TerminalNotFound, TerminalTimestampDrift,
		TerminalDialFailure, TerminalStreamTruncated, TerminalUnexpectedEOF, TerminalStalled:
		return limits.TransportFault, true
	// An empty stream is what the model produced, not what the host failed to carry, so the host's
	// window must not contract for it; DeniesCrowning is where the host still answers for it.
	case TerminalEmptyStream, TerminalBurnEmpty, TerminalErrorStream, TerminalCapabilityRefused:
		return limits.ModelOutcome, true
	}
	return limits.ModelOutcome, false
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

func (o RaceOutcome) longResponseExempt(a AttemptOutcome) bool {
	return !a.NonceFinished && a.ContentChunks > 0 && a.elapsed() >= longResponseExemption
}

func (o RaceOutcome) longNonStreamEmptyExempt(a AttemptOutcome) bool {
	return !o.Stream && a.emptyStream() && a.elapsed() >= longResponseExemption
}

func (o RaceOutcome) responsive(a AttemptOutcome) bool {
	if o.longNonStreamEmptyExempt(a) {
		return true
	}
	return a.Confirmed && a.NonceFinished && !a.emptyStream()
}

// sampleExemption is the whole ladder: the ordered reasons an attempt contributes no perf sample,
// and therefore never counts toward its host's ejection.
func (o RaceOutcome) sampleExemption(a AttemptOutcome) SampleExemption {
	switch {
	case a.SendTime.IsZero():
		return ExemptNeverDispatched
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
	// The race cancels its own losers, and Verdict already refuses to move a window for that. A sample
	// would say the host was unresponsive when it was told to stop.
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
	}, SampleRecorded
}

func (o RaceOutcome) Verdict(a AttemptOutcome) (limits.Verdict, bool) {
	switch {
	case a.PhaseTransitionAborted || a.StateDivergent:
		return limits.ModelOutcome, false
	case o.longResponseExempt(a) || o.longNonStreamEmptyExempt(a):
		return limits.ModelOutcome, false
	case a.emptyStream() && o.PoCBypassActive:
		return limits.ModelOutcome, false
	case a.Terminal == TerminalStalled && !a.FailureRateExceeded:
		return limits.ModelOutcome, false
	case (a.Terminal == TerminalWon || a.Terminal == TerminalLost) && !o.responsive(a):
		return limits.ModelOutcome, false
	}
	return a.Terminal.verdict()
}

// DeniesCrowning reports that a host produced nothing while claiming to serve, and so may keep
// receiving attempts but must not be crowned until it produces content again.
func (o RaceOutcome) DeniesCrowning(a AttemptOutcome) bool {
	if a.PhaseTransitionAborted || o.PoCBypassActive {
		return false
	}
	return a.Terminal == TerminalEmptyStream && !o.longNonStreamEmptyExempt(a)
}

func (o RaceOutcome) Labels(a AttemptOutcome) AttemptLabels {
	role := a.Role
	if role == "" {
		role = "primary"
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
	}
	return labels
}

// Label values the engine emits. metrics reads these rather than restating them, so a rename here
// cannot silently empty a dashboard panel.
const (
	AttemptOutcomeSuccess = "success"
	AttemptOutcomeFailed  = "failed"

	VisibilityWinner            = "user_visible_winner"
	VisibilityNoWinner          = "no_winner"
	VisibilitySuppressedLoser   = "suppressed_loser"
	VisibilityFailedNotFinished = "failed_not_finished"
)

func (o RaceOutcome) visibility(a AttemptOutcome, served bool) string {
	switch {
	case served && o.WinnerNonce != 0 && a.Nonce == o.WinnerNonce:
		return VisibilityWinner
	case a.Suspicious:
		return VisibilityNoWinner
	case served:
		return VisibilitySuppressedLoser
	}
	return VisibilityFailedNotFinished
}

func (o RaceOutcome) failureReason(a AttemptOutcome) string {
	if a.PhaseTransitionAborted {
		return "phase_transition_aborted"
	}
	if reason := a.Terminal.reason(); reason != "" {
		return reason
	}
	if !a.NonceFinished {
		return "not_finished"
	}
	return "unknown"
}
