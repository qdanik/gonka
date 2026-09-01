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
	longResponseExemption = 280 * time.Second

	// Below this an empty stream is the model's output; at or above it the host held it past the refusal point.
	emptyStreamHeldTooLong = types.DefaultRefusalTimeoutSeconds * time.Second
)

// Terminal is an attempt's classified end state; every downstream vocabulary is a total function of it. See race.md, "The outcome".
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
	// TerminalRejected is every other upstream status, 400 and 500 included. See race.md, "The outcome".
	TerminalRejected
	TerminalOffPath
	TerminalDialFailure
	TerminalStreamTruncated
	TerminalUnexpectedEOF
	TerminalResponseTooLarge
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
	terminalStatuses = map[Terminal]int{
		TerminalThrottled:      http.StatusTooManyRequests,
		TerminalUnavailable:    http.StatusServiceUnavailable,
		TerminalForbidden:      http.StatusForbidden,
		TerminalNotFound:       http.StatusNotFound,
		TerminalTimestampDrift: http.StatusUnauthorized,
	}

	// terminalForStatus is the derived inverse, so the two directions cannot disagree.
	terminalForStatus = func() map[int]Terminal {
		inverse := make(map[int]Terminal, len(terminalStatuses))
		for terminal, status := range terminalStatuses {
			inverse[status] = terminal
		}
		return inverse
	}()
)

// StatusFor reports the upstream status a terminal was recovered from, false for those that carried none.
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

// AttemptOutcome is what one attempt ended up doing; StartedAt is the race's, not the attempt's. See README, "Timeout votes".
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
	MaxChunkGap           time.Duration
	MaxChunkGapAt         int64
	MeanChunkGap          time.Duration
	DroppedEvents         int64

	Terminal            Terminal
	LogprobsDecoded     bool
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

// The stamp landed inside the round trip, so it is compared against that window's midpoint. See README, "Measurements".
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
	case TerminalEmptyStream, TerminalBurnEmpty, TerminalErrorStream, TerminalCapabilityRefused,
		TerminalResponseTooLarge:
		return limits.ModelOutcome, true
	}
	return limits.ModelOutcome, false
}

// String names the terminal, taking failure names from reason() rather than a second list.
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
	case TerminalResponseTooLarge:
		return "response_too_large"
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

// longResponseExempt gates on ContentSource, not ContentChunks, which counts error events too. See rules.md, "1. A committed nonce is always settled".
func (o RaceOutcome) longResponseExempt(a AttemptOutcome) bool {
	if a.Terminal == TerminalHardTimeout {
		return false
	}
	return !a.NonceFinished && a.ContentSource != "" && a.elapsed() >= longResponseExemption
}

// responsive decides whether a host earns a positive perf sample. See race.md, "The exemption ladder".
func (o RaceOutcome) responsive(a AttemptOutcome) bool {
	return a.Confirmed && a.NonceFinished && !a.emptyStream()
}

// sampleExemption is the whole ladder of reasons an attempt contributes no perf sample. See README, "The exemption ladder".
func (o RaceOutcome) sampleExemption(a AttemptOutcome) SampleExemption {
	switch {
	case a.SendTime.IsZero():
		return ExemptNeverDispatched
	// An attempt that never reported says nothing about the host: judging it would charge our own cancellation.
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
	// The race cancels its own losers; the sample and verdict ladders deliberately disagree here. See race.md, "The exemption ladder".
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
	case a.Terminal == TerminalEmptyStream && !a.NonceFinished:
		return limits.TransportFault, true
	case a.emptyStream() && a.elapsed() >= emptyStreamHeldTooLong:
		return limits.Overload, true
	case a.Terminal == TerminalStalled && !a.FailureRateExceeded:
		return limits.ModelOutcome, false
	case (a.Terminal == TerminalWon || a.Terminal == TerminalLost) && !o.responsive(a):
		return limits.ModelOutcome, false
	}
	return a.Terminal.verdict()
}

// observeCrowning reports only the attempts that say something about the host.
func (o RaceOutcome) observeCrowning(crown crownGate) {
	if crown == nil {
		return
	}
	for _, attempt := range o.Attempts {
		if !o.JudgesCrowning(attempt) {
			continue
		}
		crown.Observe(attempt.Participant, o.Model, o.DeniesCrowning(attempt))
	}
}

// JudgesCrowning reports that this attempt says something about the host's crowning. See README, "Crown denial".
func (o RaceOutcome) JudgesCrowning(a AttemptOutcome) bool {
	if a.PhaseTransitionAborted || o.PoCBypassActive {
		return false
	}
	return a.Terminal == TerminalEmptyStream ||
		((a.Terminal == TerminalWon || a.Terminal == TerminalLost) && o.responsive(a))
}

// DeniesCrowning reports that a host produced nothing while claiming to serve. See race.md, "Crown denial".
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

// IsWinner is the one test for "this attempt won the race"; whether a client saw it is Lifecycle.ClientGone.
func (o RaceOutcome) IsWinner(a AttemptOutcome) bool {
	return o.WinnerNonce != 0 && a.Nonce == o.WinnerNonce
}

func (o RaceOutcome) failureReason(a AttemptOutcome) string {
	if a.PhaseTransitionAborted {
		return TimeoutReasonPhaseAborted
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
