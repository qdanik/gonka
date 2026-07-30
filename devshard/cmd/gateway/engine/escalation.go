package engine

import (
	"time"

	"devshard/cmd/gateway/config"
)

// Backstops bound a request every tunable already failed to bound, so they are not tunable.
const (
	streamingHardTimeout      = 20 * time.Minute
	nonStreamNoContentTimeout = 20 * time.Minute
	nonStreamMaxAttemptWait   = 30 * time.Minute
)

// Admitting a very large prompt is itself work, so such a host gets twice as long to receipt.
const receiptTimeoutDoubleAboveTokens = 100_000

// EscalationStage names the condition that earned an attempt one more attempt beside it.
type EscalationStage string

const (
	StageNone           EscalationStage = ""
	StageSuspicious     EscalationStage = "suspicious_host_immediate_escalation"
	StageAttemptFailed  EscalationStage = "attempt_failed"
	StageReceiptTimeout EscalationStage = "receipt_timeout_wait_elapsed"
	StageFirstToken     EscalationStage = "first_token_timeout_wait_elapsed"
)

const (
	StartPrimarySuspicious = "primary_suspicious"
	StartReceiptTimeout    = "receipt_timeout"
)

func (s EscalationStage) Reason() string {
	switch s {
	case StageSuspicious:
		return "suspicious_host"
	case StageAttemptFailed:
		return "attempt_failed"
	case StageReceiptTimeout:
		return "receipt_timeout"
	case StageFirstToken:
		return "first_token_timeout"
	}
	return ""
}

// The ladder is pure over its arguments: no host performance, no phase snapshot, no clock of its own.
type EscalationPolicy struct {
	ReceiptTimeout         time.Duration
	FirstTokenFloor        time.Duration
	InterChunkStall        time.Duration
	LoserGrace             time.Duration
	MaxSpeculativeAttempts int
}

func EscalationPolicyFromConfig(engine config.Engine) EscalationPolicy {
	return EscalationPolicy{
		ReceiptTimeout:         time.Duration(engine.ReceiptTimeoutMS) * time.Millisecond,
		FirstTokenFloor:        time.Duration(engine.FirstTokenFloorMS) * time.Millisecond,
		InterChunkStall:        time.Duration(engine.InterChunkStallMS) * time.Millisecond,
		LoserGrace:             time.Duration(engine.LoserGraceMS) * time.Millisecond,
		MaxSpeculativeAttempts: int(engine.MaxSpeculativeAttempts),
	}
}

type EscalationRequest struct {
	InputTokens uint64
	Stream      bool
}

type EscalationAttempt struct {
	Suspicious    bool
	Escalated     bool
	Done          bool
	Crowned       bool
	Stalled       bool
	NonceFinished bool
	SendTime      time.Time
	ReceiptTime   time.Time
	FirstToken    time.Time
	FirstContent  time.Time
	LastChunk     time.Time
	Completed     time.Time
}

type StartPlan struct {
	ImmediateAttempts int // including the primary
	Reason            string
}

// ArmedEscalation is a deadline to arm, deliberately not a permission to escalate: an attempt's
// stage advances while its timer runs (a receipt landing under the receipt timeout), and escalating
// on the armed stage would start a needless attempt on every healthy request. Confirm converts it.
type ArmedEscalation struct {
	Attempt  int
	Stage    EscalationStage
	Deadline time.Time
}

type ConfirmedEscalation struct {
	Attempt int
	Stage   EscalationStage
}

func (p EscalationPolicy) Decide(budget int, primarySuspicious bool) StartPlan {
	if primarySuspicious && budget > 1 {
		return StartPlan{ImmediateAttempts: 2, Reason: StartPrimarySuspicious}
	}
	return StartPlan{ImmediateAttempts: 1, Reason: StartReceiptTimeout}
}

// AttemptBudget caps how many attempts one race may hold. Scarce nonces force a single attempt: a
// speculative one spends a nonce the chain phase the gateway is serving through will not replace.
func (p EscalationPolicy) AttemptBudget(hostCount int, nonceScarce bool) int {
	if hostCount < 1 {
		return 1
	}
	if nonceScarce {
		return 1
	}
	if p.MaxSpeculativeAttempts < 1 || p.MaxSpeculativeAttempts > hostCount {
		return hostCount
	}
	return p.MaxSpeculativeAttempts
}

func (p EscalationPolicy) NextEscalation(now time.Time, attempts []EscalationAttempt, request EscalationRequest) (ArmedEscalation, bool) {
	var earliest ArmedEscalation
	found := false
	for index, attempt := range attempts {
		armed, ok := p.triggerFor(attempt, request, now)
		if !ok {
			continue
		}
		armed.Attempt = index
		if !found || armed.Deadline.Before(earliest.Deadline) {
			earliest, found = armed, true
		}
	}
	return earliest, found
}

func (p EscalationPolicy) Confirm(armed ArmedEscalation, now time.Time, attempts []EscalationAttempt, request EscalationRequest) (ConfirmedEscalation, bool) {
	if armed.Attempt < 0 || armed.Attempt >= len(attempts) {
		return ConfirmedEscalation{}, false
	}
	current, ok := p.triggerFor(attempts[armed.Attempt], request, now)
	if !ok || current.Stage != armed.Stage || now.Before(current.Deadline) {
		return ConfirmedEscalation{}, false
	}
	return ConfirmedEscalation{Attempt: armed.Attempt, Stage: armed.Stage}, true
}

func (p EscalationPolicy) triggerFor(attempt EscalationAttempt, request EscalationRequest, now time.Time) (ArmedEscalation, bool) {
	switch {
	case attempt.Escalated:
		return ArmedEscalation{}, false
	case attempt.Suspicious:
		return ArmedEscalation{Stage: StageSuspicious, Deadline: now}, true
	case attempt.Done && attempt.NonceFinished:
		return ArmedEscalation{}, false
	case attempt.Done && !request.Stream:
		return ArmedEscalation{}, false
	case attempt.Done:
		return ArmedEscalation{Stage: StageAttemptFailed, Deadline: now}, true
	case attempt.SendTime.IsZero():
		return ArmedEscalation{}, false
	case attempt.ReceiptTime.IsZero():
		deadline := attempt.SendTime.Add(p.receiptTimeout(request.InputTokens))
		return ArmedEscalation{Stage: StageReceiptTimeout, Deadline: deadline}, true
	case !request.Stream:
		return ArmedEscalation{}, false
	case !attempt.FirstToken.IsZero():
		return ArmedEscalation{}, false
	}
	deadline := attempt.SendTime.Add(p.firstTokenTimeout(request.InputTokens))
	return ArmedEscalation{Stage: StageFirstToken, Deadline: deadline}, true
}

func (p EscalationPolicy) receiptTimeout(inputTokens uint64) time.Duration {
	if inputTokens > receiptTimeoutDoubleAboveTokens {
		return 2 * p.ReceiptTimeout
	}
	return p.ReceiptTimeout
}

// The quadratic is the measured first-token fit over prompt size; the floor keeps a short prompt
// from escalating before a healthy host has plausibly started.
func (p EscalationPolicy) firstTokenTimeout(inputTokens uint64) time.Duration {
	tokens := float64(inputTokens)
	seconds := 1.7 + 0.00003*tokens + 0.0000000005*tokens*tokens
	return max(p.FirstTokenFloor, time.Duration(seconds*float64(time.Second)))
}
