package engine

import (
	"time"

	"devshard/cmd/gateway/config"
)

const (
	// Backstops bound a request every tunable already failed to bound, so they are not tunable.
	streamingHardTimeout = 20 * time.Minute
	schedulerPickTimeout = 2 * time.Minute

	// Admitting a very large prompt is itself work, so such a host gets twice as long to receipt.
	receiptTimeoutDoubleAboveTokens = 100_000

	// The measured first-token curve over prompt size: a fixed cost to start answering, a per-token cost
	// to read the prompt, and a quadratic term that only matters on very large ones.
	firstTokenBaseSeconds      = 1.7
	firstTokenPerTokenSeconds  = 3e-5
	firstTokenQuadraticSeconds = 5e-10

	StartPrimarySuspicious = "primary_suspicious"
	StartPrimaryDegraded   = "primary_degraded"
	StartPrimary           = "primary"
)

// EscalationStage names the condition that earned an attempt one more attempt beside it.
type EscalationStage string

const (
	StageNone           EscalationStage = ""
	StageSuspicious     EscalationStage = "suspicious_host_immediate_escalation"
	StageAttemptFailed  EscalationStage = "attempt_failed"
	StageReceiptTimeout EscalationStage = "receipt_timeout_wait_elapsed"
	StageFirstToken     EscalationStage = "first_token_timeout_wait_elapsed"
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
	FirstTokenCeiling      time.Duration
	InterChunkStall        time.Duration
	LoserGrace             time.Duration
	MaxSpeculativeAttempts int
}

func EscalationPolicyFromConfig(engine config.Engine) EscalationPolicy {
	return EscalationPolicy{
		ReceiptTimeout:         time.Duration(engine.ReceiptTimeoutMS) * time.Millisecond,
		FirstTokenFloor:        time.Duration(engine.FirstTokenFloorMS) * time.Millisecond,
		FirstTokenCeiling:      time.Duration(engine.FirstTokenCeilingMS) * time.Millisecond,
		InterChunkStall:        time.Duration(engine.InterChunkStallMS) * time.Millisecond,
		LoserGrace:             time.Duration(engine.LoserGraceMS) * time.Millisecond,
		MaxSpeculativeAttempts: int(engine.MaxSpeculativeAttempts),
	}
}

const (
	firstTokenObservedLimit = 4
	firstTokenObservedSlack = 3
)

type EscalationRequest struct {
	InputTokens uint64
}

type EscalationAttempt struct {
	FirstContentP75 time.Duration
	Suspicious      bool
	Escalated       bool
	Done            bool
	Crowned         bool
	Stalled         bool
	NonceFinished   bool
	SendTime        time.Time
	ReceiptTime     time.Time
	FirstToken      time.Time
	FirstContent    time.Time
	LastChunk       time.Time
	Completed       time.Time
}

// StartPlan is how a race begins; ImmediateAttempts counts the primary.
type StartPlan struct {
	ImmediateAttempts int
	Reason            string
}

// ArmedEscalation is a deadline to arm, deliberately not a permission to escalate; Confirm is the only
// thing that converts it. See gateway-speculative-race.md, "Escalation".
type ArmedEscalation struct {
	Attempt  int
	Stage    EscalationStage
	Deadline time.Time
}

type ConfirmedEscalation struct {
	Attempt int
	Stage   EscalationStage
}

// Decide hedges a primary the race already has reason to distrust; degraded is the outlier detector's
// verdict before the routing cap. See gateway-speculative-race.md, "Escalation" and
// gateway-capacity-and-health.md, "Outlier ejection".
func (p EscalationPolicy) Decide(budget int, primarySuspicious, primaryDegraded bool) StartPlan {
	switch {
	case budget < 2:
	case primarySuspicious:
		return StartPlan{ImmediateAttempts: 2, Reason: StartPrimarySuspicious}
	case primaryDegraded:
		return StartPlan{ImmediateAttempts: 2, Reason: StartPrimaryDegraded}
	}
	return StartPlan{ImmediateAttempts: 1, Reason: StartPrimary}
}

// AttemptBudget caps how many attempts one race may hold; scarce nonces force a single attempt. See
// gateway-speculative-race.md, "Escalation".
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
	case attempt.Done:
		return ArmedEscalation{Stage: StageAttemptFailed, Deadline: now}, true
	case attempt.SendTime.IsZero():
		return ArmedEscalation{}, false
	case attempt.ReceiptTime.IsZero():
		deadline := attempt.SendTime.Add(p.receiptTimeout(request.InputTokens))
		return ArmedEscalation{Stage: StageReceiptTimeout, Deadline: deadline}, true
	case !attempt.FirstToken.IsZero():
		return ArmedEscalation{}, false
	}
	// The curve is measured from dispatch, but a receipt that used more than the curve allows would leave
	// the rung already due: the host owes a first token, not the time its receipt took.
	deadline := attempt.SendTime.Add(p.firstTokenBudget(request.InputTokens, attempt.FirstContentP75))
	if graceFromReceipt := attempt.ReceiptTime.Add(p.FirstTokenFloor); graceFromReceipt.After(deadline) {
		deadline = graceFromReceipt
	}
	return ArmedEscalation{Stage: StageFirstToken, Deadline: deadline}, true
}

func (p EscalationPolicy) receiptTimeout(inputTokens uint64) time.Duration {
	if inputTokens > receiptTimeoutDoubleAboveTokens {
		return 2 * p.ReceiptTimeout
	}
	return p.ReceiptTimeout
}

// firstTokenBudget only ever extends: a host slower than the curve keeps it, because waiting out its
// own history would delay the attempt that rescues the request.
func (p EscalationPolicy) firstTokenBudget(inputTokens uint64, observed time.Duration) time.Duration {
	curve := p.firstTokenTimeout(inputTokens)
	if observed <= 0 || observed > firstTokenObservedLimit*curve {
		return curve
	}
	budget := observed * firstTokenObservedSlack / 2
	if budget < curve {
		return curve
	}
	if p.FirstTokenCeiling > 0 && budget > p.FirstTokenCeiling {
		return p.FirstTokenCeiling
	}
	return budget
}

// firstTokenTimeout is the measured first-token fit over prompt size, floored and capped: the curve is
// quadratic, and uncapped it outgrows the backstop that cancels the attempt. See
// gateway-speculative-race.md, "Escalation".
func (p EscalationPolicy) firstTokenTimeout(inputTokens uint64) time.Duration {
	tokens := float64(inputTokens)
	seconds := firstTokenBaseSeconds + firstTokenPerTokenSeconds*tokens + firstTokenQuadraticSeconds*tokens*tokens
	wait := max(p.FirstTokenFloor, time.Duration(seconds*float64(time.Second)))
	if p.FirstTokenCeiling > 0 && wait > p.FirstTokenCeiling {
		return p.FirstTokenCeiling
	}
	return wait
}
