package engine

import (
	"time"
)

// raceTimer is the coordinator's one timer, re-armed to whatever nextDeadline chose.
type raceTimer interface {
	Reset(delay time.Duration)
	Stop()
	Fired() <-chan time.Time
}

// deadlineTrigger is what fires at a deadline; the declaration order is the tie-break precedence. See
// gateway-speculative-race.md, "Deadlines".
type deadlineTrigger int
type deadlineArm struct {
	At         time.Time
	Trigger    deadlineTrigger
	Escalation ArmedEscalation
}

// Pick is when an unanswered pick stops being worth waiting for, and is zero while none is running.
type deadlinePlan struct {
	Policy    EscalationPolicy
	Request   EscalationRequest
	Attempts  []EscalationAttempt
	Budget    int
	Drain     time.Time
	Pick      time.Time
	Cancelled bool
}

// nextDeadline is the earliest armed deadline and what fires there, with trigger precedence breaking
// exact ties. See gateway-speculative-race.md, "Deadlines".
func nextDeadline(now time.Time, plan deadlinePlan) deadlineArm {
	var arm deadlineArm
	consider := func(at time.Time, trigger deadlineTrigger) {
		if at.IsZero() {
			return
		}
		if arm.Trigger == triggerNone || at.Before(arm.At) || (at.Equal(arm.At) && trigger < arm.Trigger) {
			arm.At, arm.Trigger = at, trigger
		}
	}
	if plan.Cancelled {
		return arm
	}
	consider(plan.hardTimeout(), triggerHardTimeout)
	if armed, ok := plan.escalation(now); ok {
		consider(armed.Deadline, triggerEscalation)
		if arm.Trigger == triggerEscalation {
			arm.Escalation = armed
		}
	}
	consider(plan.Pick, triggerPick)
	consider(plan.stall(), triggerStall)
	if arm.Trigger != triggerEscalation {
		arm.Escalation = ArmedEscalation{}
	}
	return arm
}

// A race whose client left is owed no further attempt: another attempt is another nonce to settle for
// a response nobody will read. A pick already running is the escalation, so it disarms the trigger too.
func (p deadlinePlan) escalation(now time.Time) (ArmedEscalation, bool) {
	if !p.Pick.IsZero() || p.detached() || p.crowned() {
		return ArmedEscalation{}, false
	}
	armed, found := p.Policy.NextEscalation(now, p.Attempts, p.Request)
	if found && len(p.Attempts) >= p.Budget {
		return ArmedEscalation{}, false
	}
	return armed, found
}
func (p deadlinePlan) hardTimeout() time.Time {
	var earliest time.Time
	consider := func(at time.Time) { earliest = earlierSet(earliest, at) }
	if p.detached() {
		consider(p.Drain)
	}
	for _, attempt := range p.Attempts {
		switch {
		case attempt.Done:
			if attempt.Crowned && !attempt.Completed.IsZero() && p.Policy.LoserGrace > 0 {
				consider(attempt.Completed.Add(p.Policy.LoserGrace))
			}
		case !attempt.SendTime.IsZero():
			consider(attempt.SendTime.Add(streamingHardTimeout))
		}
	}
	return earliest
}
func (p deadlinePlan) stall() time.Time {
	if p.Policy.InterChunkStall <= 0 {
		return time.Time{}
	}
	var earliest time.Time
	for _, attempt := range p.Attempts {
		if attempt.Done || attempt.Stalled ||
			attempt.FirstContent.IsZero() || attempt.LastChunk.IsZero() {
			continue
		}
		earliest = earlierSet(earliest, attempt.LastChunk.Add(p.Policy.InterChunkStall))
	}
	return earliest
}

func earlierSet(earliest, candidate time.Time) time.Time {
	if earliest.IsZero() || candidate.Before(earliest) {
		return candidate
	}
	return earliest
}
func (p deadlinePlan) detached() bool { return !p.Drain.IsZero() }
func (p deadlinePlan) crowned() bool {
	for _, attempt := range p.Attempts {
		if attempt.Crowned {
			return true
		}
	}
	return false
}

type wallTimer struct{ timer *time.Timer }

func newWallTimer() raceTimer {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return &wallTimer{timer: timer}
}
func (t *wallTimer) Reset(delay time.Duration) {
	t.timer.Stop()
	t.timer.Reset(delay)
}
func (t *wallTimer) Stop()                   { t.timer.Stop() }
func (t *wallTimer) Fired() <-chan time.Time { return t.timer.C }

func (c *raceCoordinator) expire(arm deadlineArm) {
	c.catchUp()
	c.fire(arm)
}
func (c *raceCoordinator) rearm(arm deadlineArm) {
	if arm.Trigger == triggerNone {
		c.timer.Stop()
		return
	}
	c.timer.Reset(arm.At.Sub(c.deps.Now()))
}
func (c *raceCoordinator) fire(arm deadlineArm) {
	switch arm.Trigger {
	case triggerEscalation:
		c.escalate(arm.Escalation)
	case triggerPick:
		c.stopPicking()
	case triggerStall:
		c.markStalls()
	case triggerHardTimeout:
		c.cancelAll()
	}
}
func (c *raceCoordinator) escalate(armed ArmedEscalation) {
	confirmed, ok := c.deps.Policy.Confirm(armed, c.deps.Now(), c.plan().Attempts, c.escalationRequest())
	if !ok {
		return
	}
	// Consumed before the pick is started, so a pick that finds no host cannot retry the same trigger.
	c.attempts[confirmed.Attempt].escalated = true
	c.startPick(confirmed.Stage.Reason(), c.request.Params)
}
func (c *raceCoordinator) markStalls() {
	silentSince := c.deps.Now().Add(-c.deps.Policy.InterChunkStall)
	for _, attempt := range c.attempts {
		if attempt.done || attempt.firstContent.IsZero() || attempt.lastChunk.After(silentSince) {
			continue
		}
		attempt.stalled = true
	}
}
func (c *raceCoordinator) cancelAll() {
	c.cancelled = true
	c.stopPicking()
	now := c.deps.Now()
	for _, attempt := range c.attempts {
		if attempt.done {
			continue
		}
		if !attempt.sendTime.IsZero() && !now.Before(attempt.sendTime.Add(streamingHardTimeout)) {
			attempt.backstopped = true
		}
		attempt.cancel()
	}
}
func (c *raceCoordinator) plan() deadlinePlan {
	c.scratch = c.scratch[:0]
	for _, attempt := range c.attempts {
		c.scratch = append(c.scratch, EscalationAttempt{
			Suspicious:    attempt.suspicious,
			Escalated:     attempt.escalated,
			Done:          attempt.done,
			Crowned:       attempt == c.winner,
			Stalled:       attempt.stalled,
			NonceFinished: attempt.nonceFinished,
			SendTime:      attempt.sendTime,
			ReceiptTime:   attempt.receiptTime,
			FirstToken:    attempt.firstToken,
			FirstContent:  attempt.firstContent,
			LastChunk:     attempt.lastChunk,
			Completed:     attempt.completed,

			FirstContentP75: attempt.observedFirst,
		})
	}
	return deadlinePlan{
		Policy:    c.deps.Policy,
		Request:   c.escalationRequest(),
		Attempts:  c.scratch,
		Budget:    c.budget,
		Drain:     c.drain.deadline(c.clientGoneAt),
		Pick:      c.pickDeadline(),
		Cancelled: c.cancelled,
	}
}
func (c *raceCoordinator) escalationRequest() EscalationRequest {
	return EscalationRequest{InputTokens: c.request.InputTokens}
}
