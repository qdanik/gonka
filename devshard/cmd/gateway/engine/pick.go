package engine

import (
	"context"
	"errors"
	"slices"
	"time"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
	"devshard/types"
)

func (c *raceCoordinator) pick(ctx context.Context) (scheduler.Assignment, error) {
	return c.observePick(c.deps.Picker.Pick(ctx, c.requestProfile(c.request.Params)))
}

// observePick latches the out-of-funds fact a declined nonce carries; no host ever reports it.
func (c *raceCoordinator) observePick(assignment scheduler.Assignment, err error) (scheduler.Assignment, error) {
	if errors.Is(err, types.ErrInsufficientBalance) {
		c.balanceExhausted = true
	}
	return assignment, err
}

func (c *raceCoordinator) requestProfile(params any) scheduler.RequestProfile {
	return scheduler.RequestProfile{
		Model:       c.request.Model,
		Escrow:      c.escrowID,
		InputTokens: int(c.request.InputTokens),
		Exclude:     c.excluded,
		Params:      params,
	}
}

type pickedHost struct {
	assignment scheduler.Assignment
	err        error
}

func (c *raceCoordinator) picking() bool { return c.pickCancel != nil }

// startPick runs at most one speculative pick beside the race, never on the coordinator's goroutine. See race.md, "Escalation".
func (c *raceCoordinator) startPick(reason string, params any) {
	if c.picking() || len(c.attempts) >= c.budget {
		return
	}
	ctx, cancel := context.WithTimeout(c.drain.race, schedulerPickTimeout)
	c.pickCancel, c.pickStarted, c.pickReason = cancel, c.deps.Now(), reason
	profile := c.requestProfile(params)
	go func() {
		assignment, err := c.deps.Picker.Pick(ctx, profile)
		c.picked <- pickedHost{assignment: assignment, err: err}
	}()
}

func (c *raceCoordinator) startNextImmediate() {
	if c.moreImmediate <= 0 {
		return
	}
	c.moreImmediate--
	c.startPick(c.decision, c.request.Params)
}

// applyPick spends what the scheduler answered with; an assignment the race cannot use is stranded, not dropped.
func (c *raceCoordinator) applyPick(result pickedHost) {
	c.pickCancel()
	c.pickCancel = nil
	assignment, err := c.observePick(result.assignment, result.err)
	switch {
	case err != nil:
		c.reportUnfilledPick(err)
		c.startErr, c.moreImmediate = err, 0
	case c.cancelled || c.handedOff || c.winner != nil:
		c.strand(assignment, RoleSpeculative)
	default:
		c.launch(assignment, RoleSpeculative, c.pickReason)
	}
	c.startNextImmediate()
}

// reportUnfilledPick traces an escalation that reached no attempt; a race's own cancellation is no refusal.
func (c *raceCoordinator) reportUnfilledPick(err error) {
	if c.cancelled || c.handedOff {
		return
	}
	logging.Info("escalation unfilled",
		logkey.Request, c.request.RequestID, logkey.Escrow, c.escrowID, logkey.Reason, c.pickReason,
		logkey.Attempts, len(c.attempts), logkey.Error, err)
}

// stopPicking gives up on a pick the race can no longer spend; the scheduler gives back what it had handed over.
func (c *raceCoordinator) stopPicking() {
	c.moreImmediate = 0
	if c.picking() {
		c.pickCancel()
	}
}

func (c *raceCoordinator) pickDeadline() time.Time {
	if !c.picking() {
		return time.Time{}
	}
	return c.pickStarted.Add(schedulerPickTimeout)
}

func (c *raceCoordinator) observedFirstContent(participant string) time.Duration {
	observed, _ := c.deps.Perf.FirstContentP75(participant, c.request.Model)
	return observed
}

func (c *raceCoordinator) launch(assignment scheduler.Assignment, role, startReason string) {
	// The race holds the escrow for as long as its vote is owed, so the commit's own hold goes back.
	assignment.ReleaseEscrow()
	nonce := assignment.Nonce.Nonce()
	writer := newWinnerWriter(nonce, c.request.Client, c.crown, c.done)
	sink := &attemptSink{winner: writer}
	attemptCtx, cancel := context.WithCancel(c.drain.race)

	attempt := &liveAttempt{
		nonce:       nonce,
		hostIdx:     assignment.Nonce.HostIdx(),
		participant: assignment.Host,
		suspicious:  c.denied(assignment.Host),
		cancel:      cancel,
		inInference: !pocGenerating(c.deps.Snapshots.Snapshot(), c.deps.Modes),
		// Read once: plan() runs on every event, and the quantile cannot move mid-attempt.
		observedFirst: c.observedFirstContent(assignment.Host),
	}
	c.attempts = append(c.attempts, attempt)
	c.byNonce[nonce] = attempt
	c.pending++
	// Excluded on dispatch, not on failure: a sibling slot would cost a second nonce for one host's opinion.
	c.exclude(attempt.participant)
	c.deps.Perf.Acquire(attempt.participant)
	logging.Info("nonce committed",
		logkey.Request, c.request.RequestID, logkey.Escrow, assignment.Escrow, logkey.Nonce, nonce,
		logkey.Host, logkey.ShortHost(attempt.participant), logkey.Slot, attempt.hostIdx, logkey.Role, role, logkey.Reason, startReason)

	go runAttempt(attemptCtx, AttemptSpec{
		Escrow:      assignment.Escrow,
		Model:       c.request.Model,
		Participant: attempt.participant,
		HostIdx:     attempt.hostIdx,
		HostLabel:   c.target.HostLabel(attempt.hostIdx),
		Role:        role,
		StartReason: startReason,
		Suspicious:  attempt.suspicious,
		Nonce:       assignment.Nonce,
		Dispatch:    c.target,
		Limiter:     c.deps.Limiter,
		Classifier:  contentGate{streamClassifier: c.deps.Classify(attempt.participant), sink: sink},
		Sink:        sink,
		Now:         c.deps.Now,
		Events:      c.events,
	})
}

// abandonedByHosts reports the backstop cancelling every attempt while a client still waited, uncrowned.
func (c *raceCoordinator) abandonedByHosts() bool {
	return c.cancelled && c.winner == nil && !c.handedOff
}

func (c *raceCoordinator) denied(participant string) bool {
	return c.deps.Crown != nil && c.deps.Crown.Denied(participant, c.request.Model)
}

func (c *raceCoordinator) degraded(participant string) bool {
	return c.deps.Perf.Degraded(participant, c.request.Model)
}

func (c *raceCoordinator) exclude(participant string) {
	if slices.Contains(c.excluded, participant) {
		return
	}
	c.excluded = append(c.excluded, participant)
}
