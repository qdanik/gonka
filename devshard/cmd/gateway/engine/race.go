package engine

import (
	"context"
	"errors"
	"io"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
)

const (
	// RolePrimary and RoleSpeculative are the attempt role label values.
	RolePrimary     = "primary"
	RoleSpeculative = "speculative"

	// eventBuffer keeps a host's chunk progress from parking its goroutine between the coordinator's
	// selects. Terminal events block regardless of it, which is what settles a committed nonce.
	eventBuffer = 32

	// Why an attempt holds the client stream: it claimed first, or it was the last claimant standing.
	crownFirstClaim = "first_claim"
	crownNoRival    = "no_rival"
)

var errNoDispatchTarget = errors.New("assigned escrow has no dispatch target")

// picker is satisfied by *scheduler.Scheduler.
type picker interface {
	Pick(ctx context.Context, profile scheduler.RequestProfile) (scheduler.Assignment, error)
	BlockHost(escrowID, participant string)
}

// DispatchTarget is one escrow's session, seen only through what a race needs from it.
type DispatchTarget interface {
	dispatcher
	HostCount() int
	HostLabel(hostIdx int) string
	NonceFinished(nonce uint64) bool
}

// targets is satisfied by the runtime registry. Escrows rotate, so the handle is fetched per race.
type targets interface {
	Target(escrowID string) (DispatchTarget, bool)
}

// hostPerf is satisfied by *perf.Tracker.
type hostPerf interface {
	CapabilityRecorder
	Acquire(participant string)
	Release(participant string)
	Ejected(participant, model string) bool
	Degraded(participant, model string) bool
	FirstContentP75(participant, model string) (time.Duration, bool)
}

// crownGate carries the empty-stream crowning penalty between races. See gateway-speculative-race.md,
// "Crown denial".
type crownGate interface {
	Denied(participant, model string) bool
	Observe(participant, model string, contentless bool)
}

// snapshotSource is satisfied by *chain.PhaseObserver.
type snapshotSource interface{ Snapshot() chain.PhaseSnapshot }

// raceDeps is what one coordinator is wired to. Hold parks an escrow hold on the race, given back only
// once the race's vote is posted; Report receives the race's single outcome from whichever goroutine ends
// the race, which is not always the one that called runRace.
type raceDeps struct {
	Picker       picker
	Targets      targets
	Limiter      hostLimiter
	Perf         hostPerf
	Crown        crownGate
	Snapshots    snapshotSource
	Policy       EscalationPolicy
	Modes        config.Modes
	DrainTimeout time.Duration
	Classify     func(participant string) streamClassifier
	Now          func() time.Time
	Timer        func() raceTimer

	Hold func(release func())

	Report func(RaceOutcome)
}

type raceRequest struct {
	Request
	Client io.Writer
}

const (
	triggerNone deadlineTrigger = iota
	triggerHardTimeout
	triggerEscalation
	triggerPick
	triggerStall
)

// liveAttempt is the coordinator's own record of one attempt, written only by the coordinator: from
// AttemptEvents, and from its own deadline firings for the escalated and stalled marks.
type liveAttempt struct {
	nonce       uint64
	hostIdx     int
	participant string
	suspicious  bool
	cancel      context.CancelFunc

	sendTime     time.Time
	receiptTime  time.Time
	firstToken   time.Time
	firstContent time.Time
	lastChunk    time.Time
	completed    time.Time

	observedFirst time.Duration
	escalated     bool
	done          bool
	stalled       bool
	nonceFinished bool
	inInference   bool

	outcome   *AttemptOutcome
	lifecycle Lifecycle
}

type raceCoordinator struct {
	drain   drain
	deps    raceDeps
	request raceRequest

	events chan AttemptEvent
	crown  chan crownRequest
	picked chan pickedHost
	done   chan struct{}
	timer  raceTimer

	escrowID string
	target   DispatchTarget
	decision string
	budget   int
	started  time.Time

	attempts []*liveAttempt
	byNonce  map[uint64]*liveAttempt
	scratch  []EscalationAttempt
	claims   []crownRequest

	winner           *liveAttempt
	pending          int
	pickCancel       context.CancelFunc
	pickStarted      time.Time
	pickReason       string
	moreImmediate    int
	excluded         []string
	contextHint      uint64
	clientGoneAt     time.Time
	cancelled        bool
	handedOff        bool
	pocBypass        bool
	balanceExhausted bool
	startErr         error
}

// raceExit is why await stopped; only exitComplete means the race is over. See gateway-invariants.md,
// "2. Exactly one outcome and exactly one winner per race, on every path".
type raceExit int

const (
	exitComplete raceExit = iota
	exitWinnerServed
	exitClientGone
)

// runRace serves one request and reports the race exactly once. Report may run after runRace has
// returned: a client whose winner finished is released while losers still stream, and a client that
// left is released immediately, so the returned outcome is that client's view rather than the
// reported one.
func runRace(clientCtx context.Context, deps raceDeps, request raceRequest) (RaceOutcome, error) {
	coordinator := newCoordinator(clientCtx, deps, request)
	if err := coordinator.begin(); err != nil {
		return coordinator.fail(err)
	}
	exit := coordinator.await()
	if exit == exitComplete {
		outcome := coordinator.report()
		if len(outcome.Attempts) == 0 && coordinator.startErr != nil {
			return outcome, coordinator.startErr
		}
		return outcome, nil
	}
	served := coordinator.outcome()
	var err error
	if exit == exitClientGone {
		err = coordinator.drain.clientErr()
	}
	go func() {
		coordinator.await()
		coordinator.report()
	}()
	return served, err
}

func newCoordinator(clientCtx context.Context, deps raceDeps, request raceRequest) *raceCoordinator {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Timer == nil {
		deps.Timer = newWallTimer
	}
	contexts := newDrain(clientCtx, deps.DrainTimeout)
	request.Client = contexts.gate(request.Client)
	return &raceCoordinator{
		drain:       contexts,
		deps:        deps,
		request:     request,
		events:      make(chan AttemptEvent, eventBuffer),
		crown:       make(chan crownRequest),
		picked:      make(chan pickedHost, 1),
		done:        make(chan struct{}),
		timer:       deps.Timer(),
		byNonce:     map[uint64]*liveAttempt{},
		contextHint: request.ContextHint,
		escrowID:    request.Escrow,
		started:     deps.Now(),
	}
}

// begin bounds its pick: the race context deliberately never cancels, so a scheduler that waits for
// capacity would hang here forever. See specs/2026-08-03-request-queue-design.md.
func (c *raceCoordinator) begin() error {
	pickCtx, cancelPick := context.WithTimeout(c.drain.race, schedulerPickTimeout)
	defer cancelPick()
	assignment, err := c.pick(pickCtx)
	if err != nil {
		return err
	}
	target, ok := c.deps.Targets.Target(assignment.Escrow)
	if !ok {
		c.strand(assignment, RolePrimary)
		return errNoDispatchTarget
	}
	c.escrowID, c.target = assignment.Escrow, target
	if c.request.OnEscrow != nil {
		c.request.OnEscrow(c.escrowID)
	}

	snapshot := c.deps.Snapshots.Snapshot()
	c.pocBypass = pocBypassActive(snapshot, c.deps.Modes)
	c.budget = c.deps.Policy.AttemptBudget(target.HostCount(), snapshot.RequestsBlocked && !c.pocBypass)

	plan := c.deps.Policy.Decide(c.budget, c.denied(assignment.Host), c.degraded(assignment.Host))
	c.decision = plan.Reason
	c.launch(assignment, RolePrimary, plan.Reason)
	c.moreImmediate = min(plan.ImmediateAttempts, c.budget) - len(c.attempts)
	c.startNextImmediate()
	return nil
}

// strand accounts for an assignment no attempt can spend: the host slot goes back, the escrow hold is
// kept, and the nonce is carried into the timeout plan. See gateway-invariants.md, "1. A committed nonce
// is always settled" and "5. The slot and the escrow hold are taken with the nonce, and given back after
// the vote".
// A stranded nonce is committed, paid for, and answered by nobody -- the shape every recurring
// settlement defect in this gateway has taken. It is traced at Warn because it is never routine.
func (c *raceCoordinator) strand(assignment scheduler.Assignment, role string) {
	logging.Warn("nonce stranded",
		"request", c.request.RequestID, "escrow", assignment.Escrow,
		"nonce", assignment.Nonce.Nonce(), "participant", assignment.Host, "role", role)
	c.escrowID = assignment.Escrow
	c.deps.Limiter.Release(assignment.Host, c.request.Model)
	if c.deps.Hold != nil {
		c.deps.Hold(assignment.EscrowHold)
	}
	c.attempts = append(c.attempts, &liveAttempt{
		nonce:       assignment.Nonce.Nonce(),
		hostIdx:     assignment.Nonce.HostIdx(),
		participant: assignment.Host,
		done:        true,
		outcome: &AttemptOutcome{
			Participant: assignment.Host,
			HostIdx:     assignment.Nonce.HostIdx(),
			Nonce:       assignment.Nonce.Nonce(),
			Role:        role,
			Terminal:    TerminalNoReceipt,
		},
	})
}

// fail reports a race that never got as far as its first attempt. See gateway-invariants.md, "1. A
// committed nonce is always settled".
func (c *raceCoordinator) fail(err error) (RaceOutcome, error) {
	return c.report(), err
}

// await runs the coordinator until the race is over, until the crowned winner's fate is settled with
// losers still streaming, or until the client leaves. An armed escalation outlives the last pending
// attempt, because a race whose every attempt failed is exactly the one owed another.
func (c *raceCoordinator) await() raceExit {
	defer c.timer.Stop()
	for {
		c.settleClaims()
		arm := nextDeadline(c.deps.Now(), c.plan())
		if c.pending == 0 && !c.picking() && arm.Trigger != triggerEscalation {
			return exitComplete
		}
		c.rearm(arm)
		select {
		case event := <-c.events:
			c.apply(event)
		case result := <-c.picked:
			c.applyPick(result)
		case claim := <-c.crown:
			c.answer(claim)
		case <-c.timer.Fired():
			c.expire(arm)
		case <-c.clientGone():
			return c.depart()
		}
		if c.release() {
			return exitWinnerServed
		}
	}
}

// catchUp applies every event already queued, so every select arm that reads race state rather than
// adding to it starts here. See gateway-invariants.md, "3. No field crosses a goroutine except through
// the event channel".
func (c *raceCoordinator) catchUp() {
	for {
		select {
		case event := <-c.events:
			c.apply(event)
		default:
			return
		}
	}
}

func (c *raceCoordinator) depart() raceExit {
	c.catchUp()
	if c.release() {
		return exitWinnerServed
	}
	c.detach()
	if c.pending == 0 && !c.picking() {
		return exitComplete
	}
	return exitClientGone
}

// detach keeps every started attempt running for its receipt, its response and its nonce's vote, with
// the drain deadline standing in for the client that is no longer waiting on any of it. The pick is the
// exception: an attempt not yet started serves nobody, so its nonce and slot go back unspent.
func (c *raceCoordinator) detach() {
	c.handedOff = true
	c.clientGoneAt = c.deps.Now()
	c.stopPicking()
}

// clientGone is watched only while a client is still waiting on this race.
func (c *raceCoordinator) clientGone() <-chan struct{} {
	if c.handedOff {
		return nil
	}
	return c.drain.clientGone()
}

// release hands the still-pending losers to a second await on another goroutine. See
// gateway-speculative-race.md, "Client departure and the drain".
func (c *raceCoordinator) release() bool {
	if c.handedOff || c.pending == 0 || c.winner == nil || !c.winner.done {
		return false
	}
	c.handedOff = true
	return true
}
