package engine

import (
	"context"
	"errors"
	"io"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
	"devshard/types"
)

// RolePrimary and RoleSpeculative are the attempt role label values.
const (
	RolePrimary     = "primary"
	RoleSpeculative = "speculative"
)

// eventBuffer keeps a host's chunk progress from parking its goroutine between the coordinator's
// selects. Terminal events block regardless of it, which is what settles a committed nonce.
const eventBuffer = 32

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
}

// crownGate carries the empty-stream crowning penalty between races: such a host keeps receiving
// attempts and simply cannot be crowned until it produces content again.
type crownGate interface {
	Denied(participant, model string) bool
	Observe(participant, model string, contentless bool)
}

// snapshotSource is satisfied by *chain.PhaseObserver.
type snapshotSource interface{ Snapshot() chain.PhaseSnapshot }

// raceTimer is the coordinator's one timer, re-armed to whatever nextDeadline chose.
type raceTimer interface {
	Reset(delay time.Duration)
	Stop()
	Fired() <-chan time.Time
}

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

// deadlineTrigger is what fires at a deadline. The declaration order is the tie-break precedence: a
// race that must stop gains nothing from spending a nonce, and a stall flag is telemetry either way.
type deadlineTrigger int

const (
	triggerNone deadlineTrigger = iota
	triggerHardTimeout
	triggerEscalation
	triggerStall
)

type deadlineArm struct {
	At         time.Time
	Trigger    deadlineTrigger
	Escalation ArmedEscalation
}

type deadlinePlan struct {
	Policy    EscalationPolicy
	Request   EscalationRequest
	Attempts  []EscalationAttempt
	Budget    int
	Start     time.Time
	Drain     time.Time
	Cancelled bool
}

// nextDeadline is the earliest armed deadline and what fires there. Ties resolve by trigger
// precedence; an earlier deadline of any kind still wins over a later one.
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
	consider(plan.stall(), triggerStall)
	if arm.Trigger != triggerEscalation {
		arm.Escalation = ArmedEscalation{}
	}
	return arm
}

// A race whose client left is owed no further attempt: another attempt is another nonce to settle for
// a response nobody will read.
func (p deadlinePlan) escalation(now time.Time) (ArmedEscalation, bool) {
	if p.detached() || p.crowned() || len(p.Attempts) >= p.Budget {
		return ArmedEscalation{}, false
	}
	return p.Policy.NextEscalation(now, p.Attempts, p.Request)
}

func (p deadlinePlan) hardTimeout() time.Time {
	var earliest time.Time
	consider := func(at time.Time) {
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
	}
	if p.detached() {
		consider(p.Drain)
	}
	if !p.Request.Stream && !p.Start.IsZero() {
		consider(p.Start.Add(nonStreamMaxAttemptWait))
		if !p.crowned() {
			consider(p.Start.Add(nonStreamNoContentTimeout))
		}
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
		at := attempt.LastChunk.Add(p.Policy.InterChunkStall)
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
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

// liveAttempt is the coordinator's own record of one attempt, written only from AttemptEvents.
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

	winner           *liveAttempt
	pending          int
	excluded         []string
	contextHint      uint64
	clientGoneAt     time.Time
	cancelled        bool
	handedOff        bool
	pocBypass        bool
	balanceExhausted bool
	startErr         error
}

// raceExit is why await stopped. Only exitComplete means the race is over; the other two hand a race
// that still owes work to a second await on another goroutine.
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
		done:        make(chan struct{}),
		timer:       deps.Timer(),
		byNonce:     map[uint64]*liveAttempt{},
		contextHint: request.ContextHint,
		escrowID:    request.Escrow,
		started:     deps.Now(),
	}
}

func (c *raceCoordinator) begin() error {
	assignment, err := c.pick(c.drain.race)
	if err != nil {
		return err
	}
	target, ok := c.deps.Targets.Target(assignment.Escrow)
	if !ok {
		c.strand(assignment)
		return errNoDispatchTarget
	}
	c.escrowID, c.target = assignment.Escrow, target
	if c.request.OnEscrow != nil {
		c.request.OnEscrow(c.escrowID)
	}

	snapshot := c.deps.Snapshots.Snapshot()
	c.pocBypass = pocBypassActive(snapshot, c.deps.Modes)
	c.budget = c.deps.Policy.AttemptBudget(target.HostCount(), snapshot.RequestsBlocked && !c.pocBypass)

	plan := c.deps.Policy.Decide(c.budget, c.denied(assignment.Host))
	c.decision = plan.Reason
	c.launch(assignment, RolePrimary, plan.Reason)
	for len(c.attempts) < plan.ImmediateAttempts && len(c.attempts) < c.budget {
		extra, err := c.pick(c.drain.race)
		if err != nil {
			break
		}
		c.launch(extra, RoleSpeculative, plan.Reason)
	}
	return nil
}

// strand accounts for an assignment no attempt can spend: the scheduler committed its nonce and took
// its host slot in the step that produced it, so the slot goes back here and the nonce is left for the
// timeout vote rather than pinned for the process's life. Its escrow hold is kept instead of returned:
// the escrow being retired is why there is no target, and the vote still has to reach it.
func (c *raceCoordinator) strand(assignment scheduler.Assignment) {
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
			Role:        RolePrimary,
			Terminal:    TerminalNoReceipt,
		},
	})
}

// fail reports a race that never got as far as its first attempt, so a nonce it committed and cannot
// spend is still owed a vote and every race reports exactly once whatever ended it.
func (c *raceCoordinator) fail(err error) (RaceOutcome, error) {
	return c.report(), err
}

// await runs the coordinator until the race is over, until the crowned winner's fate is settled with
// losers still streaming, or until the client leaves. An armed escalation outlives the last pending
// attempt, because a race whose every attempt failed is exactly the one owed another.
func (c *raceCoordinator) await() raceExit {
	defer c.timer.Stop()
	for {
		arm := nextDeadline(c.deps.Now(), c.plan())
		if c.pending == 0 && arm.Trigger != triggerEscalation {
			return exitComplete
		}
		c.rearm(arm)
		select {
		case event := <-c.events:
			c.apply(event)
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

// catchUp applies every event already queued. A buffered event and a fired timer are equally ready in
// the select, so anything that reads the race's state rather than adding to it starts here: acting on
// a deadline the newer state has already cleared spends a nonce, mislabels a healthy host, or reports
// a completed winner as cancelled.
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

func (c *raceCoordinator) expire(arm deadlineArm) {
	c.catchUp()
	c.fire(arm)
}

func (c *raceCoordinator) depart() raceExit {
	c.catchUp()
	if c.release() {
		return exitWinnerServed
	}
	c.detach()
	if c.pending == 0 {
		return exitComplete
	}
	return exitClientGone
}

// detach keeps every started attempt running for its receipt, its response and its nonce's vote, with
// the drain deadline standing in for the client that is no longer waiting on any of it.
func (c *raceCoordinator) detach() {
	c.handedOff = true
	c.clientGoneAt = c.deps.Now()
}

// clientGone is watched only while a client is still waiting on this race.
func (c *raceCoordinator) clientGone() <-chan struct{} {
	if c.handedOff {
		return nil
	}
	return c.drain.clientGone()
}

// release hands the still-pending losers to a second await on another goroutine. Nothing they can do
// changes what the client received, and the client's response is not complete until its handler
// returns.
func (c *raceCoordinator) release() bool {
	if c.handedOff || c.pending == 0 || c.winner == nil || !c.winner.done {
		return false
	}
	c.handedOff = true
	return true
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
	case triggerStall:
		c.markStalls()
	case triggerHardTimeout:
		c.cancelAll()
	}
}

// answer settles one attempt's claim on the client stream. A suspicious host is held back only while
// a rival could still serve: alone, its answer is the one the race committed a nonce for, and refusing
// it hands the client an error for a response that exists.
func (c *raceCoordinator) answer(claim crownRequest) {
	verdict := streamSuppressed
	switch attempt := c.byNonce[claim.Nonce]; {
	case attempt == nil:
	case attempt == c.winner:
		verdict = streamWinner
	case c.winner == nil && (!attempt.suspicious || c.pending == 1):
		c.winner, verdict = attempt, streamWinner
	}
	claim.Reply <- verdict
}

func (c *raceCoordinator) apply(event AttemptEvent) {
	attempt := c.byNonce[event.Nonce]
	if attempt == nil {
		return
	}
	switch event.Kind {
	case AttemptDispatched:
		attempt.sendTime = event.At
	case AttemptReceipt:
		attempt.receiptTime = event.At
	case AttemptFirstToken:
		attempt.firstToken, attempt.lastChunk = event.At, event.At
	case AttemptContent:
		attempt.firstContent, attempt.lastChunk = event.At, event.At
	case AttemptChunk:
		attempt.lastChunk = event.At
	case AttemptDone:
		c.complete(attempt, event)
	}
}

func (c *raceCoordinator) complete(attempt *liveAttempt, event AttemptEvent) {
	attempt.done, attempt.completed = true, event.At
	attempt.outcome, attempt.lifecycle = event.Outcome, event.Lifecycle
	attempt.nonceFinished = c.target.NonceFinished(attempt.nonce)
	c.retire(attempt)

	if attempt.outcome == nil {
		return
	}
	if signal := CapabilityOf(*attempt.outcome); signal.Retriable() {
		RecordCapability(c.deps.Perf, attempt.participant, signal)
		c.contextHint = GrowContextHint(c.contextHint, signal)
		c.exclude(attempt.participant)
	}
	if attempt.outcome.StateDivergent {
		c.deps.Picker.BlockHost(c.escrowID, attempt.participant)
		c.exclude(attempt.participant)
	}
}

func (c *raceCoordinator) retire(attempt *liveAttempt) {
	c.pending--
	attempt.cancel()
	c.deps.Perf.Release(attempt.participant)
}

func (c *raceCoordinator) escalate(armed ArmedEscalation) {
	confirmed, ok := c.deps.Policy.Confirm(armed, c.deps.Now(), c.plan().Attempts, c.escalationRequest())
	if !ok {
		return
	}
	// Consumed before the new attempt is started, so a failed start cannot retry the same trigger.
	c.attempts[confirmed.Attempt].escalated = true
	assignment, err := c.pickBesideTheRace()
	if err != nil {
		c.startErr = err
		return
	}
	c.launch(assignment, RoleSpeculative, confirmed.Stage.Reason())
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
	for _, attempt := range c.attempts {
		if !attempt.done {
			attempt.cancel()
		}
	}
}

func (c *raceCoordinator) pick(ctx context.Context) (scheduler.Assignment, error) {
	return c.observePick(c.deps.Picker.Pick(ctx, c.requestProfile()))
}

// observePick latches the out-of-funds fact a declined nonce carries. The balance is spent composing
// the diff, so no host reports it and no attempt is ever started against an escrow that ran dry.
func (c *raceCoordinator) observePick(assignment scheduler.Assignment, err error) (scheduler.Assignment, error) {
	if errors.Is(err, types.ErrInsufficientBalance) {
		c.balanceExhausted = true
	}
	return assignment, err
}

func (c *raceCoordinator) requestProfile() scheduler.RequestProfile {
	return scheduler.RequestProfile{
		Model:         c.request.Model,
		Escrow:        c.escrowID,
		InputTokens:   int(c.request.InputTokens),
		RequiresTools: c.request.RequiresTools,
		ContextHint:   c.contextHint,
		Exclude:       c.excluded,
		Params:        c.request.Params,
	}
}

type pickedHost struct {
	assignment scheduler.Assignment
	err        error
}

// pickBesideTheRace runs a speculative pick off the coordinator's goroutine. The scheduler holds a
// waiter for a co-arriving request, and a crown claim is the client's first token: waiting for one
// inside the loop would spend that hold on the latency the escalation exists to avoid. A client that
// leaves cancels the pick, which is what returns the nonce and the slot it would have handed over.
func (c *raceCoordinator) pickBesideTheRace() (scheduler.Assignment, error) {
	picked := make(chan pickedHost, 1)
	ctx, cancel := context.WithCancel(c.drain.race)
	defer cancel()
	profile := c.requestProfile()
	go func() {
		assignment, err := c.deps.Picker.Pick(ctx, profile)
		picked <- pickedHost{assignment: assignment, err: err}
	}()
	for {
		select {
		case result := <-picked:
			return c.observePick(result.assignment, result.err)
		case claim := <-c.crown:
			c.answer(claim)
		case <-c.clientGone():
			cancel()
			result := <-picked
			return c.observePick(result.assignment, result.err)
		}
	}
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
	}
	c.attempts = append(c.attempts, attempt)
	c.byNonce[nonce] = attempt
	c.pending++
	c.deps.Perf.Acquire(attempt.participant)

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

// report closes the byte path's escape hatch and hands the race's one outcome to its consumers.
func (c *raceCoordinator) report() RaceOutcome {
	close(c.done)
	outcome := c.outcome()
	if c.deps.Crown != nil {
		for _, attempt := range outcome.Attempts {
			c.deps.Crown.Observe(attempt.Participant, outcome.Model, outcome.DeniesCrowning(attempt))
		}
	}
	if c.deps.Report != nil {
		c.deps.Report(outcome)
	}
	return outcome
}

func (c *raceCoordinator) outcome() RaceOutcome {
	generating := pocGenerating(c.deps.Snapshots.Snapshot(), c.deps.Modes)
	outcome := RaceOutcome{
		RequestID:       c.request.RequestID,
		EscrowID:        c.escrowID,
		Model:           c.request.Model,
		InputTokens:     c.request.InputTokens,
		Stream:          c.request.Stream,
		Decision:        c.decision,
		PoCBypassActive: c.pocBypass,
		Lifecycle:       Lifecycle{BalanceExhausted: c.balanceExhausted},
		Attempts:        make([]AttemptOutcome, 0, len(c.attempts)),
	}
	for _, attempt := range c.attempts {
		if attempt.outcome == nil {
			continue
		}
		record := *attempt.outcome
		record.NonceFinished = attempt.nonceFinished
		record.FailureRateExceeded = c.deps.Perf.Ejected(attempt.participant, c.request.Model)
		record.PhaseTransitionAborted = phaseAborted(record, attempt.inInference, generating)
		// The attempt goroutine can only see its own cancellation; the coordinator knows it cancelled a
		// host that had gone silent, and TerminalStalled is what the verdict ladder gates on.
		if attempt.stalled && record.Terminal == TerminalClientCancelled && record.ContentChunks > 0 {
			record.Terminal = TerminalStalled
		}
		if attempt == c.winner {
			outcome.WinnerNonce = attempt.nonce
			if record.Terminal == TerminalLost {
				record.Terminal = TerminalWon
				outcome.Succeeded = true
			}
		}
		outcome.Lifecycle.EscrowMissing = outcome.Lifecycle.EscrowMissing || attempt.lifecycle.EscrowMissing
		outcome.Lifecycle.BalanceExhausted = outcome.Lifecycle.BalanceExhausted || attempt.lifecycle.BalanceExhausted
		outcome.Attempts = append(outcome.Attempts, record)
	}
	return outcome
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
		})
	}
	return deadlinePlan{
		Policy:    c.deps.Policy,
		Request:   c.escalationRequest(),
		Attempts:  c.scratch,
		Budget:    c.budget,
		Start:     c.started,
		Drain:     c.drain.deadline(c.clientGoneAt),
		Cancelled: c.cancelled,
	}
}

func (c *raceCoordinator) escalationRequest() EscalationRequest {
	return EscalationRequest{InputTokens: c.request.InputTokens, Stream: c.request.Stream}
}

func (c *raceCoordinator) denied(participant string) bool {
	return c.deps.Crown != nil && c.deps.Crown.Denied(participant, c.request.Model)
}

func (c *raceCoordinator) exclude(participant string) {
	for _, excluded := range c.excluded {
		if excluded == participant {
			return
		}
	}
	c.excluded = append(c.excluded, participant)
}

// attemptSink is one attempt's byte path. It runs only on that attempt's goroutine.
type attemptSink struct {
	winner     *winnerWriter
	hasContent bool
}

func (s *attemptSink) Write(chunk []byte) (int, error) {
	if err := s.winner.Write(chunk, s.hasContent); err != nil {
		return 0, err
	}
	return len(chunk), nil
}

func (s *attemptSink) Flush() { s.winner.Flush() }

// contentGate hands the sink beside it the one fact an io.Writer signature cannot carry. Classify is
// called immediately before the Write of the same chunk, on the same goroutine.
type contentGate struct {
	streamClassifier
	sink *attemptSink
}

func (g contentGate) Classify(chunk []byte) chunkFacts {
	facts := g.streamClassifier.Classify(chunk)
	g.sink.hasContent = facts.Content
	return facts
}

// pocBypassActive reports the gateway serving through a chain phase that refuses new inferences.
func pocBypassActive(snapshot chain.PhaseSnapshot, modes config.Modes) bool {
	return modes.PoCMode == config.PoCModeRelaxed && snapshot.RequestsBlocked
}

// pocGenerating is the narrower phase that takes a host away from inference mid-attempt. Only the
// bypass has an attempt running inside it, so strict mode never reports it.
func pocGenerating(snapshot chain.PhaseSnapshot, modes config.Modes) bool {
	if modes.PoCMode != config.PoCModeRelaxed {
		return false
	}
	return snapshot.EpochPhase == chain.EpochPhasePoCGenerate ||
		snapshot.ConfirmationPoCPhase == chain.ConfirmationPoCGeneration
}

// phaseAborted reports an attempt the phase transition ended rather than the host. A no-receipt
// attempt never reached the host's queue, so the transition cannot be what ended it.
func phaseAborted(a AttemptOutcome, startedInInference, pocGenerating bool) bool {
	if !startedInInference || !pocGenerating {
		return false
	}
	return !a.NonceFinished && a.ErrorSource == "" && a.Terminal != TerminalNoReceipt
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
