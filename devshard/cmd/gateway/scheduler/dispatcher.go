package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/chain"
)

const defaultSubmitBuffer = 64

// dispatchObserver reports the nonce accounting: burns by reason, holds, and exhausted burn budgets.
type dispatchObserver interface {
	GhostBurned(escrowID, reason string)
	NonceHeld(escrowID string)
	BurnBudgetExhausted(escrowID string)
}

type dispatcherDeps struct {
	escrowID   string
	session    session
	snapshots  snapshotSource
	predicates func(chain.PhaseSnapshot) availability
	observer   dispatchObserver
	now        func() time.Time
	stale      time.Duration
	newTimer   func(time.Duration) (<-chan time.Time, func())
	// retire is asked, from inside the loop goroutine, whether an idle actor may remove itself; the
	// registry answers under the lock that also guards claims, so a claimed dispatcher is never lost.
	retire       func(*dispatcher) bool
	idleGrace    time.Duration
	submitBuffer int
}

type submitOutcome int

const (
	submitAccepted submitOutcome = iota
	submitStopped
	submitFull
)

// dispatcher turns one escrow's sequential nonce stream into request assignments. The loop goroutine
// is the sole owner of waiting and of the session, so nothing else may touch either: a waiter leaves
// by setting its own abandoned flag, never by sending the actor a message that could block.
type dispatcher struct {
	dispatcherDeps

	submit  chan *waiter
	stopCh  chan struct{}
	done    chan struct{}
	waiting []*waiter

	pendingSubmits atomic.Int64

	lifecycleMu sync.RWMutex
	started     bool
	stopped     bool
}

func newDispatcher(deps dispatcherDeps) *dispatcher {
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.newTimer == nil {
		deps.newTimer = realTimer
	}
	if deps.submitBuffer <= 0 {
		deps.submitBuffer = defaultSubmitBuffer
	}
	return &dispatcher{
		dispatcherDeps: deps,
		submit:         make(chan *waiter, deps.submitBuffer),
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func realTimer(delay time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(delay)
	return timer.C, func() { timer.Stop() }
}

func (d *dispatcher) start() {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.started || d.stopped {
		return
	}
	d.started = true
	go d.loop()
}

// stop is idempotent and blocks until the loop has exited. Holding the write lock while closing
// stopCh keeps submitWaiter from landing a waiter in a buffer nobody will ever read.
func (d *dispatcher) stop() {
	d.lifecycleMu.Lock()
	if !d.stopped {
		d.stopped = true
		close(d.stopCh)
		if !d.started {
			d.absorb()
			d.failWaiting(ErrDispatcherStopped)
		}
	}
	started := d.started
	d.lifecycleMu.Unlock()

	if started {
		<-d.done
	}
}

// submitWaiter never blocks: a submit arriving before start, or faster than the actor drains, must
// not stall its caller or pin the lifecycle lock. A full queue is back-pressure and a stopped
// dispatcher is a lost race, and only the second is worth retrying, so they are reported apart.
func (d *dispatcher) submitWaiter(queued *waiter) submitOutcome {
	d.lifecycleMu.RLock()
	defer d.lifecycleMu.RUnlock()
	if d.stopped {
		return submitStopped
	}
	select {
	case d.submit <- queued:
		return submitAccepted
	default:
		return submitFull
	}
}

func (d *dispatcher) isStopped() bool {
	d.lifecycleMu.RLock()
	defer d.lifecycleMu.RUnlock()
	return d.stopped
}

// markStopped shuts the actor down from inside its own goroutine. It refuses once a waiter is in the
// submit buffer, which is the one arrival an empty queue cannot see.
func (d *dispatcher) markStopped() bool {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.stopped || len(d.submit) > 0 {
		return false
	}
	d.stopped = true
	close(d.stopCh)
	return true
}

func (d *dispatcher) loop() {
	defer close(d.done)
	var pending, idle armedTimer
	defer pending.disarm()
	defer idle.disarm()

	for {
		select {
		case queued := <-d.submit:
			d.waiting = append(d.waiting, queued)
			d.absorb()
		case <-pending.fired:
		case <-idle.fired:
			if d.retire != nil && d.retire(d) {
				return
			}
		case <-d.stopCh:
			d.absorb()
			d.failWaiting(ErrDispatcherStopped)
			return
		}

		pending.disarm()
		idle.disarm()
		switch until, held := d.drain(); {
		case held:
			pending.fired, pending.cancel = d.newTimer(until.Sub(d.now()))
		case len(d.waiting) == 0 && d.retire != nil && d.idleGrace > 0:
			idle.fired, idle.cancel = d.newTimer(d.idleGrace)
		}
	}
}

type armedTimer struct {
	fired  <-chan time.Time
	cancel func()
}

func (a *armedTimer) disarm() {
	if a.cancel != nil {
		a.cancel()
	}
	a.fired, a.cancel = nil, nil
}

// drain assigns nonces until the queue empties, a nonce is held, or the burn budget trips. The
// freeze and the budget bound its cost in nonces -- a capped, money-backed resource: without them a
// host flipping between the sweep and the binding burns one every iteration, forever.
func (d *dispatcher) drain() (time.Time, bool) {
	avail := freeze(d.predicates(d.snapshots.Snapshot()))
	participants := d.session.ParticipantKeys()
	burnBudget := d.session.GroupSize() * (len(d.waiting) + 1)

	for {
		d.sweepExhausted(participants, avail)
		if len(d.waiting) == 0 {
			return time.Time{}, false
		}

		var decision Decision
		var bound string
		prepared, err := d.session.Advance(func(binding HostBinding) NonceIntent {
			bound = binding.Participant
			decision = match(binding, d.waiting, avail, d.now(), d.stale)
			return intentFor(decision)
		})
		if err != nil {
			d.failAdvance(decision, err)
			return time.Time{}, false
		}

		switch outcome := decision.(type) {
		case serve:
			d.handOff(outcome.waiter, bound, prepared)
		case burn:
			d.recordGhost(outcome.kind.reason())
			burnBudget--
			if burnBudget <= 0 {
				d.recordBudgetTrip()
				return time.Time{}, false
			}
		case hold:
			d.recordHold()
			return outcome.until, true
		default:
			return time.Time{}, false
		}
	}
}

// sweepExhausted drops waiters no available participant can serve, instantly and without touching the nonce.
func (d *dispatcher) sweepExhausted(participants []string, avail availability) {
	kept := d.waiting[:0]
	for _, queued := range d.waiting {
		switch {
		case queued.abandoned.Load():
		case !servable(queued, participants, avail):
			d.reply(queued, pickResult{err: ErrNoAvailableHost})
		default:
			kept = append(kept, queued)
		}
	}
	for index := len(kept); index < len(d.waiting); index++ {
		d.waiting[index] = nil
	}
	d.waiting = kept
}

func servable(queued *waiter, participants []string, avail availability) bool {
	for _, participant := range participants {
		if queued.exclude[participant] ||
			avail.pocRequired(participant) ||
			avail.throttled(participant) ||
			avail.stateBlocked(participant) ||
			avail.capability(participant, queued.profile) {
			continue
		}
		return true
	}
	return false
}

func (d *dispatcher) handOff(served *waiter, participant string, prepared Prepared) {
	d.dequeue(served)
	if prepared == nil {
		d.reply(served, pickResult{err: fmt.Errorf("escrow %s: session committed no nonce", d.escrowID)})
		return
	}
	assignment := Assignment{Escrow: d.escrowID, Host: participant, Nonce: prepared}
	// The nonce is already committed, so a caller that vanished between the decision and the handoff
	// would leave it accounted to nobody; it is charged to the ghost side instead.
	if !d.reply(served, pickResult{assignment: assignment}) {
		d.recordGhost(ghostAbandoned.reason())
	}
}

func (d *dispatcher) failAdvance(decision Decision, err error) {
	failure := fmt.Errorf("escrow %s: advancing nonce: %w", d.escrowID, err)
	if chosen, ok := decision.(serve); ok {
		d.dequeue(chosen.waiter)
		d.reply(chosen.waiter, pickResult{err: failure})
		return
	}
	d.failWaiting(failure)
}

func (d *dispatcher) failWaiting(err error) {
	for index, queued := range d.waiting {
		d.reply(queued, pickResult{err: err})
		d.waiting[index] = nil
	}
	d.waiting = d.waiting[:0]
}

// absorb takes everything already queued so co-arriving requests compete for the same nonce.
func (d *dispatcher) absorb() {
	for {
		select {
		case queued := <-d.submit:
			d.waiting = append(d.waiting, queued)
		default:
			return
		}
	}
}

// reply never blocks: a full or abandoned reply channel means the caller is gone.
func (d *dispatcher) reply(target *waiter, result pickResult) bool {
	if target.abandoned.Load() {
		return false
	}
	select {
	case target.replyCh <- result:
		return true
	default:
		return false
	}
}

func (d *dispatcher) dequeue(target *waiter) {
	for index, queued := range d.waiting {
		if queued != target {
			continue
		}
		copy(d.waiting[index:], d.waiting[index+1:])
		d.waiting[len(d.waiting)-1] = nil
		d.waiting = d.waiting[:len(d.waiting)-1]
		return
	}
}

func (d *dispatcher) recordGhost(reason string) {
	if d.observer != nil {
		d.observer.GhostBurned(d.escrowID, reason)
	}
}

func (d *dispatcher) recordHold() {
	if d.observer != nil {
		d.observer.NonceHeld(d.escrowID)
	}
}

func (d *dispatcher) recordBudgetTrip() {
	if d.observer != nil {
		d.observer.BurnBudgetExhausted(d.escrowID)
	}
}

func intentFor(decision Decision) NonceIntent {
	switch outcome := decision.(type) {
	case serve:
		return NonceIntent{Commit: true, Params: outcome.waiter.profile.Params}
	case burn:
		return NonceIntent{Commit: true, Ghost: true}
	default:
		return NonceIntent{}
	}
}

type capabilityKey struct {
	participant   string
	model         string
	requiresTools bool
	contextHint   uint64
}

// freeze memoises each predicate for the length of one drain, so a host cannot look usable to the
// sweep and unusable to the binding that follows it.
func freeze(live availability) availability {
	capabilities := map[capabilityKey]bool{}
	return availability{
		pocRequired:  memoise(live.pocRequired),
		throttled:    memoise(live.throttled),
		stateBlocked: memoise(live.stateBlocked),
		capability: func(participant string, profile RequestProfile) bool {
			key := capabilityKey{
				participant:   participant,
				model:         profile.Model,
				requiresTools: profile.RequiresTools,
				contextHint:   profile.ContextHint,
			}
			blocked, known := capabilities[key]
			if !known {
				blocked = live.capability(participant, profile)
				capabilities[key] = blocked
			}
			return blocked
		},
	}
}

func memoise(live func(string) bool) func(string) bool {
	answers := map[string]bool{}
	return func(participant string) bool {
		answer, known := answers[participant]
		if !known {
			answer = live(participant)
			answers[participant] = answer
		}
		return answer
	}
}
