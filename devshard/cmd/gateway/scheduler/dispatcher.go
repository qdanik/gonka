package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

const defaultSubmitBuffer = 64

// EscrowRetired lets an observer forget an escrow: ids are monotonic chain identifiers and are never
// reused, so a per-escrow metric series that outlives its escrow grows with uptime and nothing else.
type dispatchObserver interface {
	GhostBurned(escrowID string, nonce uint64, participant, reason string)
	NonceHeld(escrowID string)
	BurnBudgetExhausted(escrowID string)
	EscrowRetired(escrowID string)
}

// dispatcherDeps wires one escrow's actor. acquireSlot and holdEscrow run inside the same Advance that
// commits the nonce -- holdEscrow on the serve path only, so a ghost commits unprotected against a
// concurrent retire. The slot travels with the assignment, so releaseSlot covers only the paths that never
// reach a dispatch; retire is answered under the lock that guards claims, so a claimed actor is never lost.
type dispatcherDeps struct {
	escrowID     string
	session      session
	snapshots    snapshotSource
	predicates   func(chain.PhaseSnapshot) availability
	acquireSlot  func(participant string) bool
	releaseSlot  func(participant string)
	holdEscrow   func() (func(), bool)
	observer     dispatchObserver
	now          func() time.Time
	matchWait    time.Duration
	newTimer     func(time.Duration) (<-chan time.Time, func())
	retire       func(*dispatcher) bool
	idleGrace    time.Duration
	submitBuffer int
	onExhausted  func(escrowID, reason string)
}

type submitOutcome int

const (
	submitAccepted submitOutcome = iota
	submitStopped
	submitFull
)

// dispatcher turns one escrow's sequential nonce stream into request assignments; its loop goroutine is the
// sole owner of waiting and of the session. See gateway-routing-and-nonces.md, "The per-escrow dispatcher".
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
	if deps.holdEscrow == nil {
		deps.holdEscrow = func() (func(), bool) { return nil, true }
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
// not stall its caller or pin the lifecycle lock. A full queue and a stopped dispatcher are reported
// apart. See gateway-routing-and-nonces.md, "The per-escrow dispatcher".
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

// markStopped shuts the actor down from inside its own goroutine, refusing once a waiter is in the submit
// buffer. See gateway-routing-and-nonces.md, "Idle dispatchers are reaped".
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

// failAdvance answers the whole queue, not just the waiter a serve decision chose: the session could not
// advance its nonce at all. A spent deposit is terminal for the escrow rather than for this request, so
// only the exhaustion notice gets it replaced.
func (d *dispatcher) failAdvance(decision Decision, taken reservation, err error) {
	if _, chosen := decision.(serve); chosen {
		d.giveBack(taken)
	}
	if errors.Is(err, types.ErrInsufficientBalance) && d.onExhausted != nil {
		d.onExhausted(d.escrowID, "insufficient_balance")
	}
	d.failWaiting(fmt.Errorf("escrow %s: advancing nonce: %w", d.escrowID, err))
}

func (d *dispatcher) failWaiting(err error) {
	for index, queued := range d.waiting {
		queued.deliver(pickResult{err: err})
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

func (d *dispatcher) recordGhost(nonce uint64, participant, reason string) {
	if d.observer != nil {
		d.observer.GhostBurned(d.escrowID, nonce, participant, reason)
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

// memoiseOrNil keeps a predicate nobody set nil rather than wrapping it, so the reader above can tell
// "no allowlist" from "an allowlist that refuses everybody".
func memoiseOrNil(predicate func(string) bool) func(string) bool {
	if predicate == nil {
		return nil
	}
	return memoise(predicate)
}

func freeze(live availability) availability {
	return availability{
		pocRequired:  memoise(live.pocRequired),
		throttled:    memoise(live.throttled),
		ejected:      memoise(live.ejected),
		notAllowed:   memoiseOrNil(live.notAllowed),
		stateBlocked: memoise(live.stateBlocked),
	}
}

// admit couples the drain's admission to its frozen predicates: a participant whose window refused a slot
// counts as throttled for the rest of the drain. See
// gateway-routing-and-nonces.md, "Where the nonce, the slot and the hold are taken".
func admit(avail *availability, acquire func(string) bool) func(string) bool {
	refused := map[string]bool{}
	throttled := avail.throttled
	avail.throttled = func(participant string) bool { return refused[participant] || throttled(participant) }
	return func(participant string) bool {
		if acquire(participant) {
			return true
		}
		refused[participant] = true
		return false
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
