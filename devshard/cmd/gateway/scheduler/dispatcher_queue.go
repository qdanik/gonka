package scheduler

import (
	"fmt"
	"time"
)

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

// drain assigns nonces until the queue empties, a nonce is held, or the burn budget trips; the freeze
// and the budget are what bound its cost in nonces. It returns with a waiter still queued ONLY when it
// reports a held nonce -- the one exit the loop arms a timer for -- so nothing is parked unwoken.
func (d *dispatcher) drain() (time.Time, bool) {
	avail := freeze(d.predicates(d.snapshots.Snapshot()))
	acquire := admit(&avail, d.acquireSlot)
	participants := d.session.ParticipantKeys()
	burnBudget := d.session.GroupSize() * (len(d.waiting) + 1)

	for {
		d.sweepExhausted(participants, avail)
		if len(d.waiting) == 0 {
			return time.Time{}, false
		}

		var decision Decision
		var taken reservation
		escrowRetired := false
		prepared, err := d.session.Advance(func(binding HostBinding) NonceIntent {
			taken.participant = binding.Participant
			decision = match(binding, d.waiting, participants, avail, d.now(), d.matchWait)
			if _, serving := decision.(serve); !serving {
				return intentFor(decision)
			}
			if !acquire(binding.Participant) {
				decision = burn{kind: ghostThrottled}
				return intentFor(decision)
			}
			var held bool
			if taken.escrowHold, held = d.holdEscrow(); !held {
				d.releaseSlot(binding.Participant)
				escrowRetired = true
				return NonceIntent{}
			}
			return intentFor(decision)
		})
		switch {
		case escrowRetired:
			d.failWaiting(ErrEscrowGone)
			return time.Time{}, false
		case err != nil:
			d.failAdvance(decision, taken, err)
			return time.Time{}, false
		}

		switch outcome := decision.(type) {
		case serve:
			d.handOff(outcome.waiter, taken, prepared)
		case burn:
			// A burn decided before the session could commit has no nonce to name: the escrow is out of
			// them, or the decision was made without one being taken.
			ghostNonce := uint64(0)
			if prepared != nil {
				ghostNonce = prepared.Nonce()
			}
			d.recordGhost(ghostNonce, taken.participant, outcome.kind.reason())
			burnBudget--
			if burnBudget <= 0 {
				d.recordBudgetTrip()
				d.failWaiting(ErrNoAvailableHost)
				return time.Time{}, false
			}
		case hold:
			d.recordHold()
			return outcome.until, true
		case decline:
			d.dropAbandoned()
			return time.Time{}, false
		default:
			d.failWaiting(fmt.Errorf("escrow %s: session offered no binding", d.escrowID))
			return time.Time{}, false
		}
	}
}

// sweepExhausted drops waiters no available participant can serve, instantly and without touching the nonce.
func (d *dispatcher) sweepExhausted(participants []string, avail availability) {
	d.keepWaiting(func(queued *waiter) bool {
		if queued.abandoned.Load() {
			return false
		}
		canServe, toolsUnsupported, busy, chainBlocked := servable(queued, participants, avail)
		if canServe {
			return true
		}
		switch {
		case toolsUnsupported:
			queued.deliver(pickResult{err: ErrToolsUnsupported})
		case busy, chainBlocked:
			queued.deliver(pickResult{err: ErrHostsBusy})
		default:
			queued.deliver(pickResult{err: ErrNoAvailableHost})
		}
		return false
	})
}

// dropAbandoned answers nobody: the goroutine that left already delivered the waiter's result.
func (d *dispatcher) dropAbandoned() {
	d.keepWaiting(func(queued *waiter) bool { return !queued.abandoned.Load() })
}

// keepWaiting compacts the queue, clearing the tail so a departed waiter is not held by the array.
func (d *dispatcher) keepWaiting(accept func(*waiter) bool) {
	kept := d.waiting[:0]
	for _, queued := range d.waiting {
		if accept(queued) {
			kept = append(kept, queued)
		}
	}
	for index := len(kept); index < len(d.waiting); index++ {
		d.waiting[index] = nil
	}
	d.waiting = kept
}

// servable also reports the one blocking reason a caller can fix and no wait can: tool support. It
// separates a busy host from one the chain has stopped -- the first passes on its own and is what a
// waiter may be held for, the second lasts an epoch phase.
func servable(queued *waiter, participants []string, avail availability) (canServe, toolsUnsupported, busy, chainBlocked bool) {
	anyToolRefusal, anyOtherReason, anyBusy, anyChainBlocked := false, false, false, false
	for _, participant := range participants {
		switch reason := avail.blocks(participant, queued); reason {
		case blockNone:
			return true, false, false, false
		case blockToolsUnsupported:
			anyToolRefusal = true
		case blockThrottled:
			anyOtherReason, anyBusy = true, true
		case blockPoCRequired:
			anyOtherReason, anyChainBlocked = true, true
		default:
			anyOtherReason = true
		}
	}
	return false, anyToolRefusal && !anyOtherReason, anyBusy, anyChainBlocked
}

// reservation is what a serve decision took before the nonce was committed: the participant's concurrency
// slot and the escrow's in-flight hold, given back together or not at all. See
// gateway-routing-and-nonces.md, "Where the nonce, the slot and the hold are taken".
type reservation struct {
	participant string
	escrowHold  func()
}

func (d *dispatcher) giveBack(taken reservation) {
	d.releaseSlot(taken.participant)
	if taken.escrowHold != nil {
		taken.escrowHold()
	}
}

func (d *dispatcher) handOff(served *waiter, taken reservation, prepared Prepared) {
	d.dequeue(served)
	if prepared == nil {
		d.giveBack(taken)
		served.deliver(pickResult{err: fmt.Errorf("escrow %s: session committed no nonce", d.escrowID)})
		return
	}
	assignment := Assignment{Escrow: d.escrowID, Host: taken.participant, Nonce: prepared, EscrowHold: taken.escrowHold}
	if !served.deliver(pickResult{assignment: assignment}) {
		d.giveBack(taken)
		d.recordGhost(prepared.Nonce(), taken.participant, ghostAbandoned.reason())
	}
}
