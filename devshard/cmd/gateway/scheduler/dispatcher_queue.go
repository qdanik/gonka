package scheduler

import (
	"fmt"
	"time"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
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

// drain assigns nonces until the queue empties, a nonce is held, or the burn budget trips. See README, "The drain".
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
			if outcome.despiteExclusion {
				logging.Info("nonce spent on a host the request excluded", logkey.Escrow, d.escrowID,
					logkey.Host, logkey.ShortHost(taken.participant))
			}
			d.handOff(outcome.waiter, taken, prepared)
		case burn:
			// A burn decided before the session could commit has no nonce to name.
			burned := Burn{Participant: taken.participant, Reason: outcome.kind.reason()}
			if prepared != nil {
				burned.Nonce, burned.Prepared = prepared.Nonce(), prepared
			}
			d.recordGhost(burned)
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
		canServe, busy, chainBlocked, excludedOnly := servable(queued, participants, avail)
		// An excluded host is not a dead end: past the stale window match spends the nonce on it.
		if canServe || excludedOnly {
			return true
		}
		switch {
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

// servable separates a busy host, which passes on its own, from one the chain has stopped. See README, "The drain".
func servable(queued *waiter, participants []string, avail availability) (canServe, busy, chainBlocked, excludedOnly bool) {
	anyBusy, anyChainBlocked, anyExcluded := false, false, false
	for _, participant := range participants {
		switch avail.blocks(participant, queued) {
		case blockNone:
			return true, false, false, false
		case blockThrottled:
			anyBusy = true
		case blockPoCRequired:
			anyChainBlocked = true
		case blockExcluded:
			anyExcluded = true
		}
	}
	return false, anyBusy, anyChainBlocked, anyExcluded
}

// reservation is the slot and the escrow hold a serve took, given back together or not at all. See routing.md, "Where the nonce, the slot and the hold are taken".
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
		d.recordGhost(Burn{Nonce: prepared.Nonce(), Participant: taken.participant, Reason: ghostAbandoned.reason(), Prepared: prepared})
	}
}
