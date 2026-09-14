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

// offer is what one Advance decided, kept across the loop so the decide closure is built once per drain.
type offer struct {
	decision      Decision
	taken         reservation
	escrowRetired bool
	throttled     *waiter
}

// drain assigns nonces until the queue empties, a nonce is held, or the burn budget trips. See README, "The drain".
func (d *dispatcher) drain() (time.Time, bool) {
	participants := d.session.ParticipantKeys()
	avail := freeze(d.predicates(d.snapshots.Snapshot()), len(participants))
	acquire := admit(&avail, d.acquireSlot)
	burnBudget := d.session.GroupSize() * (len(d.waiting) + 1)

	var offered offer
	decide := func(binding HostBinding) NonceIntent {
		offered.taken.participant = binding.Participant
		offered.decision = match(binding, d.waiting, participants, avail, d.now(), d.matchWait)
		switch decided := offered.decision.(type) {
		case serve:
			if !acquire(binding.Participant) {
				offered.throttled = decided.waiter
				offered.decision = burn{kind: ghostThrottled}
			}
		case burn:
		default:
			return intentFor(offered.decision)
		}
		var held bool
		if offered.taken.escrowHold, held = d.holdEscrow(); !held {
			if _, serving := offered.decision.(serve); serving {
				d.releaseSlot(binding.Participant)
			}
			offered.escrowRetired = true
			return NonceIntent{}
		}
		return intentFor(offered.decision)
	}

	for {
		d.sweepExhausted(participants, avail)
		if len(d.waiting) == 0 {
			return time.Time{}, false
		}

		offered = offer{}
		prepared, err := d.session.Advance(decide)
		switch {
		case offered.escrowRetired:
			d.failWaiting(ErrEscrowGone)
			return time.Time{}, false
		case err != nil:
			d.failAdvance(offered.decision, offered.taken, err)
			return time.Time{}, false
		}

		switch outcome := offered.decision.(type) {
		case serve:
			if outcome.despiteExclusion {
				logging.Info("nonce spent on a host the request excluded", logkey.Escrow, d.escrowID,
					logkey.Host, logkey.ShortHost(offered.taken.participant))
			}
			d.handOff(outcome.waiter, offered.taken, prepared)
		case burn:
			// A real session always commits the ghost it was asked for; only a session double leaves Nonce zero.
			burned := Burn{Participant: offered.taken.participant, Reason: outcome.kind.reason(), RequestID: d.burnedDuring(offered.throttled)}
			if prepared != nil {
				burned.Nonce = prepared.Nonce()
			}
			d.recordGhost(burned)
			offered.taken.releaseHold()
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

// burnedDuring names the waiter a refused slot was meant for, else the oldest waiter still waiting. See README, "The boundary types".
func (d *dispatcher) burnedDuring(throttled *waiter) string {
	if throttled != nil {
		return throttled.profile.RequestID
	}
	for _, queued := range d.waiting {
		if !queued.abandoned.Load() {
			return queued.profile.RequestID
		}
	}
	return ""
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

// reservation is what a decision took: a serve the slot and the escrow hold, a burn the hold alone. See routing.md, "Where the nonce, the slot and the hold are taken".
type reservation struct {
	participant string
	escrowHold  func()
}

func (r reservation) releaseHold() {
	if r.escrowHold != nil {
		r.escrowHold()
	}
}

func (d *dispatcher) giveBack(taken reservation) {
	d.releaseSlot(taken.participant)
	taken.releaseHold()
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
		d.recordGhost(Burn{Nonce: prepared.Nonce(), Participant: taken.participant, Reason: ghostAbandoned.reason(), RequestID: served.profile.RequestID})
	}
}
