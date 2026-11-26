// Package share counts the nonces each host is bound, served, failed and passed, and turns the counts into the targets of host_load_mode. See docs/proposals/gateway-weight-proportional-load.md.
package share

import (
	"math"
	"sync"
	"time"
)

// Decayed is the six decayed counts of one host.
type Decayed struct {
	Assigned, Offered, Served, Failed, Passed, Excused float64
}

// Totals is every count of one host since the last Reset.
type Totals [kinds]uint64

func (t Totals) Of(kind Kind) uint64 { return t[kind] }

type key struct{ model, participant string }

type host struct {
	decayed [decayedKinds]float64
	touched time.Time
	totals  Totals
}

// Ledger is safe for concurrent use; it changes no routing by itself.
type Ledger struct {
	mu       sync.Mutex
	now      func() time.Time
	halfLife func(model string) time.Duration
	hosts    map[key]*host
}

// New reads the half-life per count, so an admin change reaches a running ledger.
func New(now func() time.Time, halfLife func(model string) time.Duration) *Ledger {
	return &Ledger{now: now, halfLife: halfLife, hosts: map[key]*host{}}
}

// Count adds one event of kind to the host.
func (l *Ledger) Count(model, participant string, kind Kind) {
	l.mu.Lock()
	defer l.mu.Unlock()
	state := l.hostLocked(key{model, participant})
	state.totals[kind]++
	if int(kind) < decayedKinds {
		now := l.now()
		l.decayLocked(state, model, now)
		state.decayed[kind]++
	}
}

// Decayed reads the host's decayed counts at the current time.
func (l *Ledger) Decayed(model, participant string) Decayed {
	l.mu.Lock()
	defer l.mu.Unlock()
	state, known := l.hosts[key{model, participant}]
	if !known {
		return Decayed{}
	}
	l.decayLocked(state, model, l.now())
	return Decayed{
		Assigned: state.decayed[Assigned], Offered: state.decayed[Offered], Served: state.decayed[Served],
		Failed: state.decayed[Failed], Passed: state.decayed[Passed], Excused: state.decayed[Excused],
	}
}

// Totals reads the host's counts since the last Reset.
func (l *Ledger) Totals(model, participant string) Totals {
	l.mu.Lock()
	defer l.mu.Unlock()
	if state, known := l.hosts[key{model, participant}]; known {
		return state.totals
	}
	return Totals{}
}

// Hosts lists the participants the ledger holds a count for under the model.
func (l *Ledger) Hosts(model string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var participants []string
	for tracked := range l.hosts {
		if tracked.model == model {
			participants = append(participants, tracked.participant)
		}
	}
	return participants
}

// Reset forgets every count, at the epoch boundary where host state is reset.
func (l *Ledger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	clear(l.hosts)
}

func (l *Ledger) hostLocked(tracked key) *host {
	state, known := l.hosts[tracked]
	if !known {
		state = &host{}
		l.hosts[tracked] = state
	}
	return state
}

func (l *Ledger) decayLocked(state *host, model string, now time.Time) {
	halfLife := l.halfLife(model)
	if elapsed := now.Sub(state.touched); !state.touched.IsZero() && elapsed > 0 && halfLife > 0 {
		weight := math.Exp2(-float64(elapsed) / float64(halfLife))
		for index := range state.decayed {
			state.decayed[index] *= weight
		}
	}
	state.touched = now
}
