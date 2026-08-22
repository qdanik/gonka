package registry

import (
	"slices"
	"sync"

	"devshard/cmd/gateway/scheduler"
)

// Candidates satisfies scheduler.escrowSource.
func (r *Registry) Candidates(model string) []scheduler.Escrow {
	entries := r.live.Load().byModel[model]
	candidates := make([]scheduler.Escrow, 0, len(entries))
	for _, entry := range entries {
		if !entry.accepting() {
			continue
		}
		candidates = append(candidates, scheduler.Escrow{
			ID:          entry.id,
			Model:       entry.model,
			Session:     entry.stream,
			ActiveUsers: int(entry.inFlight.Load()),
			Hold:        entry.hold,
		})
	}
	return candidates
}

type EscrowState struct {
	ID           string
	Model        string
	Accepting    bool
	InFlight     int64
	Participants []string
}

// Snapshot returns every published escrow in id order, plus the retired ones still draining. It takes
// no lock. See gateway-capacity-and-health.md, "What in-flight actually counts".
func (r *Registry) Snapshot() []EscrowState {
	published := r.live.Load()
	states := make([]EscrowState, 0, len(published.byID))
	for _, id := range sortedKeys(published.byID) {
		states = append(states, stateOf(published.byID[id], false))
	}
	if draining := r.drainingView.Load(); draining != nil {
		for _, entry := range *draining {
			if entry.busy() {
				states = append(states, stateOf(entry, true))
			}
		}
	}
	return states
}

func stateOf(entry *escrowEntry, draining bool) EscrowState {
	return EscrowState{
		ID:           entry.id,
		Model:        entry.model,
		Accepting:    !draining && entry.accepting(),
		InFlight:     entry.inFlight.Load(),
		Participants: sortedKeys(entry.slots),
	}
}

// RoutableSession is the read-only handle the status routes read. It takes no in-flight count and is
// deliberately not the dispatch path: a race resolves its escrow through Acquire, which returns the
// session and its release together so a handle cannot be held without the hold.
func (r *Registry) RoutableSession(escrowID string) (EscrowSession, bool) {
	entry, known := r.live.Load().byID[escrowID]
	if !known {
		return nil, false
	}
	return entry.session, true
}

// SettlementSession resolves the handle this process still holds, published or draining -- deliberately
// asymmetric with routing's published-only lookup. See gateway-invariants.md,
// "4. Routing and settlement read the escrow set asymmetrically, on purpose".
func (r *Registry) SettlementSession(escrowID string) (EscrowSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, known := r.live.Load().byID[escrowID]; known {
		return entry.session, true
	}
	for _, entry := range drainingInIDOrder(r.draining) {
		if entry.id == escrowID {
			return entry.session, true
		}
	}
	return nil, false
}

func (r *Registry) Models() []string {
	published := r.live.Load()
	models := make([]string, 0, len(published.byModel))
	for model, entries := range published.byModel {
		if slices.ContainsFunc(entries, (*escrowEntry).accepting) {
			models = append(models, model)
		}
	}
	slices.Sort(models)
	return models
}

func (r *Registry) Serves(model string) bool {
	return slices.ContainsFunc(r.live.Load().byModel[model], (*escrowEntry).accepting)
}

// Acquire resolves the dispatch handle and counts one in-flight request against it in the same locked step;
// ok is false when the escrow is gone and the caller must not dispatch to it. See
// gateway-routing-and-nonces.md, "The escrow registry".
func (r *Registry) Acquire(escrowID string) (session EscrowSession, release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, known := r.live.Load().byID[escrowID]
	if !known {
		return nil, nil, false
	}
	return entry.session, r.holdLocked(entry), true
}

// holdFor is the scheduler's view of the same count, taken in the step that commits a nonce and bound to
// the entry rather than to its id. See gateway-routing-and-nonces.md, "Where the nonce, the slot and the
// hold are taken".
func (r *Registry) holdFor(entry *escrowEntry) func() (func(), bool) {
	return func() (func(), bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.live.Load().byID[entry.id] != entry {
			return nil, false
		}
		return r.holdLocked(entry), true
	}
}

func (r *Registry) holdLocked(entry *escrowEntry) func() {
	entry.inFlight.Add(1)
	var once sync.Once
	return func() { once.Do(func() { r.release(entry) }) }
}
