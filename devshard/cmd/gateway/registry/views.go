package registry

import (
	"slices"
	"sync"

	"devshard/cmd/gateway/scheduler"
	"devshard/types"
)

// Candidates satisfies scheduler.escrowSource.
func (r *Registry) Candidates(model string) []scheduler.Escrow {
	entries := r.live.Load().byModel[model]
	candidates := make([]scheduler.Escrow, 0, len(entries))
	for _, entry := range entries {
		if !entry.routable() {
			continue
		}
		candidates = append(candidates, entry.candidate())
	}
	return candidates
}

// Routable answers for one escrow what Candidates answers for a model, by id rather than by scanning every model.
func (r *Registry) Routable(escrowID string) (scheduler.Escrow, bool) {
	entry, known := r.live.Load().byID[escrowID]
	if !known || !entry.routable() {
		return scheduler.Escrow{}, false
	}
	return entry.candidate(), true
}

// ResumeCandidate resolves a live, accepting escrow whether or not it is on hold, for the tick that decides when it may resume.
func (r *Registry) ResumeCandidate(escrowID string) (scheduler.Escrow, bool) {
	entry, known := r.live.Load().byID[escrowID]
	if !known || !entry.accepting() {
		return scheduler.Escrow{}, false
	}
	return entry.candidate(), true
}

func (r *Registry) OnHold(escrowID string) bool {
	entry, known := r.live.Load().byID[escrowID]
	return known && entry.onHold.Load()
}

// SetOnHold touches candidate selection alone: holds, membership, accounting and the session are unchanged.
func (r *Registry) SetOnHold(escrowID string, onHold bool) {
	if entry, known := r.live.Load().byID[escrowID]; known {
		entry.onHold.Store(onHold)
	}
}

// Funds reads the money an escrow holds for unresolved nonces; it deep-copies the state, so it is for rare events only.
func (r *Registry) Funds(escrowID string) (balance, reserved, challenged uint64, known bool) {
	entry, live := r.live.Load().byID[escrowID]
	if !live {
		return 0, 0, 0, false
	}
	for _, record := range entry.session.SnapshotState().Inferences {
		switch record.Status {
		case types.StatusPending, types.StatusStarted:
			reserved += record.ReservedCost
		case types.StatusChallenged:
			challenged += record.ActualCost
		}
	}
	return entry.session.Balance(), reserved, challenged, true
}

func (e *escrowEntry) candidate() scheduler.Escrow {
	return scheduler.Escrow{
		ID:          e.id,
		Model:       e.model,
		SessionID:   e.sessionID,
		Session:     e.stream,
		ActiveUsers: int(e.inFlight.Load()),
		Hold:        e.hold,
	}
}

type EscrowState struct {
	ID           string
	Model        string
	Accepting    bool
	OnHold       bool
	InFlight     int64
	Participants []string
}

// Snapshot takes no registry lock, and includes the retired escrows still draining. See capacity.md, "What in-flight actually counts".
func (r *Registry) Snapshot() []EscrowState {
	published := r.live.Load()
	states := make([]EscrowState, 0, len(published.ordered))
	for _, entry := range published.ordered {
		states = append(states, stateOf(entry, false))
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
		OnHold:       !draining && entry.onHold.Load(),
		InFlight:     entry.inFlight.Load(),
		Participants: slices.Clone(entry.participants),
	}
}

// RoutableSession takes no in-flight count and is deliberately not the dispatch path; that is Acquire.
func (r *Registry) RoutableSession(escrowID string) (EscrowSession, bool) {
	entry, known := r.live.Load().byID[escrowID]
	if !known {
		return nil, false
	}
	return entry.session, true
}

// SettlementSession resolves published or draining, deliberately asymmetric with routing. See rules.md, "4. Routing and settlement read the escrow set asymmetrically".
func (r *Registry) SettlementSession(escrowID string) (EscrowSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, known := r.settlementEntryLocked(escrowID)
	if !known {
		return nil, false
	}
	return entry.session, true
}

// HoldSettlement: see README.md, "The published set and its readers".
func (r *Registry) HoldSettlement(escrowID string) (EscrowSession, func(), bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, known := r.settlementEntryLocked(escrowID)
	if !known || entry.closeClaimed {
		return nil, nil, false
	}
	entry.settlementHolds.Add(1)
	var once sync.Once
	return entry.session, func() { once.Do(func() { r.releaseSettlement(entry) }) }, true
}

func (r *Registry) settlementEntryLocked(escrowID string) (*escrowEntry, bool) {
	if entry, known := r.live.Load().byID[escrowID]; known {
		return entry, true
	}
	for entry := range r.draining {
		if entry.id == escrowID {
			return entry, true
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

// Acquire resolves the dispatch handle and counts one in-flight request in the same locked step. See routing.md, "The escrow registry".
func (r *Registry) Acquire(escrowID string) (session EscrowSession, release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, known := r.live.Load().byID[escrowID]
	if !known {
		return nil, nil, false
	}
	return entry.session, r.holdLocked(entry), true
}

// holdFor is bound to the entry rather than its id. See routing.md, "Where the nonce, the slot and the hold are taken".
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

// HostDials is every address the live escrows can reach, once each. See README.md.
func (r *Registry) HostDials() []HostDial {
	published := r.live.Load()
	seen := make(map[string]struct{}, len(published.ordered))
	dials := make([]HostDial, 0, len(published.ordered))
	for _, entry := range published.ordered {
		for _, dial := range entry.session.HostDials() {
			if dial.BaseURL == "" {
				continue
			}
			if _, known := seen[dial.BaseURL]; known {
				continue
			}
			seen[dial.BaseURL] = struct{}{}
			dials = append(dials, dial)
		}
	}
	return dials
}
