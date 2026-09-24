// Package registry owns the live escrow set: each escrow's session handle, its membership in the
// participant capacity model, and the in-flight request count routing scores it by.
package registry

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/scheduler"
)

var (
	// ErrClosed rejects work on a registry whose sessions have already been released.
	ErrClosed = errors.New("registry closed")

	// ErrDraining refuses an id whose earlier entry still owns nonces awaiting votes and the storage they settle through.
	ErrDraining = errors.New("escrow is still draining")
)

type Deps struct {
	ServingSessions  SessionFactory
	ReadOnlySessions SessionFactory
	Membership       membership
	Exhaustion       exhaustion
	Publications     publications
	Narrator         escrowNarrator
	Retiring         retiringObserver
	Now              func() time.Time
}

// retiringObserver is told about an escrow once its last request has ended and before its session is
// released, which is the last moment anything can be read from it. See README.md, "Publishing, retiring and draining".
type retiringObserver func(escrowID string, session EscrowSession)

// Registry owns the live escrow set: live is written only under mu and read without it. See routing.md, "The escrow registry".
type Registry struct {
	servingSessions  SessionFactory
	readOnlySessions SessionFactory
	membership       membership
	sessions         atomic.Uint64
	exhaustion       exhaustion
	publications     publications
	narrator         escrowNarrator
	retiring         retiringObserver
	now              func() time.Time

	live         atomic.Pointer[liveSet]
	drainingView atomic.Pointer[[]*escrowEntry]

	openings sync.Map

	drainCloseFailures atomic.Int64

	closing sync.WaitGroup

	// sweepCursor rotates where the timeout sweep starts, so one escrow's backlog cannot hold the budget.
	sweepCursor atomic.Uint64

	mu       sync.Mutex
	draining map[*escrowEntry]struct{}
	closed   bool
}

func New(deps Deps) *Registry {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	registry := &Registry{
		servingSessions:  deps.ServingSessions,
		readOnlySessions: deps.ReadOnlySessions,
		membership:       deps.Membership,
		exhaustion:       deps.Exhaustion,
		publications:     deps.Publications,
		narrator:         deps.Narrator,
		retiring:         deps.Retiring,
		now:              deps.Now,
		draining:         map[*escrowEntry]struct{}{},
	}
	registry.live.Store(emptyLiveSet())
	return registry
}

// Add publishes one escrow for routing and releases an unpublished session without flushing. See routing.md, "The escrow registry".
func (r *Registry) Add(ctx context.Context, escrowID, model string) error {
	return r.add(ctx, escrowID, model, false)
}

// AddOnHold publishes an escrow whose row is on hold, so a restart does not route it for a moment first.
func (r *Registry) AddOnHold(ctx context.Context, escrowID, model string) error {
	return r.add(ctx, escrowID, model, true)
}

func (r *Registry) add(ctx context.Context, escrowID, model string, onHold bool) error {
	switch {
	case escrowID == "":
		return fmt.Errorf("escrow id is required")
	case model == "":
		return fmt.Errorf("escrow %s: model is required", escrowID)
	case r.servingSessions == nil:
		return fmt.Errorf("escrow %s: no serving session factory", escrowID)
	}
	if err := r.refuseIfDraining(escrowID); err != nil {
		return err
	}

	opening := r.openingLock(escrowID)
	opening.Lock()
	defer opening.Unlock()
	if _, alreadyLive := r.live.Load().byID[escrowID]; alreadyLive {
		return nil
	}

	session, err := r.servingSessions(ctx, escrowID)
	if err != nil {
		return fmt.Errorf("opening serving session for escrow %s: %w", escrowID, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.Join(ErrClosed, session.Close())
	}
	if err := r.refuseIfDrainingLocked(escrowID); err != nil {
		return errors.Join(err, session.Close())
	}
	published := r.live.Load()
	if _, alreadyLive := published.byID[escrowID]; alreadyLive {
		return session.Close()
	}
	entry := newEscrowEntry(escrowID, model, r.sessions.Add(1), session, r.now)
	entry.onHold.Store(onHold)
	entry.hold = r.holdFor(entry)
	r.live.Store(published.with(entry))
	r.pushMembershipLocked()
	if r.publications != nil {
		r.publications.EscrowPublished(escrowID, model)
	}
	if r.narrator != nil {
		r.narrator.EscrowServing(escrowID, model)
	}
	return nil
}

// openingLock serializes opens of one escrow, whose session is a SQLite file. See README.md, "Publishing, retiring and draining".
func (r *Registry) openingLock(escrowID string) *sync.Mutex {
	lock, _ := r.openings.LoadOrStore(escrowID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (r *Registry) refuseIfDraining(escrowID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.refuseIfDrainingLocked(escrowID)
}

func (r *Registry) refuseIfDrainingLocked(escrowID string) error {
	for entry := range r.draining {
		if entry.id == escrowID {
			return fmt.Errorf("escrow %s: %w", escrowID, ErrDraining)
		}
	}
	return nil
}

// Retire stops routing to an escrow; its session is released once the requests already running end.
func (r *Registry) Retire(escrowID string) error {
	entry, closing := r.unpublish(escrowID)
	if !closing {
		return nil
	}
	return r.closeDraining(entry)
}

// unpublish takes an escrow out of routing and reports whether the caller owns its close.
func (r *Registry) unpublish(escrowID string) (*escrowEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	published := r.live.Load()
	entry, known := published.byID[escrowID]
	if !known {
		return nil, false
	}
	r.live.Store(published.without(escrowID))
	if r.membership != nil {
		r.membership.RemoveEscrow(escrowID)
	}
	r.pushMembershipLocked()
	r.draining[entry] = struct{}{}
	r.publishDrainingLocked()
	if entry.busy() || entry.settlementHolds.Load() > 0 {
		if r.narrator != nil {
			r.narrator.EscrowRetiredDraining(escrowID, entry.inFlight.Load())
		}
		return nil, false
	}
	entry.closeClaimed = true
	if r.narrator != nil {
		r.narrator.EscrowRetired(escrowID)
	}
	return entry, true
}

// closeDraining releases the session with the registry lock free, in the session-then-registry lock order.
func (r *Registry) closeDraining(entry *escrowEntry) error {
	r.flushPendingAtRetirement(entry)
	if r.retiring != nil {
		r.retiring(entry.id, entry.session)
	}
	// Only an unreleased store leaves the entry in draining, so Add keeps refusing that id.
	released, err := entry.close()
	if !released {
		r.mu.Lock()
		entry.closeClaimed = false
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	delete(r.draining, entry)
	r.publishDrainingLocked()
	r.mu.Unlock()
	return err
}

// publishDrainingLocked keeps Snapshot lock-free: a scrape cannot wait on a retirement.
func (r *Registry) publishDrainingLocked() {
	entries := drainingInIDOrder(r.draining)
	r.drainingView.Store(&entries)
}

// release closes a drained escrow off the request's goroutine. See README.md, "Publishing, retiring and draining".
func (r *Registry) release(entry *escrowEntry) {
	r.mu.Lock()
	entry.inFlight.Add(-1)
	claimed := r.claimCloseLocked(entry)
	r.mu.Unlock()
	if claimed {
		r.closeInBackground(entry)
	}
}

func (r *Registry) releaseSettlement(entry *escrowEntry) {
	r.mu.Lock()
	entry.settlementHolds.Add(-1)
	claimed := r.claimCloseLocked(entry)
	r.mu.Unlock()
	if claimed {
		r.closeInBackground(entry)
	}
}

func (r *Registry) closeInBackground(entry *escrowEntry) {
	go func() {
		defer r.closing.Done()
		closeErr := r.closeDraining(entry)
		if closeErr != nil {
			r.drainCloseFailures.Add(1)
		}
		if r.narrator != nil {
			r.narrator.DrainingEscrowClosed(entry.id, closeErr)
		}
	}()
}

func (r *Registry) claimCloseLocked(entry *escrowEntry) bool {
	if entry.inFlight.Load() > 0 || entry.settlementHolds.Load() > 0 || entry.closeClaimed || r.closed {
		return false
	}
	if _, isDraining := r.draining[entry]; !isDraining {
		return false
	}
	entry.closeClaimed = true
	r.closing.Add(1)
	return true
}

// DrainCloseFailures counts a flush or close failure with no caller left to return it to.
func (r *Registry) DrainCloseFailures() int64 { return r.drainCloseFailures.Load() }

// IsBusy satisfies escrow.SettlementSource: a settlement must not claim funds while nonces are still being spent.
func (r *Registry) IsBusy(escrowID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, known := r.live.Load().byID[escrowID]; known && entry.busy() {
		return true
	}
	for entry := range r.draining {
		if entry.id == escrowID && entry.busy() {
			return true
		}
	}
	return false
}

// Exhausted reports an escrow routing declined as spent to the rotation lifecycle, which is what replaces it.
func (r *Registry) Exhausted(escrowID string, reason scheduler.ExhaustionReason) {
	if r.exhaustion == nil {
		return
	}
	r.exhaustion.OnBalanceExhausted(escrowID, reason)
}

// Close releases every session still held, including the escrows still draining.
func (r *Registry) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.closing.Wait()

	r.mu.Lock()
	published := r.live.Load()
	closing := make([]*escrowEntry, 0, len(published.byID)+len(r.draining))
	for _, id := range sortedKeys(published.byID) {
		closing = append(closing, published.byID[id])
	}
	closing = append(closing, drainingInIDOrder(r.draining)...)
	r.live.Store(emptyLiveSet())
	clear(r.draining)
	r.mu.Unlock()

	errs := make([]error, 0, len(closing))
	for _, entry := range closing {
		_, err := entry.close()
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func drainingInIDOrder(draining map[*escrowEntry]struct{}) []*escrowEntry {
	entries := make([]*escrowEntry, 0, len(draining))
	for entry := range draining {
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(first, second *escrowEntry) int { return cmp.Compare(first.id, second.id) })
	return entries
}

// pushMembershipLocked republishes every live escrow's share. See routing.md, "Membership: what the capacity model is told".
func (r *Registry) pushMembershipLocked() {
	if r.membership == nil {
		return
	}
	published := r.live.Load()
	totalSlots := map[string]int{}
	for _, id := range sortedKeys(published.byID) {
		for participant, count := range published.byID[id].slots {
			totalSlots[participant] += count
		}
	}
	for _, id := range sortedKeys(published.byID) {
		r.membership.SetEscrowMembership(id, hostShares(published.byID[id].slots, totalSlots))
	}
}

func sortedKeys[K cmp.Ordered, V any](entries map[K]V) []K {
	keys := make([]K, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
