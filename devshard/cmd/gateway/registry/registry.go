// Package registry owns the live escrow set: each escrow's session handle, its membership in the
// participant capacity model, and the in-flight request count routing scores it by.
package registry

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
)

var (
	// ErrClosed rejects work on a registry whose sessions have already been released.
	ErrClosed = errors.New("registry closed")

	// ErrDraining refuses an escrow id whose earlier entry has not finished: that entry owns the nonces
	// still awaiting votes and holds the storage they settle through.
	ErrDraining = errors.New("escrow is still draining")
)

type Deps struct {
	ServingSessions  SessionFactory
	ReadOnlySessions SessionFactory
	Membership       membership
	Exhaustion       exhaustion
	Now              func() time.Time
}

// Registry owns the live escrow set. live is written only under mu and read without it, so Candidates costs
// one atomic load; draining holds escrows removed from routing whose requests have not finished, keyed by
// entry rather than id. See gateway-routing-and-nonces.md, "The escrow registry".
type Registry struct {
	servingSessions  SessionFactory
	readOnlySessions SessionFactory
	membership       membership
	exhaustion       exhaustion
	now              func() time.Time

	live         atomic.Pointer[liveSet]
	drainingView atomic.Pointer[[]*escrowEntry]

	openings sync.Map

	drainCloseFailures atomic.Int64

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
		now:              deps.Now,
		draining:         map[*escrowEntry]struct{}{},
	}
	registry.live.Store(emptyLiveSet())
	return registry
}

// Add publishes one escrow for routing, refuses an id an earlier entry is still draining, and releases an
// unpublished session without flushing. See gateway-routing-and-nonces.md, "The escrow registry".
func (r *Registry) Add(ctx context.Context, escrowID, model string) error {
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
	entry := newEscrowEntry(escrowID, model, session, r.now)
	entry.hold = r.holdFor(entry)
	r.live.Store(published.with(entry))
	r.pushMembershipLocked()
	logging.Info("escrow serving", "escrow", escrowID, "model", model)
	return nil
}

// openingLock serializes opens of one escrow. A session is a SQLite file: two concurrent opens fail
// with SQLITE_BUSY before either reaches the already-published check below, so the escrow an operator
// just created can report a failure while another caller serves it. Locks are kept for the life of the
// process -- one small mutex per escrow id ever published -- because removing one a waiter still holds
// would let the next caller take a fresh lock and race it.
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

// unpublish takes an escrow out of routing and reports whether the caller owns its close. An entry
// stays in draining until the close finishes, whether or not requests were running, so Add refuses the
// id for the whole close rather than opening a second session over storage the first still holds.
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
	if entry.busy() {
		logging.Info("escrow retired, draining", "escrow", escrowID, "in_flight", entry.inFlight.Load())
		return nil, false
	}
	logging.Info("escrow retired", "escrow", escrowID)
	return entry, true
}

// closeDraining releases the session with the registry lock free. Closing flushes the snapshot, which
// takes the session lock, and a dispatch takes the session lock before the registry lock: holding the
// two in the opposite order here wedges every later route and settlement behind one retirement. It
// also keeps the snapshot write and the storage close off the path of every pick.
func (r *Registry) closeDraining(entry *escrowEntry) error {
	// A close that failed leaves the entry in draining on purpose: its storage is still held, and Add
	// must keep refusing that id rather than open a second session over it.
	if err := entry.close(); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.draining, entry)
	r.publishDrainingLocked()
	r.mu.Unlock()
	return nil
}

// publishDrainingLocked keeps Snapshot lock-free: a scrape cannot wait on a retirement.
func (r *Registry) publishDrainingLocked() {
	entries := make([]*escrowEntry, 0, len(r.draining))
	for entry := range r.draining {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(first, second int) bool { return entries[first].id < entries[second].id })
	r.drainingView.Store(&entries)
}

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
	sort.Strings(models)
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

func (r *Registry) release(entry *escrowEntry) {
	if !r.lastHoldDropped(entry) {
		return
	}
	if err := r.closeDraining(entry); err != nil {
		r.drainCloseFailures.Add(1)
		logging.Error("draining escrow failed to close, its storage stays held", "escrow", entry.id, "error", err)
		return
	}
	logging.Info("draining escrow closed", "escrow", entry.id)
}

// lastHoldDropped reports that this release ended the final in-flight request of a retired escrow, so
// exactly one caller closes it. The count reaches zero once, and only a retired entry is in draining.
func (r *Registry) lastHoldDropped(entry *escrowEntry) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry.inFlight.Add(-1) > 0 {
		return false
	}
	_, isDraining := r.draining[entry]
	return isDraining
}

// DrainCloseFailures counts the drained escrows whose flush or close failed. Retire and Close hand
// that error to their caller; here the request that kept the escrow open has already been answered
// and there is nobody left to return it to, so it is counted instead of lost.
func (r *Registry) DrainCloseFailures() int64 { return r.drainCloseFailures.Load() }

// IsBusy satisfies escrow.SettlementSource: a settlement must not claim funds while a request is
// still spending nonces on the escrow, including one still draining from an earlier session of it.
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

// Exhausted reports an escrow routing declined as spent -- out of nonces or out of deposit -- for the
// rotation lifecycle, which is what replaces it.
func (r *Registry) Exhausted(escrowID, reason string) {
	if r.exhaustion == nil {
		return
	}
	r.exhaustion.OnBalanceExhausted(escrowID, reason)
}

// Close releases every session still held, including the escrows still draining.
func (r *Registry) Close() error {
	r.mu.Lock()
	r.closed = true
	published := r.live.Load()
	closing := make([]*escrowEntry, 0, len(published.byID)+len(r.draining))
	for _, id := range sortedKeys(published.byID) {
		closing = append(closing, published.byID[id])
	}
	closing = append(closing, drainingInIDOrder(r.draining)...)
	r.live.Store(emptyLiveSet())
	clear(r.draining)
	r.mu.Unlock()

	var errs []error
	for _, entry := range closing {
		errs = append(errs, entry.close())
	}
	return errors.Join(errs...)
}

func drainingInIDOrder(draining map[*escrowEntry]struct{}) []*escrowEntry {
	entries := make([]*escrowEntry, 0, len(draining))
	for entry := range draining {
		entries = append(entries, entry)
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].id < entries[j].id })
	return entries
}

// pushMembershipLocked republishes every live escrow's share, not just the one that changed. See
// gateway-routing-and-nonces.md, "Membership: what the capacity model is told".
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
