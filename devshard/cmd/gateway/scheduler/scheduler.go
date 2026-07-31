// Package scheduler picks the escrow, host, and nonce that serve a chat-completions request.
package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

// idleDispatcherGrace is how long an escrow's actor stays alive with an empty queue. Escrows are
// replaced as they deplete, so actors that never retired would accumulate one dead session each.
const idleDispatcherGrace = 5 * time.Minute

// Deps wires the runtime facts routing reads. Escrows, Capacity, Limiter, Perf, Snapshots and Config
// are required; Observer, Now and SubmitBuffer fall back to a no-op, the wall clock, and the actor's
// default queue depth.
type Deps struct {
	Escrows      escrowSource
	Capacity     escrowWeights
	Limiter      hostLimiter
	Perf         hostCapability
	Snapshots    snapshotSource
	Config       *config.Holder
	Observer     dispatchObserver
	Now          func() time.Time
	SubmitBuffer int
	// OnNonceExhausted reports an escrow declined because its nonce budget is spent, so the rotation
	// lifecycle can schedule a replacement. Optional; nil disables the notification.
	OnNonceExhausted func(escrowID string)
}

type Scheduler struct {
	escrows          escrowSource
	capacity         escrowWeights
	limiter          hostLimiter
	perf             hostCapability
	snapshots        snapshotSource
	settings         *config.Holder
	observer         dispatchObserver
	now              func() time.Time
	newTimer         func(time.Duration) (<-chan time.Time, func())
	submitBuffer     int
	onNonceExhausted func(escrowID string)

	// tieBreak is one counter shared across every model and tie-set shape: a pseudo-round-robin over
	// whichever tie set exists right now, not a fair per-model rotation.
	tieBreak atomic.Int64

	registryMu  sync.Mutex
	dispatchers map[string]*dispatcher
	stopped     bool

	blocksMu     sync.RWMutex
	blockedHosts map[string]map[string]bool
}

func NewScheduler(deps Deps) *Scheduler {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Scheduler{
		escrows:          deps.Escrows,
		capacity:         deps.Capacity,
		limiter:          deps.Limiter,
		perf:             deps.Perf,
		snapshots:        deps.Snapshots,
		settings:         deps.Config,
		observer:         deps.Observer,
		now:              deps.Now,
		submitBuffer:     deps.SubmitBuffer,
		onNonceExhausted: deps.OnNonceExhausted,
		dispatchers:      map[string]*dispatcher{},
		blockedHosts:     map[string]map[string]bool{},
	}
}

// Pick serves one request or one escalation attempt within a request; an escalation reuses the
// pinned escrow rather than re-deriving it.
func (s *Scheduler) Pick(ctx context.Context, profile RequestProfile) (Assignment, error) {
	escrow, err := s.pickEscrow(profile, s.snapshots.Snapshot())
	if err != nil {
		return Assignment{}, err
	}

	queued := newWaiter(profile, s.now())
	for {
		target, err := s.dispatcherFor(escrow)
		if err != nil {
			return Assignment{}, err
		}
		outcome := target.submitWaiter(queued)
		target.pendingSubmits.Add(-1)
		if outcome == submitAccepted {
			break
		}
		if outcome == submitFull {
			return Assignment{}, ErrEscrowBusy
		}
		// A stopped dispatcher is replaced by the next get-or-create, and the claim above keeps the
		// replacement alive, so this retries at most once more before the registry itself is closed.
	}

	select {
	case result := <-queued.replyCh:
		if result.err != nil {
			return Assignment{}, result.err
		}
		return result.assignment, nil
	case <-ctx.Done():
		// Leaving and taking whatever was already handed over is one step: an assignment delivered in
		// this same instant holds a committed nonce and a concurrency slot nobody else will give back.
		if delivered, wasDelivered := queued.abandon(); wasDelivered && delivered.err == nil {
			s.dropAssignment(delivered.assignment, profile.Model)
		}
		return Assignment{}, ctx.Err()
	}
}

func (s *Scheduler) dropAssignment(assignment Assignment, model string) {
	s.limiter.Release(assignment.Host, model)
	if s.observer != nil {
		s.observer.GhostBurned(assignment.Escrow, ghostAbandoned.reason())
	}
}

// BlockHost bars a participant from ever serving another real request on one escrow. The block is
// permanent and is never cleared, by design: the host applied this escrow's state and produced a
// diverging post-state-root, so every later dispatch on it would build on state the two no longer
// share. It is a correctness valve rather than a performance signal, so it has no expiry, no
// eviction, and no recovery path for the process's lifetime.
func (s *Scheduler) BlockHost(escrowID, participant string) {
	s.blocksMu.Lock()
	defer s.blocksMu.Unlock()
	blocked, known := s.blockedHosts[escrowID]
	if !known {
		blocked = map[string]bool{}
		s.blockedHosts[escrowID] = blocked
	}
	blocked[participant] = true
}

// Stop shuts every escrow's actor down and is safe to call more than once.
func (s *Scheduler) Stop() {
	s.registryMu.Lock()
	s.stopped = true
	running := make([]*dispatcher, 0, len(s.dispatchers))
	for _, active := range s.dispatchers {
		running = append(running, active)
	}
	clear(s.dispatchers)
	s.registryMu.Unlock()

	for _, active := range running {
		active.stop()
	}
}

func (s *Scheduler) dispatcherFor(escrow Escrow) (*dispatcher, error) {
	s.registryMu.Lock()
	defer s.registryMu.Unlock()
	if s.stopped {
		return nil, ErrDispatcherStopped
	}

	target, known := s.dispatchers[escrow.ID]
	if !known || target.isStopped() {
		target = newDispatcher(dispatcherDeps{
			escrowID:     escrow.ID,
			session:      escrow.Session,
			snapshots:    s.snapshots,
			predicates:   s.predicates(escrow),
			acquireSlot:  s.acquireSlot(escrow),
			releaseSlot:  s.releaseSlot(escrow),
			observer:     s.observer,
			now:          s.now,
			stale:        s.holdGrace(),
			newTimer:     s.newTimer,
			retire:       s.retire,
			idleGrace:    idleDispatcherGrace,
			submitBuffer: s.submitBuffer,
		})
		s.dispatchers[escrow.ID] = target
		target.start()
	}
	// Claimed under the registry lock, so an actor deciding to retire cannot slip between this and
	// the submit that follows.
	target.pendingSubmits.Add(1)
	return target, nil
}

func (s *Scheduler) retire(idle *dispatcher) bool {
	s.registryMu.Lock()
	defer s.registryMu.Unlock()
	if idle.pendingSubmits.Load() != 0 || !idle.markStopped() {
		return false
	}
	if s.dispatchers[idle.escrowID] == idle {
		delete(s.dispatchers, idle.escrowID)
	}
	return true
}

// predicates rebuilds the host filters from their own sources on every drain; the dispatcher freezes
// the result for that drain.
func (s *Scheduler) predicates(escrow Escrow) func(chain.PhaseSnapshot) availability {
	model := escrow.Model
	stateBlocked := s.stateBlocked(escrow.ID)
	return func(snapshot chain.PhaseSnapshot) availability {
		preserved := pocPreserved(snapshot, model)
		return availability{
			pocRequired: func(participant string) bool { return !preserved(participant) },
			throttled:   func(participant string) bool { return !s.limiter.Available(participant, model) },
			capability: func(participant string, profile RequestProfile) bool {
				_, cannotServe := s.perf.CannotServe(participant, profile.RequiresTools, profile.ContextHint)
				return cannotServe
			},
			stateBlocked: stateBlocked,
		}
	}
}

func (s *Scheduler) acquireSlot(escrow Escrow) func(participant string) bool {
	model := escrow.Model
	return func(participant string) bool { return s.limiter.Acquire(participant, model) }
}

func (s *Scheduler) releaseSlot(escrow Escrow) func(participant string) {
	model := escrow.Model
	return func(participant string) { s.limiter.Release(participant, model) }
}

func (s *Scheduler) stateBlocked(escrowID string) func(string) bool {
	return func(participant string) bool {
		s.blocksMu.RLock()
		defer s.blocksMu.RUnlock()
		return s.blockedHosts[escrowID][participant]
	}
}

func (s *Scheduler) holdGrace() time.Duration {
	return time.Duration(s.settings.Load().Scheduler.HoldGraceMS) * time.Millisecond
}

// pocPreserved reads the PoC-preserved set, preferring the model's own. A nil set means the observer
// has not loaded one yet and every participant counts as preserved: reading absent data as "nobody is
// preserved" would ghost every nonce the escrow owns.
func pocPreserved(snapshot chain.PhaseSnapshot, model string) func(string) bool {
	preserved := snapshot.PreservedByModel[model]
	if preserved == nil {
		preserved = snapshot.Preserved
	}
	if preserved == nil {
		return func(string) bool { return true }
	}
	loaded := make(map[string]bool, len(preserved))
	for _, participant := range preserved {
		loaded[participant] = true
	}
	return func(participant string) bool { return loaded[participant] }
}

// RequestProfile describes one request, or one escalation attempt within the same request.
type RequestProfile struct {
	Model         string
	Escrow        string // pinned escrow for escalation; "" = pick one
	InputTokens   int
	RequiresTools bool
	ContextHint   uint64
	Exclude       []string // participant keys already raced for this request
	// Params is forwarded to session.Advance unread. The far end commits it as the escrow's inference
	// params, so it must be exactly devshard/user.InferenceParams -- not the request body it was
	// built from, which the adapter cannot commit and will reject.
	Params       any
	AffinityHint *AffinityHint
}

type Assignment struct {
	Escrow string
	Host   string
	Nonce  Prepared
}

// escrowSource is the candidate-escrow registry; api wires it over the live runtime map.
type escrowSource interface {
	// Candidates returns the escrows that could serve model, each with its Session handle, in a
	// stable order, with active/phase already filtered to accepts-new-inferences==true.
	Candidates(model string) []Escrow
}

type Escrow struct {
	ID          string
	Model       string
	Session     session
	ActiveUsers int // in-flight user requests, for the W(e) load score
}

// NonceIntent is what the scheduler tells a session to do with the nonce it is offering. The
// adapter that implements session lives outside this package, so it branches on this rather than on
// the unexported Decision; Params is RequestProfile.Params forwarded verbatim, under the same type
// requirement, and is set only for a non-ghost commit.
type NonceIntent struct {
	Commit bool
	Ghost  bool
	Params any
}

// session is the narrow view of devshard/user.Session the scheduler needs; api wires an adapter.
type session interface {
	// Advance is the one atomic nonce-peek->decide->commit unit: it computes the next candidate
	// binding, calls decide(binding), and commits the nonce (composing the real-or-ghost diff) IFF
	// the returned intent commits. A declined nonce is left untouched and yields a nil Prepared.
	Advance(decide func(HostBinding) NonceIntent) (Prepared, error)
	ParticipantKeys() []string // distinct participants (slots deduped) -- the exclusion universe
	GroupSize() int            // len(group); nonce % GroupSize == hostIdx
	LatestNonce() uint64       // for the nonce-cap gate
}

type HostBinding struct {
	Nonce       uint64
	HostIdx     int
	Participant string // participant key for HostIdx (slot-deduped)
}

// Prepared is a committed nonce ready for dispatch. *user.PreparedInference already satisfies this
// verbatim, so the api adapter needs no conversion code; a nil Prepared means the nonce was declined.
type Prepared interface {
	Nonce() uint64
	HostIdx() int
}

// snapshotSource is satisfied by *chain.PhaseObserver.
type snapshotSource interface{ Snapshot() chain.PhaseSnapshot }

// escrowWeights is satisfied by *limits.Capacity.
type escrowWeights interface {
	EscrowWeight(escrowID, model string) float64
}

// hostLimiter is satisfied by *limits.ParticipantLimiter. Acquire is the admission authority and runs
// with the commit; Available is only a cheap pre-filter, so a stale answer from it costs nothing. A
// slot handed to a caller is released by the engine that spends it, never here.
type hostLimiter interface {
	Available(participant, model string) bool
	Acquire(participant, model string) bool
	Release(participant, model string)
}

// hostCapability is satisfied by *perf.Tracker.
type hostCapability interface {
	CannotServe(participant string, requiresTools bool, contextHint uint64) (string, bool)
}
