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

// idleDispatcherGrace is how long an escrow's actor stays alive with an empty queue. See
// gateway-routing-and-nonces.md, "Idle dispatchers are reaped".
const idleDispatcherGrace = 5 * time.Minute

// Deps wires the runtime facts routing reads. Observer, Now, SubmitBuffer and OnEscrowExhausted are
// optional; the last is how the rotation lifecycle learns an escrow is out of nonces or out of deposit.
type Deps struct {
	Escrows           escrowSource
	Capacity          escrowWeights
	Limiter           hostLimiter
	Perf              hostHealth
	Snapshots         snapshotSource
	Config            *config.Holder
	Observer          dispatchObserver
	Now               func() time.Time
	SubmitBuffer      int
	OnEscrowExhausted func(escrowID string)
}

// Scheduler owns one actor per escrow. tieBreak is a single counter shared across every model and tie-set
// shape. See gateway-routing-and-nonces.md, "Picking an escrow".
type Scheduler struct {
	escrows           escrowSource
	capacity          escrowWeights
	limiter           hostLimiter
	perf              hostHealth
	snapshots         snapshotSource
	settings          *config.Holder
	observer          dispatchObserver
	now               func() time.Time
	newTimer          func(time.Duration) (<-chan time.Time, func())
	submitBuffer      int
	onEscrowExhausted func(escrowID string)

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
		escrows:           deps.Escrows,
		capacity:          deps.Capacity,
		limiter:           deps.Limiter,
		perf:              deps.Perf,
		snapshots:         deps.Snapshots,
		settings:          deps.Config,
		observer:          deps.Observer,
		now:               deps.Now,
		submitBuffer:      deps.SubmitBuffer,
		onEscrowExhausted: deps.OnEscrowExhausted,
		dispatchers:       map[string]*dispatcher{},
		blockedHosts:      map[string]map[string]bool{},
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
	assignment.ReleaseEscrow()
	if s.observer != nil {
		s.observer.GhostBurned(assignment.Escrow, ghostAbandoned.reason())
	}
}

// BlockHost permanently bars a participant from one escrow; never cleared, by design. See
// gateway-routing-and-nonces.md.
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
			holdEscrow:   escrow.Hold,
			observer:     s.observer,
			now:          s.now,
			stale:        s.holdGrace(),
			newTimer:     s.newTimer,
			retire:       s.retire,
			idleGrace:    idleDispatcherGrace,
			submitBuffer: s.submitBuffer,
			onExhausted:  s.onEscrowExhausted,
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
	if s.observer != nil {
		s.observer.EscrowRetired(idle.escrowID)
	}
	// Escrow ids are chain-monotonic and never reused, so a block list kept past retirement is one entry
	// per escrow that ever saw a divergent host, held for the life of the process.
	s.blocksMu.Lock()
	delete(s.blockedHosts, idle.escrowID)
	s.blocksMu.Unlock()
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
			ejected:     func(participant string) bool { return s.perf.Ejected(participant, model) },
			capability: func(participant string, profile RequestProfile) (string, bool) {
				return s.perf.CannotServe(participant, profile.RequiresTools, profile.ContextHint)
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

// pocPreserved reads the PoC-preserved set, preferring the model's own; a nil set means not loaded yet, so
// every participant counts as preserved. See
// gateway-invariants.md, "8. Fail-closed and fail-open are chosen per signal, and each choice is deliberate".
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

// RequestProfile is one request as routing reads it; an empty Escrow picks one, a set Escrow is an
// escalation reusing the pinned one. Params is forwarded to session.Advance unread and committed there as
// the escrow's inference params, so it must be exactly devshard/user.InferenceParams -- not the request
// body it was built from, which the adapter cannot commit and will reject.
type RequestProfile struct {
	Model         string
	Escrow        string
	InputTokens   int
	RequiresTools bool
	ContextHint   uint64
	Exclude       []string
	Params        any
}

// Assignment is a committed nonce ready to spend. EscrowHold gives back the escrow's in-flight count the
// commit took; it is idempotent, and nil when the escrow source counts nothing.
type Assignment struct {
	Escrow     string
	Host       string
	Nonce      Prepared
	EscrowHold func()
}

// ReleaseEscrow gives the hold back. A caller that has taken its own hold on the escrow calls this as
// soon as it has one; a caller that never dispatches calls it instead of dispatching.
func (a Assignment) ReleaseEscrow() {
	if a.EscrowHold != nil {
		a.EscrowHold()
	}
}

// escrowSource is the candidate-escrow registry; api wires it over the live runtime map. Candidates
// returns escrows in a stable order with active/phase already filtered to accepts-new-inferences.
type escrowSource interface {
	Candidates(model string) []Escrow
}

// Escrow is one candidate. ActiveUsers is the in-flight user request count the W(e) load score reads. Hold
// counts one in-flight request against the escrow and yields its release; it is taken with the nonce commit
// and refused once the escrow has been retired, and a nil Hold counts nothing.
type Escrow struct {
	ID          string
	Model       string
	Session     session
	ActiveUsers int
	Hold        func() (release func(), ok bool)
}

// NonceIntent is what the scheduler tells a session to do with the nonce it is offering. The
// adapter that implements session lives outside this package, so it branches on this rather than on
// Decision, whose variants are unexported; Params is RequestProfile.Params forwarded verbatim, under the same type
// requirement, and is set only for a non-ghost commit.
type NonceIntent struct {
	Commit bool
	Ghost  bool
	Params any
}

// session is the narrow view of devshard/user.Session the scheduler needs; api wires an adapter.
// Advance is the one atomic nonce-peek->decide->commit unit: it computes the next candidate binding,
// calls decide, and commits the nonce only if the returned intent says to; a declined nonce is left
// untouched and yields a nil Prepared.
type session interface {
	Advance(decide func(HostBinding) NonceIntent) (Prepared, error)
	ParticipantKeys() []string // distinct participants (slots deduped) -- the exclusion universe
	GroupSize() int            // len(group); nonce % GroupSize == hostIdx
	LatestNonce() uint64       // for the nonce-cap gate
}

// HostBinding is the nonce the session is offering and the host it is bound to; Participant is that host's
// participant key, deduped across the slots one validator holds.
type HostBinding struct {
	Nonce       uint64
	HostIdx     int
	Participant string
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

// hostHealth is satisfied by *perf.Tracker. Ejected is already capped to a fraction of the model's known
// hosts, so honouring it here can never empty the pool.
type hostHealth interface {
	CannotServe(participant string, requiresTools bool, contextHint uint64) (string, bool)
	Ejected(participant, model string) bool
}
