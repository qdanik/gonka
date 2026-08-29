// Package scheduler picks the escrow, host, and nonce that serve a chat-completions request.
package scheduler

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

// idleDispatcherGrace is how long an escrow's actor stays alive with an empty queue. See routing.md, "Idle dispatchers are reaped".
const idleDispatcherGrace = 5 * time.Minute

// Deps wires the runtime facts routing reads; Observer, Now, SubmitBuffer and OnEscrowExhausted are optional.
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
	OnEscrowExhausted func(escrowID, reason string)
}

// Scheduler owns one actor per escrow. See routing.md, "Picking an escrow".
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
	onEscrowExhausted func(escrowID, reason string)

	tieBreak atomic.Int64

	registryMu  sync.Mutex
	dispatchers map[string]*dispatcher
	stopped     bool

	blocksMu     sync.RWMutex
	blockedHosts map[string]map[string]bool
	replays      replayCredit
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

// Pick serves one request or one escalation attempt; an escalation reuses the pinned escrow.
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
		// A stopped dispatcher is replaced by the next get-or-create, so this retries at most once more.
	}

	select {
	case result := <-queued.replyCh:
		if result.err != nil {
			return Assignment{}, result.err
		}
		return result.assignment, nil
	case <-ctx.Done():
		// Leaving and taking are one step: an assignment delivered in this instant holds a nonce and a slot.
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
		s.observer.GhostBurned(assignment.Escrow, Burn{Nonce: assignment.Nonce.Nonce(), Participant: assignment.Host, Reason: ghostAbandoned.reason(), Prepared: assignment.Nonce})
	}
}

// HostDiverged reports whether the participant still had its catch-up replay; the last one blocks it.
func (s *Scheduler) HostDiverged(escrowID, participant string, at time.Time) bool {
	if s.replays.spend(escrowID, participant, at) {
		return true
	}
	s.BlockHost(escrowID, participant)
	return false
}

// HostServed returns the replay to a participant whose later send the group accepted.
func (s *Scheduler) HostServed(escrowID, participant string, sentAt time.Time) {
	s.replays.restore(escrowID, participant, sentAt)
}

// BlockHost bars a participant from one escrow for as long as the escrow's dispatcher lives. See routing.md.
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
			matchWait:    s.matchWait(),
			newTimer:     s.newTimer,
			retire:       s.retire,
			idleGrace:    idleDispatcherGrace,
			submitBuffer: s.submitBuffer,
			onExhausted:  s.onEscrowExhausted,
		})
		s.dispatchers[escrow.ID] = target
		target.start()
	}
	// Claimed under the registry lock, so an actor deciding to retire cannot slip in before the submit.
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
	// The block and the spent replay outlive the actor on purpose. See README, "Dispatcher lifecycle".
	return true
}

// predicates rebuilds the host filters on every drain; the dispatcher freezes the result for that drain.
func (s *Scheduler) predicates(escrow Escrow) func(chain.PhaseSnapshot) availability {
	model := escrow.Model
	stateBlocked := s.stateBlocked(escrow.ID)
	return func(snapshot chain.PhaseSnapshot) availability {
		preserved := pocPreserved(snapshot, model)
		allowed := allowedParticipants(s.settings.Load().Scheduler.ParticipantAllowlist)
		return availability{
			notAllowed:   func(participant string) bool { return !allowed(participant) },
			pocRequired:  func(participant string) bool { return !preserved(participant) },
			throttled:    func(participant string) bool { return !s.limiter.Available(participant, model) },
			ejected:      func(participant string) bool { return s.perf.Ejected(participant, model) },
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

func (s *Scheduler) matchWait() time.Duration {
	return time.Duration(s.settings.Load().Scheduler.MatchWaitMS) * time.Millisecond
}

// reserveTokens is what the request already sent plus the most this gateway will let the host answer with.
func (s *Scheduler) reserveTokens(profile RequestProfile) uint64 {
	if s.settings == nil {
		return 0
	}
	return uint64(max(profile.InputTokens, 0)) + uint64(max(s.settings.Load().Limits.MaxTokensCap, 0))
}

// pocPreserved prefers the model's own set; a nil set means not loaded yet, so everybody counts as preserved. See rules.md, "8. Fail-closed and fail-open are chosen per signal".
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

// RequestProfile is one request as routing reads it; Params must be exactly devshard/user.InferenceParams. See README, "The boundary types".
type RequestProfile struct {
	Model       string
	Escrow      string
	InputTokens int
	Exclude     []string
	Params      any
}

// Burn is a nonce the scheduler spent on nobody; Prepared is nil when the decision preceded the commit.
type Burn struct {
	Nonce       uint64
	Participant string
	Reason      string
	Prepared    Prepared
}

// Assignment is a committed nonce ready to spend. See README, "The boundary types".
type Assignment struct {
	Escrow     string
	Host       string
	Nonce      Prepared
	EscrowHold func()
}

// ReleaseEscrow gives the hold back: as soon as the caller has its own, or instead of dispatching.
func (a Assignment) ReleaseEscrow() {
	if a.EscrowHold != nil {
		a.EscrowHold()
	}
}

// escrowSource is the candidate-escrow registry; Candidates returns a stable, already-filtered order.
type escrowSource interface {
	Candidates(model string) []Escrow
}

// Escrow is one candidate; a nil Hold counts nothing. See README, "The boundary types".
type Escrow struct {
	ID          string
	Model       string
	Session     session
	ActiveUsers int
	Hold        func() (release func(), ok bool)
}

// NonceIntent is what the scheduler tells a session to do with the nonce it offers. See README, "The boundary types".
type NonceIntent struct {
	Commit bool
	Ghost  bool
	Params any
}

// session is the narrow view of devshard/user.Session the scheduler needs; Advance is the atomic peek->decide->commit unit. See README, "The boundary types".
type session interface {
	Advance(decide func(HostBinding) NonceIntent) (Prepared, error)
	ParticipantKeys() []string // distinct participants (slots deduped) -- the exclusion universe
	GroupSize() int            // len(group); nonce % GroupSize == hostIdx
	LatestNonce() uint64       // for the nonce-cap gate
	Balance() uint64           // for the balance floor
	TokenPrice() uint64        // for the balance floor
}

// HostBinding is the nonce the session is offering and the host it is bound to, deduped across a validator's slots.
type HostBinding struct {
	Nonce       uint64
	HostIdx     int
	Participant string
}

// Prepared is a committed nonce ready for dispatch; nil means the nonce was declined.
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

// hostLimiter is satisfied by *limits.ParticipantLimiter. See README, "The boundary types".
type hostLimiter interface {
	Available(participant, model string) bool
	Acquire(participant, model string) bool
	Release(participant, model string)
}

// hostHealth is satisfied by *perf.Tracker; Ejected is already capped, so honouring it cannot empty the pool.
type hostHealth interface {
	Ejected(participant, model string) bool
}

// allowedParticipants answers true for everybody when the list is empty.
func allowedParticipants(allowlist []string) func(participant string) bool {
	if len(allowlist) == 0 {
		return func(string) bool { return true }
	}
	allowed := make(map[string]bool, len(allowlist))
	for _, participant := range allowlist {
		allowed[strings.TrimSpace(participant)] = true
	}
	return func(participant string) bool { return allowed[participant] }
}
