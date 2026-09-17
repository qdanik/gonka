// Package scheduler picks the escrow, host, and nonce that serve a chat-completions request.
package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/limits"
	"devshard/types"
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

// NewScheduler refuses a dependency set it cannot route with, rather than panicking on the first request.
func NewScheduler(deps Deps) (*Scheduler, error) {
	switch {
	case deps.Config == nil:
		return nil, errors.New("scheduler: Config is required")
	case deps.Escrows == nil:
		return nil, errors.New("scheduler: Escrows is required")
	case deps.Capacity == nil:
		return nil, errors.New("scheduler: Capacity is required")
	case deps.Limiter == nil:
		return nil, errors.New("scheduler: Limiter is required")
	case deps.Perf == nil:
		return nil, errors.New("scheduler: Perf is required")
	case deps.Snapshots == nil:
		return nil, errors.New("scheduler: Snapshots is required")
	}
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
	}, nil
}

// Pick serves one request or one escalation attempt; an escalation reuses the pinned escrow. See routing.md, "One re-pick when an escrow gives up".
func (s *Scheduler) Pick(ctx context.Context, profile RequestProfile) (Assignment, error) {
	assignment, routedTo, err := s.pickOnce(ctx, profile, "")
	if profile.Escrow != "" || !errors.Is(err, ErrHostsBusy) {
		return assignment, err
	}

	if ctx.Err() != nil {
		return Assignment{}, err
	}

	retried, _, retryErr := s.pickOnce(ctx, profile, routedTo)
	if retryErr == nil || outranksBusy(retryErr) {
		return retried, retryErr
	}
	return Assignment{}, err
}

// outranksBusy holds for the answers a second round may return in place of a busy shard's. See routing.md, "One re-pick when an escrow gives up".
func outranksBusy(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, types.ErrInsufficientBalance)
}

// pickOnce names the escrow it routed to, so a caller that re-picks can leave that one out of the second round.
func (s *Scheduler) pickOnce(ctx context.Context, profile RequestProfile, avoidEscrowID string) (Assignment, string, error) {
	queued := newWaiter(profile, s.now())
	escrow, err := s.pickEscrow(profile, s.snapshots.Snapshot(), queued, avoidEscrowID)
	if err != nil {
		return Assignment{}, "", err
	}

	claimed, err := s.claimAndSubmit(escrow, queued)
	if err != nil {
		return Assignment{}, escrow.ID, err
	}
	// Held until Pick returns, so the reaper cannot forget an escrow this caller may still burn a nonce on. See README, "Dispatcher lifecycle".
	defer claimed.pendingSubmits.Add(-1)

	select {
	case result := <-queued.replyCh:
		if result.err != nil {
			return Assignment{}, escrow.ID, result.err
		}
		return result.assignment, escrow.ID, nil
	case <-ctx.Done():
		// Leaving and taking are one step: an assignment delivered in this instant holds a nonce and a slot.
		if delivered, wasDelivered := queued.abandon(); wasDelivered && delivered.err == nil {
			s.dropAssignment(delivered.assignment, profile)
		}
		return Assignment{}, escrow.ID, ctx.Err()
	}
}

// queuedAhead estimates how far each candidate's cursor will have moved before this request draws a nonce. See routing.md, "Pricing an escrow by the burns it will cost".
func (s *Scheduler) queuedAhead(candidates []Escrow) []uint64 {
	ahead := make([]uint64, len(candidates))
	s.registryMu.Lock()
	defer s.registryMu.Unlock()
	for index, candidate := range candidates {
		if running := s.dispatchers[candidate.ID]; running != nil && running.sessionID == candidate.SessionID {
			ahead[index] = uint64(max(running.pendingSubmits.Load(), 0))
		}
	}
	return ahead
}

// claimAndSubmit returns the dispatcher that accepted the waiter, still claimed; a stopped one is replaced by the next get-or-create, so this retries at most once more.
func (s *Scheduler) claimAndSubmit(escrow Escrow, queued *waiter) (*dispatcher, error) {
	for {
		target, err := s.dispatcherFor(escrow)
		if err != nil {
			return nil, err
		}
		outcome := target.submitWaiter(queued)
		if outcome == submitAccepted {
			return target, nil
		}
		target.pendingSubmits.Add(-1)
		if outcome == submitFull {
			return nil, ErrEscrowBusy
		}
	}
}

func (s *Scheduler) dropAssignment(assignment Assignment, profile RequestProfile) {
	assignment.ReleaseHostSlot()
	assignment.ReleaseEscrow()
	if s.observer != nil {
		s.observer.GhostBurned(assignment.Escrow, Burn{
			Nonce: assignment.Nonce.Nonce(), Participant: assignment.Host,
			Reason: ghostAbandoned.reason(), RequestID: profile.RequestID,
		})
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
	blocked := s.blockedHosts[escrowID]
	if blocked == nil {
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

	// A reopened escrow needs its own dispatcher: the old one still holds the retired session.
	target, known := s.dispatchers[escrow.ID]
	if !known || target.isStopped() || target.sessionID != escrow.SessionID {
		target = newDispatcher(dispatcherDeps{
			escrowID:            escrow.ID,
			sessionID:           escrow.SessionID,
			session:             escrow.Session,
			snapshots:           s.snapshots,
			predicates:          s.predicates(escrow),
			acquireSlot:         s.acquireSlot(escrow),
			holdEscrow:          escrow.Hold,
			observer:            s.observer,
			now:                 s.now,
			matchWait:           s.matchWait(),
			maxConsecutiveBurns: s.maxConsecutiveBurns,
			retirementReserve:   s.retirementReserve,
			newTimer:            s.newTimer,
			retire:              s.retire,
			idleGrace:           idleDispatcherGrace,
			submitBuffer:        s.submitBuffer,
			onExhausted:         s.onEscrowExhausted,
		})
		s.dispatchers[escrow.ID] = target
		target.start()
	}
	// Claimed under the registry lock and released when Pick returns, so an actor cannot retire while its caller holds a waiter or an assignment.
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
	model, escrowID := escrow.Model, escrow.ID
	return func(snapshot chain.PhaseSnapshot) availability {
		return s.fleetGates(model, snapshot).forEscrow(s.stateBlocked(escrowID))
	}
}

// fleetGates is the part of the ladder that depends on the model alone. See routing.md, "Pricing an escrow by the burns it will cost".
func (s *Scheduler) fleetGates(model string, snapshot chain.PhaseSnapshot) availability {
	preserved := pocPreserved(snapshot, model)
	return availability{
		notAllowed:  refusedByAllowlist(s.participantAllowlist()),
		pocRequired: func(participant string) bool { return preserved != nil && !preserved[participant] },
		congested:   func(participant string) blockReason { return blockForAdmission(s.limiter.Admits(participant, model)) },
		ejected:     func(participant string) bool { return s.perf.Ejected(participant, model) },
	}
}

// blockForAdmission maps the limiter's verdict onto the drain's ladder, so only a full window is ever forced through. See routing.md, "The forced send".
func blockForAdmission(admission limits.Admission) blockReason {
	switch admission {
	case limits.AdmissionWindowFull:
		return blockWindowFull
	case limits.AdmissionCutOff:
		return blockCutOff
	}
	return blockNone
}

func (s *Scheduler) acquireSlot(escrow Escrow) func(participant string, cost limits.TokenCost, overFullWindow bool) (func(), limits.Admission) {
	model := escrow.Model
	return func(participant string, cost limits.TokenCost, overFullWindow bool) (func(), limits.Admission) {
		if overFullWindow {
			return s.limiter.Overdraft(participant, model, cost)
		}
		return s.limiter.Acquire(participant, model, cost)
	}
}

// slotCost prices one request against a host's congestion windows: the input it must prefill, the output it may produce.
func slotCost(profile RequestProfile) limits.TokenCost {
	return limits.TokenCost{
		Input:  int64(max(profile.InputTokens, 0)),
		Output: int64(max(profile.OutputTokens, 0)),
	}
}

// stateBlocked reads the blocks live: the drain asks once per participant, and a block that lands while it runs must reach the hosts it has not offered yet.
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

// maxConsecutiveBurns is read per drain rather than per dispatcher, so an admin change reaches an escrow already running. See routing.md, "The forced send".
func (s *Scheduler) maxConsecutiveBurns() int64 {
	return s.settings.Load().Scheduler.MaxConsecutiveBurns
}

// requestReserve prices this one request the way the chain will charge it. See capacity.md, "The balance floor".
func requestReserve(profile RequestProfile) uint64 {
	return uint64(max(profile.InputBytes, 0)) + uint64(max(profile.OutputTokens, 0))
}

// retirementReserve prices one capped answer and reads nothing from the arriving request. See capacity.md, "The balance floor".
func (s *Scheduler) retirementReserve() uint64 {
	if s.settings == nil {
		return 0
	}
	return uint64(max(s.settings.Load().Limits.MaxTokensCap, 0))
}

// pocPreserved prefers the model's own set; a nil set means not loaded yet, so everybody counts as preserved. See rules.md, "8. Fail-closed and fail-open are chosen per signal".
func pocPreserved(snapshot chain.PhaseSnapshot, model string) map[string]bool {
	preserved := snapshot.PreservedByModel[model]
	if preserved == nil {
		preserved = snapshot.Preserved
	}
	if preserved == nil {
		return nil
	}
	loaded := make(map[string]bool, len(preserved))
	for _, participant := range preserved {
		loaded[participant] = true
	}
	return loaded
}

// RequestProfile is one request as routing reads it; RequestID only names it on the burns it causes, and Params must be exactly devshard/user.InferenceParams. See README, "The boundary types".
type RequestProfile struct {
	RequestID    string
	Model        string
	Escrow       string
	InputTokens  int
	InputBytes   int
	OutputTokens int
	Exclude      []string
	Params       any
}

// Burn is a committed nonce the scheduler spent on nobody, and the request it was spent during.
type Burn struct {
	Nonce       uint64
	Participant string
	Reason      string
	RequestID   string
}

// Assignment is a committed nonce ready to spend. See README, "The boundary types".
type Assignment struct {
	Escrow     string
	Host       string
	Nonce      Prepared
	EscrowHold func()
	HostSlot   func()
}

// ReleaseEscrow gives the hold back: as soon as the caller has its own, or instead of dispatching.
func (a Assignment) ReleaseEscrow() {
	if a.EscrowHold != nil {
		a.EscrowHold()
	}
}

// ReleaseHostSlot gives the host's congestion tokens back: when the attempt ends, or instead of dispatching.
func (a Assignment) ReleaseHostSlot() {
	if a.HostSlot != nil {
		a.HostSlot()
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
	SessionID   uint64
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
	ParticipantKeys() []string  // distinct participants (slots deduped) -- the exclusion universe
	SlotParticipants() []string // one per slot, duplicates kept, read-only; nonce % GroupSize indexes it
	GroupSize() int             // len(group); nonce % GroupSize == hostIdx
	LatestNonce() uint64        // for the nonce-cap gate and the burn forecast
	Balance() uint64            // for the balance floor
	TokenPrice() uint64         // for the balance floor
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
	Admits(participant, model string) limits.Admission
	Acquire(participant, model string, cost limits.TokenCost) (func(), limits.Admission)
	Overdraft(participant, model string, cost limits.TokenCost) (func(), limits.Admission)
}

// hostHealth is satisfied by *perf.Tracker; Ejected is already capped, so honouring it cannot empty the pool.
type hostHealth interface {
	Ejected(participant, model string) bool
}

// refusedByAllowlist stays nil when nobody narrowed routing, so the drain reads "no allowlist" as no rung at all.
func refusedByAllowlist(allowlist []string) func(participant string) bool {
	if len(allowlist) == 0 {
		return nil
	}
	allowed := allowedParticipants(allowlist)
	return func(participant string) bool { return !allowed(participant) }
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
