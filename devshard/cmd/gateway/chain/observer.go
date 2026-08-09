package chain

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

const (
	defaultObserverPollInterval = 5 * time.Second
	// versionsTTLPollMultiplier lets versions entries survive a couple of missed polls without
	// outliving the poller's own cadence.
	versionsTTLPollMultiplier = 3

	epochInfoPath    = "/v1/epochs/latest"
	participantsPath = "/v1/epochs/current/participants"
)

// ObserverConfig configures a PhaseObserver. Zero-value poll/client/clock fields take package
// defaults; PublicAPIBaseURL is required. Chain serves the preserved-nodes snapshot during PoC and the
// nonce ceiling; when nil both are skipped and the legacy preservation rule applies.
type ObserverConfig struct {
	PublicAPIBaseURL string
	Chain            Reader
	PollInterval     time.Duration
	HTTPClient       *http.Client
	Now              func() time.Time
}

// PhaseObserver polls chain phase and participant state, derives an immutable PhaseSnapshot, and
// publishes it to subscribers; it folds raw inputs only. See gateway-escrow-lifecycle.md, "What the
// chain observer provides".
type PhaseObserver struct {
	publicAPIBaseURL string
	chain            Reader
	client           *http.Client
	pollInterval     time.Duration
	now              func() time.Time
	versions         *VersionsCache

	current atomic.Pointer[PhaseSnapshot]
	health  snapshotHealth

	mu          sync.Mutex
	subscribers map[int]func(PhaseSnapshot)
	nextID      int

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	doneCh      chan struct{}
}

// NewPhaseObserver validates cfg and applies defaults for unset fields; it errors only when
// PublicAPIBaseURL is blank.
func NewPhaseObserver(cfg ObserverConfig) (*PhaseObserver, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.PublicAPIBaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("public API base URL is required")
	}
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultObserverPollInterval
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	observer := &PhaseObserver{
		publicAPIBaseURL: baseURL,
		chain:            cfg.Chain,
		client:           client,
		pollInterval:     pollInterval,
		now:              now,
		versions:         NewVersionsCache(client, versionsTTLPollMultiplier*pollInterval, now),
		subscribers:      make(map[int]func(PhaseSnapshot)),
	}
	observer.current.Store(&PhaseSnapshot{})
	return observer, nil
}

// Start spawns the poll loop and the versions poller on a context derived from ctx; doneCh closes
// once both have exited. It returns immediately, and is idempotent: a call while already running is
// a no-op rather than a second set of pollers.
func (o *PhaseObserver) Start(ctx context.Context) {
	o.lifecycleMu.Lock()
	defer o.lifecycleMu.Unlock()
	if o.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	o.cancel, o.doneCh = cancel, done

	var running sync.WaitGroup
	running.Add(2)
	go func() {
		defer running.Done()
		o.run(ctx)
	}()
	go func() {
		defer running.Done()
		o.versions.Run(ctx, o.pollInterval)
	}()
	go func() {
		running.Wait()
		close(done)
	}()
}

// Stop cancels the context Start derived and blocks until both loops have exited; it is a no-op
// before Start. It is a barrier for every caller: doneCh outlives cancel, so a second concurrent
// Stop waits for the pollers to exit instead of returning while they still run.
func (o *PhaseObserver) Stop() {
	o.lifecycleMu.Lock()
	cancel, done := o.cancel, o.doneCh
	o.cancel = nil
	o.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (o *PhaseObserver) run(ctx context.Context) {
	o.refresh(ctx)
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.refresh(ctx)
		}
	}
}

// refresh runs one poll and publishes a snapshot; a fetch error republishes the previous fields with
// LastError set. See gateway-escrow-lifecycle.md, "What the chain observer provides".
func (o *PhaseObserver) refresh(ctx context.Context) {
	previous := o.Snapshot()

	epoch, err := o.fetchEpochInfo(ctx)
	if err != nil {
		previous.LastError = fmt.Sprintf("fetch epoch info: %v", err)
		o.publish(previous)
		return
	}

	blocked, reason := rawPoCBlockingState(epoch.Phase, epoch.ConfirmationPoCPhase)
	snapshot := PhaseSnapshot{
		BlockHeight:            epoch.BlockHeight,
		EpochSwitchBlockHeight: epoch.EpochSwitchBlockHeight,
		EpochIndex:             epoch.EpochIndex,
		EpochPhase:             epoch.Phase,
		ConfirmationPoCPhase:   epoch.ConfirmationPoCPhase,
		RequestsBlocked:        blocked,
		BlockReason:            reason,
		LastUpdatedAt:          o.now(),
	}

	if maxNonce, fetched, maxNonceErr := o.fetchMaxNonce(ctx); fetched {
		snapshot.MaxNonce = maxNonce
	} else {
		snapshot.MaxNonce = previous.MaxNonce
		if maxNonceErr != nil {
			snapshot.LastError = joinSnapshotError(snapshot.LastError, fmt.Sprintf("fetch devshard escrow params: %v", maxNonceErr))
		}
	}

	participantsBody, err := o.getBody(ctx, participantsPath)
	if err != nil {
		o.publishWithPreviousParticipants(snapshot, previous, fmt.Sprintf("fetch participants: %v", err))
		return
	}

	preservation, preservedNodes, preservationErr := o.resolvePreservation(ctx, epoch, blocked, reason)
	if preservationErr != nil {
		// Non-fatal: the preserved-set view falls back to the legacy rule for this poll.
		snapshot.LastError = joinSnapshotError(snapshot.LastError, fmt.Sprintf("fetch preserved snapshot: %v", preservationErr))
	}

	participants, err := parseParticipants(participantsBody, preservation, preservedNodes)
	if err != nil {
		o.publishWithPreviousParticipants(snapshot, previous, fmt.Sprintf("fetch participants: %v", err))
		return
	}

	o.versions.SetCandidates(participants.InferenceURLs)

	preserved := participants.Preserved
	preservedByModel := participants.PreservedByModel
	currentWeights := participants.Weights
	currentWeightsByModel := participants.WeightsByModel
	if rawPoCValidationState(epoch.Phase, epoch.ConfirmationPoCPhase) {
		// During PoC validation, excluded miners with validation-inference-capable nodes rejoin
		// the preserved/current views with those nodes' weight. Capability is fail-closed, so a
		// cold versions cache keeps the merge conservative.
		preserved, preservedByModel, currentWeights, currentWeightsByModel =
			mergePreservedWithValidationCapable(participants, o.versions.IsNodeValidationCapable)
	}

	snapshot.CurrentWeights = currentWeights
	snapshot.FullWeights = participants.FullWeights
	snapshot.CurrentWeightsByModel = currentWeightsByModel
	snapshot.FullWeightsByModel = participants.FullWeightsByModel
	snapshot.Preserved = preserved
	snapshot.PreservedByModel = preservedByModel
	snapshot.InferenceURLs = participants.InferenceURLs

	o.publish(snapshot)
}

func (o *PhaseObserver) publishWithPreviousParticipants(snapshot, previous PhaseSnapshot, lastError string) {
	snapshot.CurrentWeights = previous.CurrentWeights
	snapshot.FullWeights = previous.FullWeights
	snapshot.CurrentWeightsByModel = previous.CurrentWeightsByModel
	snapshot.FullWeightsByModel = previous.FullWeightsByModel
	snapshot.Preserved = previous.Preserved
	snapshot.PreservedByModel = previous.PreservedByModel
	snapshot.InferenceURLs = previous.InferenceURLs
	snapshot.LastError = joinSnapshotError(snapshot.LastError, lastError)
	o.publish(snapshot)
}

// resolvePreservation picks the node-preservation rule: all nodes outside PoC; a current
// preserved-nodes snapshot during PoC, allow-all in the grace period, else the legacy rule.
func (o *PhaseObserver) resolvePreservation(ctx context.Context, epoch epochInfo, blocked bool, reason BlockReason) (preservationMode, preservedSnapshotState, error) {
	if !blocked && !epoch.IsConfirmationPoCActive {
		return preservationModeAll, preservedSnapshotState{}, nil
	}
	preservedNodes, status, err := o.fetchPreservedSnapshot(ctx, preservedSnapshotAnchor(epoch, reason))
	switch {
	case status == preservedSnapshotCurrent:
		return preservationModeSnapshot, preservedNodes, nil
	case status == preservedSnapshotMissingCurrent && allowAllParticipantsUntilSnapshot(epoch, reason):
		return preservationModeAll, preservedSnapshotState{}, nil
	default:
		return preservationModeLegacy, preservedSnapshotState{}, err
	}
}

// preservedSnapshotAnchor is the episode anchor height a preserved snapshot must match to count
// as current: the confirmation-PoC trigger height, or the epoch PoC start height; 0 skips the
// anchor check.
func preservedSnapshotAnchor(epoch epochInfo, reason BlockReason) int64 {
	switch reason {
	case BlockReasonConfirmationPoC:
		return epoch.ConfirmationPoCTriggerHeight
	case BlockReasonPoC:
		return epoch.PoCStartBlockHeight
	}
	if epoch.IsConfirmationPoCActive {
		return epoch.ConfirmationPoCTriggerHeight
	}
	return 0
}

// allowAllParticipantsUntilSnapshot reports the confirmation-PoC grace period, when the matching
// preserved snapshot intentionally does not exist yet.
func allowAllParticipantsUntilSnapshot(epoch epochInfo, reason BlockReason) bool {
	return reason == BlockReasonConfirmationPoC && epoch.ConfirmationPoCPhase == ConfirmationPoCGracePeriod
}

// fetchPreservedSnapshot reads the chain's preserved-nodes snapshot. A chain that answers no snapshot
// for the current episode is a different thing from one that could not be reached, and only the second
// is an error: the first makes routing fall back to the epoch group.
func (o *PhaseObserver) fetchPreservedSnapshot(ctx context.Context, expectedAnchor int64) (preservedSnapshotState, preservedSnapshotStatus, error) {
	if o.chain == nil {
		return preservedSnapshotState{}, preservedSnapshotUnavailable, nil
	}
	snapshot, found, err := o.chain.PreservedNodes(ctx)
	if err != nil {
		return preservedSnapshotState{}, preservedSnapshotUnavailable, err
	}
	if !found {
		return preservedSnapshotState{}, preservedSnapshotMissingCurrent, nil
	}
	if expectedAnchor > 0 && snapshot.EpisodeAnchorHeight != expectedAnchor {
		return preservedSnapshotState{}, preservedSnapshotMissingCurrent, nil
	}
	return newPreservedSnapshotState(snapshot), preservedSnapshotCurrent, nil
}

// fetchMaxNonce reads the chain's devshard escrow nonce ceiling. fetched=false with a nil error means
// the chain carries no devshard escrow params at all.
func (o *PhaseObserver) fetchMaxNonce(ctx context.Context) (maxNonce uint64, fetched bool, err error) {
	if o.chain == nil {
		return 0, false, nil
	}
	return o.chain.MaxNonce(ctx)
}

func (o *PhaseObserver) fetchEpochInfo(ctx context.Context) (epochInfo, error) {
	body, err := o.getBody(ctx, epochInfoPath)
	if err != nil {
		return epochInfo{}, err
	}
	return parseEpochInfo(body)
}

func (o *PhaseObserver) getBody(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.publicAPIBaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("%s status %d", path, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (o *PhaseObserver) Snapshot() PhaseSnapshot {
	return *o.current.Load()
}

// Subscribe registers a callback invoked synchronously on every publish, mirroring
// config.Holder's semantics (store-before-notify, cancel via map delete). The returned cancel
// removes the subscription.
func (o *PhaseObserver) Subscribe(cb func(PhaseSnapshot)) (cancel func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	id := o.nextID
	o.nextID++
	o.subscribers[id] = cb
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		delete(o.subscribers, id)
	}
}

// snapshotHealth remembers the last publish's health so a five-second poll speaks only on the turn.
type snapshotHealth struct {
	mu       sync.Mutex
	degraded bool
}

// healthChange is what one publish is worth saying about, separated from saying it so the decision can
// be tested: the whole value of this type is writing nothing while a failure persists.
type healthChange struct {
	degraded  bool
	recovered bool
}

func (h *snapshotHealth) advance(lastError string) healthChange {
	h.mu.Lock()
	defer h.mu.Unlock()
	failed := lastError != ""
	change := healthChange{degraded: failed && !h.degraded, recovered: !failed && h.degraded}
	h.degraded = failed
	return change
}

// joinSnapshotError keeps every failed read of one poll. Overwriting left the operator reading the last
// failure only, so a frozen MaxNonce -- the ceiling nonces are issued against -- went unnamed.
func joinSnapshotError(existing, added string) string {
	switch {
	case added == "":
		return existing
	case existing == "":
		return added
	}
	return existing + "; " + added
}

// narrateHealth carries the cause the health gauge cannot: LastError names which of four reads failed.
func (o *PhaseObserver) narrateHealth(snapshot PhaseSnapshot) {
	change := o.health.advance(snapshot.LastError)
	switch {
	case change.degraded:
		logging.Warn("chain snapshot stale", logkey.Error, snapshot.LastError,
			logkey.Epoch, snapshot.EpochIndex, logkey.Height, snapshot.BlockHeight)
	case change.recovered:
		logging.Info("chain snapshot recovered", logkey.Epoch, snapshot.EpochIndex, logkey.Height, snapshot.BlockHeight)
	}
}

// publish stores snapshot then synchronously notifies the current subscribers, in no particular
// order.
func (o *PhaseObserver) publish(snapshot PhaseSnapshot) {
	o.current.Store(&snapshot)
	o.narrateHealth(snapshot)
	o.mu.Lock()
	callbacks := make([]func(PhaseSnapshot), 0, len(o.subscribers))
	for _, cb := range o.subscribers {
		callbacks = append(callbacks, cb)
	}
	o.mu.Unlock()
	for _, cb := range callbacks {
		cb(snapshot)
	}
}
