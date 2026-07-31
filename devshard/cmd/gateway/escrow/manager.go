package escrow

import (
	"context"
	"errors"
	"log"
	"time"

	"devshard/cmd/gateway/config"
)

const escrowTickInterval = 15 * time.Second

type Deps struct {
	Tx         escrowTxClient
	Store      escrowStore
	Snapshots  snapshotSource
	Settlement SettlementSource
	Signer     SignerSource
	Config     *config.Holder
	Now        func() time.Time
}

func NewManager(d Deps) *Manager {
	return &Manager{
		tx:               d.Tx,
		store:            d.Store,
		snapshots:        d.Snapshots,
		signer:           d.Signer,
		breaker:          newCreateBreaker(),
		now:              d.Now,
		config:           d.Config,
		settlementSource: d.Settlement,
	}
}

// Start is idempotent: a call while already running is a no-op.
func (m *Manager) Start(ctx context.Context) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.stop != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := make(chan struct{})
	done := make(chan struct{})
	m.stop, m.done, m.cancel = stop, done, cancel

	go func() {
		defer cancel()
		defer close(done)
		m.runTick(ctx)
		ticker := time.NewTicker(escrowTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				m.runTick(ctx)
			}
		}
	}()
}

func (m *Manager) runTick(ctx context.Context) {
	if err := m.tick(ctx); err != nil {
		log.Printf("escrow tick: %v", err)
	}
}

// Stop is idempotent and is a barrier for every caller: done outlives stop, so a second concurrent
// Stop waits for the exit rather than returning early. Cancelling the context interrupts a tick
// already in flight — a tick can outlast the shutdown grace period, and waiting only between ticks
// would let the process close its store while a settlement write is still running.
func (m *Manager) Stop() {
	m.lifecycleMu.Lock()
	stop, done, cancel := m.stop, m.done, m.cancel
	m.stop, m.cancel = nil, nil
	m.lifecycleMu.Unlock()
	if stop != nil {
		close(stop)
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (m *Manager) tick(ctx context.Context) error {
	reconcileErr := m.reconcile(ctx) // crash recovery must not depend on the rotation toggle

	devshards, err := m.store.ListDevshards(ctx)
	if err != nil {
		return errors.Join(reconcileErr, err)
	}
	// Parked escrows must settle whatever the rotation toggle says: their row is the only record of
	// which key can settle them, so nothing else will ever pick them up.
	pendingErr := m.settlePending(ctx, devshards)
	// An escrow gone from chain must stop taking traffic whatever the rotation toggle says.
	missingErr := m.checkMissing(ctx)

	cfg := m.config.Load()
	if !cfg.Rotation.Enabled {
		return errors.Join(reconcileErr, pendingErr, missingErr)
	}
	models, err := parseModels(cfg.Rotation.ModelsJSON)
	if err != nil {
		return errors.Join(reconcileErr, pendingErr, missingErr, err)
	}

	// Pulled, not subscribed: a 15s poll is equivalent at this cadence and avoids callback races.
	snapshot := m.snapshots.Snapshot()
	if snapshot.EpochIndex == 0 || snapshot.BlockHeight == 0 {
		return errors.Join(reconcileErr, pendingErr, missingErr) // cold start, no chain data yet
	}

	var bridgeErr error
	blocksToEpochSwitch := snapshot.EpochSwitchBlockHeight - snapshot.BlockHeight
	if blocksToEpochSwitch >= 0 && blocksToEpochSwitch <= cfg.Rotation.PrePoCBlocks {
		bridgeErr = m.prepareBridge(ctx, snapshot, models, devshards) // wins even when PoC is also inactive
	} else if !snapshot.RequestsBlocked {
		bridgeErr = m.finishBridge(ctx, snapshot, models, devshards)
	}

	depletionErr := m.checkDepletion(ctx, snapshot, models, devshards)
	return errors.Join(reconcileErr, pendingErr, missingErr, bridgeErr, depletionErr)
}
