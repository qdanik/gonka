package escrow

import (
	"context"
	"errors"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

const escrowTickInterval = 15 * time.Second

type Deps struct {
	Tx          escrowTxClient
	Store       escrowStore
	Snapshots   snapshotSource
	Settlement  SettlementSource
	Timeouts    TimeoutSweeper
	Sweeps      SweepRecorder
	Signer      SignerSource
	Config      *config.Holder
	Now         func() time.Time
	RoutePrefix string
}

// NewManager refuses a dependency set it cannot run a tick with, rather than panicking on the first one.
func NewManager(d Deps) (*Manager, error) {
	switch {
	case d.Config == nil:
		return nil, errors.New("escrow: Config is required")
	case d.Tx == nil:
		return nil, errors.New("escrow: Tx is required")
	case d.Store == nil:
		return nil, errors.New("escrow: Store is required")
	case d.Snapshots == nil:
		return nil, errors.New("escrow: Snapshots is required")
	case d.Signer == nil:
		return nil, errors.New("escrow: Signer is required")
	case d.Now == nil:
		return nil, errors.New("escrow: Now is required")
	}
	return &Manager{
		tx:               d.Tx,
		store:            d.Store,
		snapshots:        d.Snapshots,
		signer:           d.Signer,
		breaker:          newCreateBreaker(),
		now:              d.Now,
		config:           d.Config,
		settlementSource: d.Settlement,
		timeoutSweeper:   d.Timeouts,
		sweepRecorder:    d.Sweeps,
		routePrefix:      d.RoutePrefix,
	}, nil
}

// Start is idempotent: a call while already running is a no-op.
func (m *Manager) Start(ctx context.Context) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.done, m.cancel = done, cancel

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
			case <-ticker.C:
				m.runTick(ctx)
			}
		}
	}()
}

func (m *Manager) runTick(ctx context.Context) {
	if err := m.tick(ctx); err != nil {
		logging.Error("escrow tick failed", logkey.Error, err)
	}
}

// Stop is idempotent and a barrier for every caller. See escrows.md, "The tick".
func (m *Manager) Stop() {
	m.lifecycleMu.Lock()
	done, cancel := m.done, m.cancel
	m.cancel = nil
	m.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	m.sweepWork.Wait()
}

// sweepTimeouts runs off the tick rather than on it: one vote round can outlast the tick interval, and
// a second sweep over the same escrows would double the load the budget exists to bound.
func (m *Manager) sweepTimeouts(ctx context.Context) {
	settings := m.config.Load().TimeoutSweep
	budget := int(settings.BudgetPerTick)
	if m.timeoutSweeper == nil || budget <= 0 {
		return
	}
	if !m.sweeping.CompareAndSwap(false, true) {
		return
	}
	m.sweepWork.Go(func() {
		defer m.sweeping.Store(false)
		grace := time.Duration(settings.GraceSeconds) * time.Second
		due, applied, failed := m.timeoutSweeper.SweepExecutionTimeouts(ctx, grace, budget)
		if m.sweepRecorder != nil {
			m.sweepRecorder.RecordSweep(due, applied, failed)
		}
		if due == 0 {
			return
		}
		logging.Info("execution timeouts swept",
			logkey.SweptDue, due, logkey.SweptApplied, applied, logkey.SweptFailed, failed)
	})
}

func (m *Manager) tick(ctx context.Context) error {
	reconcileErr := m.reconcile(ctx) // crash recovery must not depend on the rotation toggle

	devshards, err := m.store.ListDevshards(ctx)
	if err != nil {
		return errors.Join(reconcileErr, err)
	}
	// Parked escrows must settle whatever the rotation toggle says: nothing else will ever pick them up.
	pendingErr := m.settlePending(ctx, devshards)
	// An escrow gone from chain must stop taking traffic whatever the rotation toggle says.
	missingErr := m.checkMissing(ctx)
	m.sweepTimeouts(ctx)

	cfg := m.config.Load()
	// Pulled, not subscribed: a 15s poll is equivalent at this cadence and avoids callback races.
	snapshot := m.snapshots.Snapshot()
	models, modelsErr := rotationModels(cfg.Rotation)
	// An exhausted escrow must stop taking traffic whatever the toggle says; models is empty unless rotation can supply a replacement.
	depletionErr := m.checkDepletion(ctx, snapshot, models, devshards)
	lifecycleErr := errors.Join(reconcileErr, pendingErr, missingErr, modelsErr, depletionErr)

	if !cfg.Rotation.Enabled || modelsErr != nil {
		return lifecycleErr
	}
	if snapshot.EpochIndex == 0 || snapshot.BlockHeight == 0 {
		return lifecycleErr // cold start, no chain data yet
	}

	var bridgeErr error
	blocksToEpochSwitch := snapshot.EpochSwitchBlockHeight - snapshot.BlockHeight
	if blocksToEpochSwitch >= 0 && blocksToEpochSwitch <= cfg.Rotation.PrePoCBlocks {
		bridgeErr = m.prepareBridge(ctx, snapshot, models, devshards) // wins even when PoC is also inactive
	} else if !snapshot.RequestsBlocked {
		bridgeErr = m.finishBridge(ctx, snapshot, models, devshards)
	}
	return errors.Join(lifecycleErr, bridgeErr)
}

// rotationModels is empty when rotation is off, so no caller downstream has to re-read the toggle.
func rotationModels(rotation config.Rotation) ([]ModelConfig, error) {
	if !rotation.Enabled {
		return nil, nil
	}
	return parseModels(rotation.ModelsJSON)
}
