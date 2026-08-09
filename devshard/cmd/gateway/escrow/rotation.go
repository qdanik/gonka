package escrow

import (
	"context"
	"errors"
	"fmt"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/store"
	"devshard/logging"
)

const (
	roleTemp    = "temp"
	roleRegular = "regular"

	stagePrepareTemp   = "prepare_temp"
	stageFinishRegular = "finish_regular"
)

// errCreateSuppressed marks a create the breaker refused. It is not "nothing needed": the breaker is
// gated exactly when creation has been failing, so the bridge must take its degrade path and keep the
// escrows it has instead of retiring them for replacements that were never created.
var errCreateSuppressed = errors.New("escrow creation suppressed by the create breaker")

// ensureToTarget creates escrows up to target for (model, role, epoch). devshards is the
// snapshot the caller already loaded once this tick; it is only filtered here, never reloaded.
func (m *Manager) ensureToTarget(ctx context.Context, role string, target int, model ModelConfig, snapshot chain.PhaseSnapshot, devshards []store.DevshardRecord) (created int, err error) {
	existing := countActive(devshards, model.ModelID, role, int64(snapshot.EpochIndex))
	if existing >= target {
		return 0, nil
	}
	if served, known := servedByNetwork(snapshot, model.ModelID); known && !served {
		// Written down because it is a rotation that produced nothing on purpose: without it an
		// operator looking for the escrow that never appeared finds no reason anywhere.
		logging.Warn("rotation skipped, the network serves no such model",
			logkey.Model, model.ModelID, logkey.Role, role, logkey.Epoch, snapshot.EpochIndex)
		return 0, nil
	}
	if m.breaker.gated(model.ModelID, role) {
		return 0, errCreateSuppressed
	}

	for i := existing; i < target; i++ {
		if _, err = m.createEscrow(ctx, model, role, snapshot.EpochIndex, snapshot.BlockHeight); err != nil {
			m.breaker.recordFailure(model.ModelID, role)
			return created, err
		}
		created++
	}
	return created, nil
}

func countActive(devshards []store.DevshardRecord, modelID, role string, epoch int64) int {
	count := 0
	for _, record := range devshards {
		if record.Active && record.RotationRole == role && record.RotationEpoch == epoch && record.Model == modelID {
			count++
		}
	}
	return count
}

// prepareBridge swaps temp escrows in ahead of an epoch switch, one model at a time; a
// failure on one model is recorded and skipped, never stopping the rest.
func (m *Manager) prepareBridge(ctx context.Context, snapshot chain.PhaseSnapshot, models []ModelConfig, devshards []store.DevshardRecord) error {
	var errs []error
	for _, model := range models {
		created, err := m.ensureToTarget(ctx, roleTemp, model.TempCount, model, snapshot, devshards)
		if err != nil {
			// degrade, don't abort: relabel existing regulars as temp so this epoch still has bridge coverage.
			promoted, promoteErr := m.promoteRegularsToTemp(ctx, model, devshards)
			if promoted > 0 {
				logging.Warn("regular escrows promoted to temp", logkey.Model, model.ModelID, logkey.Epoch, snapshot.EpochIndex, logkey.Promoted, promoted)
			}
			if promoteErr != nil {
				errs = append(errs, promoteErr)
			}
			status := store.RotationStatus{Model: model.ModelID, Role: roleTemp, Stage: stagePrepareTemp, Epoch: snapshot.EpochIndex, CreateError: err.Error()}
			if saveErr := m.saveRotationStatus(ctx, status); saveErr != nil {
				errs = append(errs, saveErr)
			}
			errs = append(errs, fmt.Errorf("prepare bridge for %s: %w", model.ModelID, err))
			continue
		}

		retired, settleFailed := 0, 0
		for _, record := range devshards {
			// a "regular" is any active escrow not already in the temp role.
			if record.Active && record.RotationRole != roleTemp && record.Model == model.ModelID {
				if err := m.retire(ctx, record); err != nil {
					settleFailed++
					if !deferredRetire(err) {
						errs = append(errs, fmt.Errorf("retiring escrow %s while bridging %s: %w", record.EscrowID, model.ModelID, err))
					}
					continue
				}
				retired++
			}
		}
		if created > 0 || retired > 0 {
			logging.Info("bridge prepared", logkey.Model, model.ModelID, logkey.Epoch, snapshot.EpochIndex, logkey.Created, created, logkey.Retired, retired)
		}
		status := store.RotationStatus{Model: model.ModelID, Role: roleTemp, Stage: stagePrepareTemp, Epoch: snapshot.EpochIndex, Completed: settleFailed == 0}
		if err := m.saveRotationStatus(ctx, status); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// finishBridge swaps regulars back in once PoC is over (per-model, failure-isolated like prepareBridge).
func (m *Manager) finishBridge(ctx context.Context, snapshot chain.PhaseSnapshot, models []ModelConfig, devshards []store.DevshardRecord) error {
	var errs []error
	for _, model := range models {
		if !hasActiveTemp(devshards, model.ModelID, int64(snapshot.EpochIndex)) {
			continue // nothing bridged for this model, nothing to finish this tick.
		}

		created, err := m.ensureToTarget(ctx, roleRegular, model.TargetCount, model, snapshot, devshards)
		if err != nil {
			status := store.RotationStatus{Model: model.ModelID, Role: roleRegular, Stage: stageFinishRegular, Epoch: snapshot.EpochIndex, CreateError: err.Error()}
			if saveErr := m.saveRotationStatus(ctx, status); saveErr != nil {
				errs = append(errs, saveErr)
			}
			errs = append(errs, fmt.Errorf("finish bridge for %s: %w", model.ModelID, err))
			continue
		}

		retired, settleFailed := 0, 0
		for _, record := range devshards {
			if isActiveTemp(record, model.ModelID, int64(snapshot.EpochIndex)) {
				if err := m.retire(ctx, record); err != nil {
					settleFailed++
					if !deferredRetire(err) {
						errs = append(errs, fmt.Errorf("retiring escrow %s while bridging %s: %w", record.EscrowID, model.ModelID, err))
					}
					continue
				}
				retired++
			}
		}
		if created > 0 || retired > 0 {
			logging.Info("bridge finished", logkey.Model, model.ModelID, logkey.Epoch, snapshot.EpochIndex, logkey.Created, created, logkey.Retired, retired)
		}
		status := store.RotationStatus{Model: model.ModelID, Role: roleRegular, Stage: stageFinishRegular, Epoch: snapshot.EpochIndex, Completed: settleFailed == 0}
		if err := m.saveRotationStatus(ctx, status); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// isActiveTemp selects the rows finishBridge retires; hasActiveTemp is the same predicate folded
// over the set, so the gate and the retirement can never disagree about which rows are bridged.
func isActiveTemp(record store.DevshardRecord, modelID string, epoch int64) bool {
	return record.Active && record.RotationRole == roleTemp && record.Model == modelID && record.RotationEpoch <= epoch
}

func hasActiveTemp(devshards []store.DevshardRecord, modelID string, epoch int64) bool {
	for _, record := range devshards {
		if isActiveTemp(record, modelID, epoch) {
			return true
		}
	}
	return false
}

// promoteRegularsToTemp is the prepareBridge degrade path: it relabels every active regular
// escrow for the model to temp, in place. It keeps going past a write failure to promote as
// many as it can, surfacing only the first error.
// deferredRetire reports the outcomes that mean "not yet" rather than "failed": a draining escrow and a
// settlement already running are both retried by the next tick, so surfacing them would make an error of
// every ordinary rotation. Anything else is a real failure and must reach the tick.
func deferredRetire(err error) bool {
	return errors.Is(err, ErrDevshardBusy) || errors.Is(err, ErrSettlementInFlight)
}

func (m *Manager) promoteRegularsToTemp(ctx context.Context, model ModelConfig, devshards []store.DevshardRecord) (promoted int, err error) {
	for _, record := range devshards {
		if !record.Active || record.RotationRole == roleTemp || record.Model != model.ModelID {
			continue
		}
		record.RotationRole = roleTemp
		if writeErr := m.store.WithRetry(ctx, func() error { return m.store.UpsertDevshard(ctx, record) }); writeErr != nil {
			if err == nil {
				err = fmt.Errorf("promoting escrow %s to temp: %w", record.EscrowID, writeErr)
			}
			continue
		}
		promoted++
	}
	return promoted, err
}

func (m *Manager) saveRotationStatus(ctx context.Context, s store.RotationStatus) error {
	s.UpdatedAt = m.now()
	return m.store.WithRetry(ctx, func() error { return m.store.SaveRotationStatus(ctx, s) })
}
