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

// OnBalanceExhausted marks an escrow for replacement; the work happens in the next tick, so this
// hook does no I/O and never fans out a per-escrow chain call.
func (m *Manager) OnBalanceExhausted(escrowID, reason string) {
	if m.depleted.mark(escrowID) {
		logging.Warn("escrow left routing", logkey.Escrow, escrowID, logkey.Reason, reason)
	}
}

func (m *Manager) checkDepletion(ctx context.Context, snapshot chain.PhaseSnapshot, models []ModelConfig, devshards []store.DevshardRecord) error {
	marked := m.depleted.drain()
	if len(marked) == 0 {
		return nil
	}
	modelByID := make(map[string]ModelConfig, len(models))
	for _, model := range models {
		modelByID[model.ModelID] = model
	}

	var errs []error
	for _, record := range devshards {
		if !record.Active || !marked[record.EscrowID] {
			continue
		}
		if err := m.retireDepleted(ctx, record, modelByID, snapshot); err != nil {
			m.OnBalanceExhausted(record.EscrowID, "retire_failed")
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// retireDepleted stops routing to an exhausted escrow, whose low in-flight count is exactly what makes
// the load score prefer it while it fails every request. A replacement is created first where one is
// configured, so coverage never drops; where none is, the escrow is retired anyway.
func (m *Manager) retireDepleted(ctx context.Context, record store.DevshardRecord, modelByID map[string]ModelConfig, snapshot chain.PhaseSnapshot) error {
	model, replaceable := modelByID[record.Model]
	if replaceable {
		return m.replaceDepleted(ctx, record, model, snapshot)
	}
	logging.Warn("escrow depleted with no replacement configured", logkey.Escrow, record.EscrowID, logkey.Model, record.Model)
	if err := m.retire(ctx, record); err != nil {
		return fmt.Errorf("retiring depleted escrow %s: %w", record.EscrowID, err)
	}
	return nil
}

// replaceDepleted creates the replacement before retiring the depleted escrow, so coverage never
// drops to zero; a failed create leaves the depleted escrow in place for the next attempt. The
// replacement is always regular: inheriting a temp role would hand the next bridge an escrow to retire
// rather than the lasting coverage the depleted one was providing.
func (m *Manager) replaceDepleted(ctx context.Context, record store.DevshardRecord, model ModelConfig, snapshot chain.PhaseSnapshot) error {
	if _, err := m.createEscrow(ctx, model, roleRegular, snapshot.EpochIndex, snapshot.BlockHeight); err != nil {
		return fmt.Errorf("creating replacement for depleted escrow %s: %w", record.EscrowID, err)
	}
	if err := m.retire(ctx, record); err != nil {
		return fmt.Errorf("retiring depleted escrow %s: %w", record.EscrowID, err)
	}
	return nil
}
