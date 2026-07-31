package escrow

import (
	"context"
	"errors"
	"fmt"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
)

// OnBalanceExhausted marks an escrow for replacement; the work happens in the next tick, so this
// hook does no I/O and never fans out a per-escrow chain call.
func (m *Manager) OnBalanceExhausted(escrowID string) {
	m.depletionMu.Lock()
	defer m.depletionMu.Unlock()
	if m.depletedMarks == nil {
		m.depletedMarks = make(map[string]bool)
	}
	m.depletedMarks[escrowID] = true
}

func (m *Manager) checkDepletion(ctx context.Context, snapshot chain.PhaseSnapshot, models []ModelConfig, devshards []store.DevshardRecord) error {
	marked := m.drainMarks()
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
		model, ok := modelByID[record.Model]
		if !ok {
			continue
		}
		if err := m.replaceDepleted(ctx, record, model, snapshot); err != nil {
			m.OnBalanceExhausted(record.EscrowID) // a failed replacement must not un-schedule itself
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) drainMarks() map[string]bool {
	m.depletionMu.Lock()
	defer m.depletionMu.Unlock()
	marks := m.depletedMarks
	m.depletedMarks = nil
	return marks
}

// replaceDepleted creates the replacement before retiring the depleted escrow, so coverage never
// drops to zero; a failed create leaves the depleted escrow in place for the next attempt.
func (m *Manager) replaceDepleted(ctx context.Context, record store.DevshardRecord, model ModelConfig, snapshot chain.PhaseSnapshot) error {
	if _, err := m.createEscrow(ctx, model, record.RotationRole, snapshot.EpochIndex, snapshot.BlockHeight); err != nil {
		return fmt.Errorf("creating replacement for depleted escrow %s: %w", record.EscrowID, err)
	}
	if err := m.retire(ctx, record); err != nil {
		return fmt.Errorf("retiring depleted escrow %s: %w", record.EscrowID, err)
	}
	return nil
}
