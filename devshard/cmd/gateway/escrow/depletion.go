package escrow

import (
	"context"
	"errors"
	"fmt"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
)

// depletionScanBudget caps how many escrows are probed on-chain per tick, so the proactive scan's
// RPC rate is constant regardless of how many escrows exist (the whole set sweeps over several ticks).
const depletionScanBudget = 16

// OnBalanceExhausted marks an escrow for replacement; the work happens in the next checkDepletion
// tick, so this hook does no I/O and never fans out a per-escrow chain call.
func (m *Manager) OnBalanceExhausted(escrowID string) {
	m.depletionMu.Lock()
	defer m.depletionMu.Unlock()
	if m.depletedMarks == nil {
		m.depletedMarks = make(map[string]bool)
	}
	m.depletedMarks[escrowID] = true
}

func (m *Manager) checkDepletion(ctx context.Context, snapshot chain.PhaseSnapshot, models []ModelConfig, devshards []store.DevshardRecord) error {
	modelByID := make(map[string]ModelConfig, len(models))
	for _, model := range models {
		modelByID[model.ModelID] = model
	}

	var errs []error
	replace := func(record store.DevshardRecord) {
		model, ok := modelByID[record.Model]
		if !ok {
			return
		}
		if err := m.replaceDepleted(ctx, record, model, snapshot); err != nil {
			errs = append(errs, err)
		}
	}

	for _, record := range m.drainMarkedActive(devshards) {
		replace(record)
	}
	for _, record := range m.proactiveScanBatch(activeRecords(devshards)) {
		info, found, err := m.tx.GetEscrow(ctx, record.EscrowID)
		if err != nil || !found || info.Balance != 0 {
			continue
		}
		replace(record)
	}
	return errors.Join(errs...)
}

func (m *Manager) drainMarkedActive(devshards []store.DevshardRecord) []store.DevshardRecord {
	m.depletionMu.Lock()
	marks := m.depletedMarks
	m.depletedMarks = nil
	m.depletionMu.Unlock()
	if len(marks) == 0 {
		return nil
	}
	var marked []store.DevshardRecord
	for _, record := range devshards {
		if record.Active && marks[record.EscrowID] {
			marked = append(marked, record)
		}
	}
	return marked
}

// proactiveScanBatch returns at most depletionScanBudget active records starting at the round-robin
// cursor and advances it, so consecutive ticks sweep the whole set. The cursor is tick-only state.
func (m *Manager) proactiveScanBatch(active []store.DevshardRecord) []store.DevshardRecord {
	if len(active) == 0 {
		return nil
	}
	count := min(depletionScanBudget, len(active))
	batch := make([]store.DevshardRecord, 0, count)
	for i := range count {
		batch = append(batch, active[(m.depletionCursor+i)%len(active)])
	}
	m.depletionCursor = (m.depletionCursor + count) % len(active)
	return batch
}

func activeRecords(devshards []store.DevshardRecord) []store.DevshardRecord {
	var active []store.DevshardRecord
	for _, record := range devshards {
		if record.Active {
			active = append(active, record)
		}
	}
	return active
}

// replaceDepleted creates the replacement before retiring the depleted escrow, so coverage never
// drops to zero; a failed create leaves the depleted escrow in place for the next attempt.
func (m *Manager) replaceDepleted(ctx context.Context, record store.DevshardRecord, model ModelConfig, snapshot chain.PhaseSnapshot) error {
	if err := m.createEscrow(ctx, model, record.RotationRole, snapshot.EpochIndex, snapshot.BlockHeight); err != nil {
		return fmt.Errorf("creating replacement for depleted escrow %s: %w", record.EscrowID, err)
	}
	if err := m.retire(ctx, record); err != nil {
		return fmt.Errorf("retiring depleted escrow %s: %w", record.EscrowID, err)
	}
	return nil
}
