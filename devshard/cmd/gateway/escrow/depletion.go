package escrow

import (
	"context"
	"errors"
	"fmt"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
)

// OnBalanceExhausted marks an escrow for replacement in the next tick, so this hook does no I/O.
func (m *Manager) OnBalanceExhausted(escrowID string, reason scheduler.ExhaustionReason) {
	if m.depleted.mark(escrowID, reason) && m.narrator != nil {
		m.narrator.EscrowMarkedForReplacement(escrowID, string(reason))
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
	counts := newModelCounts(devshards)

	var errs []error
	for _, record := range devshards {
		reason, isMarked := marked[record.EscrowID]
		if !record.Active || !isMarked {
			continue
		}
		model, replaceable := modelByID[record.Model]
		var err error
		if m.holdApplies(replaceable) {
			err = m.holdOrPark(ctx, record, reason, model, snapshot, counts)
		} else {
			err = m.replaceDepleted(ctx, record, reason, model, replaceable, snapshot)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// replaceDepleted parks an exhausted escrow, then makes one attempt at replacing it. See README.md, "Replacing a depleted escrow".
func (m *Manager) replaceDepleted(ctx context.Context, record store.DevshardRecord, reason scheduler.ExhaustionReason, model ModelConfig, replaceable bool, snapshot chain.PhaseSnapshot) error {
	// An escrow created under an epoch-less snapshot is counted by no epoch at all, so the next bridge funds a full set on top of it.
	if replaceable && snapshotHasNoEpochYet(snapshot) {
		m.depleted.mark(record.EscrowID, reason)
		return fmt.Errorf("replacing depleted escrow %s: the chain snapshot carries no epoch yet", record.EscrowID)
	}
	parked, err := m.parkIfServing(ctx, record.EscrowID)
	if !parked {
		if err != nil {
			m.depleted.mark(record.EscrowID, reason)
		}
		return err
	}
	routingErr := err
	if !replaceable {
		if m.narrator != nil {
			m.narrator.EscrowDepletedWithoutReplacement(record.EscrowID, record.Model)
		}
		return routingErr
	}
	if _, createErr := m.createEscrow(ctx, model, roleRegular, snapshot.EpochIndex, snapshot.BlockHeight); createErr != nil {
		return errors.Join(routingErr, fmt.Errorf("creating replacement for depleted escrow %s: %w", record.EscrowID, createErr))
	}
	return routingErr
}
