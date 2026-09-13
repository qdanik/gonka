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

// OnBalanceExhausted marks an escrow for replacement in the next tick, so this hook does no I/O.
func (m *Manager) OnBalanceExhausted(escrowID, reason string) {
	if m.depleted.mark(escrowID) {
		logging.Warn("escrow marked for replacement", logkey.Escrow, escrowID, logkey.Reason, reason)
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
		if err := m.replaceDepleted(ctx, record, modelByID, snapshot); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// replaceDepleted parks an exhausted escrow, then makes one attempt at replacing it. See README.md, "Replacing a depleted escrow".
func (m *Manager) replaceDepleted(ctx context.Context, record store.DevshardRecord, modelByID map[string]ModelConfig, snapshot chain.PhaseSnapshot) error {
	model, replaceable := modelByID[record.Model]
	// An escrow created under an epoch-less snapshot is counted by no epoch at all, so the next bridge funds a full set on top of it.
	if replaceable && (snapshot.EpochIndex == 0 || snapshot.BlockHeight == 0) {
		m.depleted.mark(record.EscrowID)
		return fmt.Errorf("replacing depleted escrow %s: the chain snapshot carries no epoch yet", record.EscrowID)
	}
	parked, err := m.parkIfServing(ctx, record.EscrowID)
	if !parked {
		if err != nil {
			m.depleted.mark(record.EscrowID)
		}
		return err
	}
	routingErr := err
	if !replaceable {
		logging.Warn("escrow depleted with no replacement configured", logkey.Escrow, record.EscrowID, logkey.Model, record.Model)
		return routingErr
	}
	if _, createErr := m.createEscrow(ctx, model, roleRegular, snapshot.EpochIndex, snapshot.BlockHeight); createErr != nil {
		return errors.Join(routingErr, fmt.Errorf("creating replacement for depleted escrow %s: %w", record.EscrowID, createErr))
	}
	return routingErr
}
