package escrow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
)

// modelCounts tracks, within one tick, how many escrows of a model serve and how many are on hold.
type modelCounts struct {
	serving map[string]int
	onHold  map[string]int
}

func newModelCounts(devshards []store.DevshardRecord) modelCounts {
	counts := modelCounts{serving: map[string]int{}, onHold: map[string]int{}}
	for _, record := range devshards {
		switch {
		case !record.Active:
		case record.OnHold:
			counts.onHold[record.Model]++
		default:
			counts.serving[record.Model]++
		}
	}
	return counts
}

// holdApplies is false whenever today's park-and-replace must run unchanged. See README.md, "An escrow on hold".
func (m *Manager) holdApplies(replaceable bool) bool {
	return m.holds != nil && replaceable && m.config.Load().Rotation.HoldEnabled
}

func (m *Manager) holdOrPark(ctx context.Context, record store.DevshardRecord, reason scheduler.ExhaustionReason, model ModelConfig, snapshot chain.PhaseSnapshot, counts modelCounts) error {
	if snapshotHasNoEpochYet(snapshot) {
		m.depleted.mark(record.EscrowID, reason)
		return fmt.Errorf("handling depleted escrow %s: the chain snapshot carries no epoch yet", record.EscrowID)
	}
	if record.OnHold {
		return nil
	}
	if reason == scheduler.ExhaustionNonceCap || counts.onHold[record.Model] >= int(m.config.Load().Rotation.HoldMaxPerModel) {
		return m.parkDepleted(ctx, record, reason, model, snapshot, counts)
	}
	if epoch, known := m.creationEpoch(ctx, record); !known || epoch < int64(snapshot.EpochIndex) {
		return m.parkDepleted(ctx, record, reason, model, snapshot, counts)
	}
	held, err := m.putOnHold(ctx, record.EscrowID)
	if !held {
		if err != nil {
			m.depleted.mark(record.EscrowID, reason)
		}
		return err
	}
	counts.serving[record.Model]--
	counts.onHold[record.Model]++
	replacementID, replacementErr := m.replaceIfShort(ctx, model, snapshot, counts)
	if m.narrator != nil {
		balance, reserved, challenged, _ := m.holds.Funds(record.EscrowID)
		m.narrator.EscrowPutOnHold(record.EscrowID, record.Model, string(reason), balance, reserved, challenged, replacementID)
	}
	return replacementErr
}

func (m *Manager) parkDepleted(ctx context.Context, record store.DevshardRecord, reason scheduler.ExhaustionReason, model ModelConfig, snapshot chain.PhaseSnapshot, counts modelCounts) error {
	parked, err := m.parkIfServing(ctx, record.EscrowID)
	if !parked {
		if err != nil {
			m.depleted.mark(record.EscrowID, reason)
		}
		return err
	}
	counts.serving[record.Model]--
	_, replacementErr := m.replaceIfShort(ctx, model, snapshot, counts)
	return errors.Join(err, replacementErr)
}

// putOnHold writes the row first, so a crash before the registry call leaves the row to be published on hold.
func (m *Manager) putOnHold(ctx context.Context, escrowID string) (bool, error) {
	var held bool
	if err := m.store.WithRetry(ctx, func() error {
		var err error
		held, err = m.store.PutOnHoldIfServing(ctx, escrowID)
		return err
	}); err != nil {
		return false, fmt.Errorf("putting escrow %s on hold: %w", escrowID, err)
	}
	if held {
		m.holds.SetOnHold(escrowID, true)
	}
	return held, nil
}

// replaceIfShort funds a replacement only while the model has fewer serving escrows than its target. See README.md, "An escrow on hold".
func (m *Manager) replaceIfShort(ctx context.Context, model ModelConfig, snapshot chain.PhaseSnapshot, counts modelCounts) (string, error) {
	if counts.serving[model.ModelID] >= model.TargetCount {
		return "", nil
	}
	result, err := m.createEscrow(ctx, model, roleRegular, snapshot.EpochIndex, snapshot.BlockHeight)
	if err != nil {
		return "", fmt.Errorf("creating replacement for model %s: %w", model.ModelID, err)
	}
	counts.serving[model.ModelID]++
	return strconv.FormatUint(result.EscrowID, 10), nil
}

// resumeHeld runs before checkDepletion, so the rows it syncs the registry from predate this tick's own holds. See README.md, "An escrow on hold".
func (m *Manager) resumeHeld(ctx context.Context, snapshot chain.PhaseSnapshot, models []ModelConfig, devshards []store.DevshardRecord) ([]store.DevshardRecord, error) {
	if m.holds == nil {
		return devshards, nil
	}
	rotation := m.config.Load().Rotation
	replaceable := make(map[string]bool, len(models))
	for _, model := range models {
		replaceable[model.ModelID] = true
	}
	updated := slices.Clone(devshards)
	var errs []error
	for index, record := range updated {
		if !record.Active {
			continue
		}
		if !record.OnHold {
			m.holds.SetOnHold(record.EscrowID, false)
			continue
		}
		settled, err := m.settleHold(ctx, record, rotation, replaceable[record.Model], snapshot)
		if err != nil {
			errs = append(errs, err)
		}
		updated[index] = settled
	}
	return updated, errors.Join(errs...)
}

func (m *Manager) settleHold(ctx context.Context, record store.DevshardRecord, rotation config.Rotation, replaceable bool, snapshot chain.PhaseSnapshot) (store.DevshardRecord, error) {
	epoch, known := m.creationEpoch(ctx, record)
	if reason := holdEndReason(rotation, replaceable, epoch, known, snapshot); reason != "" {
		return m.endHold(ctx, record, reason)
	}
	switch m.holds.Verdict(record.EscrowID, uint64(rotation.HoldResumeAnswers)) {
	case HoldNonceSpent:
		return m.endHold(ctx, record, holdEndedNonceSpent)
	case HoldResume:
		return m.resume(ctx, record)
	default:
		m.holds.SetOnHold(record.EscrowID, true)
		return record, nil
	}
}

func holdEndReason(rotation config.Rotation, replaceable bool, epoch int64, known bool, snapshot chain.PhaseSnapshot) holdEnding {
	switch {
	case !rotation.HoldEnabled:
		return holdEndedDisabled
	case !rotation.Enabled || !replaceable:
		return holdEndedRotationOff
	case snapshotHasNoEpochYet(snapshot):
		return ""
	case !known:
		return holdEndedEpochUnknown
	case epoch < int64(snapshot.EpochIndex):
		return holdEndedEpochPassed
	}
	return ""
}

// creationEpoch is never written back to the row: a seeded row with an epoch would start counting in the bridge.
func (m *Manager) creationEpoch(ctx context.Context, record store.DevshardRecord) (int64, bool) {
	if record.RotationEpoch > 0 {
		return record.RotationEpoch, true
	}
	info, found, err := m.tx.GetEscrow(ctx, record.EscrowID)
	if err != nil || !found || info.EpochIndex == 0 {
		return 0, false
	}
	return int64(info.EpochIndex), true
}

// endHold parks through the same statement depletion uses, which clears the hold with active.
func (m *Manager) endHold(ctx context.Context, record store.DevshardRecord, reason holdEnding) (store.DevshardRecord, error) {
	parked, err := m.parkIfServing(ctx, record.EscrowID)
	if parked {
		record.Active = false
		record.OnHold = false
		record.SettlementPending = true
		if m.narrator != nil {
			m.narrator.EscrowHoldEnded(record.EscrowID, string(reason))
		}
	}
	return record, err
}

func (m *Manager) resume(ctx context.Context, record store.DevshardRecord) (store.DevshardRecord, error) {
	var resumed bool
	if err := m.store.WithRetry(ctx, func() error {
		var err error
		resumed, err = m.store.ResumeFromHold(ctx, record.EscrowID)
		return err
	}); err != nil {
		return record, fmt.Errorf("resuming escrow %s from hold: %w", record.EscrowID, err)
	}
	if !resumed {
		return record, nil
	}
	record.OnHold = false
	// A report from before the resume would put the escrow straight back on hold.
	m.depleted.forget(record.EscrowID)
	m.holds.SetOnHold(record.EscrowID, false)
	if m.narrator != nil {
		balance, _, _, _ := m.holds.Funds(record.EscrowID)
		m.narrator.EscrowResumed(record.EscrowID, balance)
	}
	return record, nil
}
