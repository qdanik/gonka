package accounting

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
)

func (s *Store) Save(ctx context.Context, book *Book) (err error) {
	if s == nil || s.db == nil {
		return nil
	}
	snapshot := book.Snapshot()
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning accounting write: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, transaction.Rollback())
		}
	}()

	for _, table := range clearedTables {
		if _, err = transaction.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("clearing %s: %w", table, err)
		}
	}
	if _, err = transaction.ExecContext(ctx,
		`INSERT INTO accounting_meta (key, value) VALUES (?, ?), (?, ?)`,
		metaSchemaVersion, strconv.Itoa(SchemaVersion),
		metaUpdatedAt, snapshot.UpdatedAt.Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("writing accounting meta: %w", err)
	}
	for _, escrow := range snapshot.Escrows {
		if err = writeEscrow(ctx, transaction, escrow); err != nil {
			return err
		}
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("committing accounting write: %w", err)
	}
	return nil
}

func writeEscrow(ctx context.Context, transaction *sql.Tx, escrow EscrowSnapshot) error {
	identity := escrow.Metadata.EscrowID
	if _, err := transaction.ExecContext(ctx,
		`INSERT INTO accounting_escrows (escrow_id, model, creation_epoch, latest_nonce, retired)
		 VALUES (?, ?, ?, ?, ?)`,
		identity, escrow.Metadata.Model, escrow.Metadata.CreationEpoch, escrow.LatestNonce, escrow.Retired,
	); err != nil {
		return fmt.Errorf("writing escrow %s: %w", identity, err)
	}
	for _, slot := range escrow.Metadata.Slots {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_slots (escrow_id, slot_id, validator_address) VALUES (?, ?, ?)`,
			identity, slot.SlotID, slot.ValidatorAddress,
		); err != nil {
			return fmt.Errorf("writing slot %d of %s: %w", slot.SlotID, identity, err)
		}
	}
	for _, slotID := range slices.Sorted(maps.Keys(escrow.HostStats)) {
		stats := escrow.HostStats[slotID]
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_host_stats
			 (escrow_id, slot_id, missed, invalid, cost, required_validations, completed_validations)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			identity, slotID, stats.Missed, stats.Invalid, stats.Cost,
			stats.RequiredValidations, stats.CompletedValidations,
		); err != nil {
			return fmt.Errorf("writing host stats for slot %d of %s: %w", slotID, identity, err)
		}
	}
	for _, slotID := range slices.Sorted(maps.Keys(escrow.SlotActivity)) {
		activity := escrow.SlotActivity[slotID]
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_slot_activity
			 (escrow_id, slot_id, challenged, validations, timeouts_applied, rejected)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			identity, slotID, activity.Challenged, activity.Validations,
			activity.TimeoutsApplied, activity.Rejected,
		); err != nil {
			return fmt.Errorf("writing slot activity for slot %d of %s: %w", slotID, identity, err)
		}
	}
	for _, slotID := range slices.Sorted(maps.Keys(slotsWithTotals(escrow))) {
		money := escrow.Money[slotID]
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_money
			 (escrow_id, slot_id, reserved_cost, actual_cost, refunded_cost, estimated_input, estimated_error,
			  max_tokens, input_tokens, counted_nonces, output_tokens)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			identity, slotID, money.Reserved, money.Actual, money.Refunded, money.EstimatedInput,
			money.EstimatedError, money.MaxTokens, money.Input, money.CountedNonces,
			escrow.Produced[slotID],
		); err != nil {
			return fmt.Errorf("writing money for slot %d of %s: %w", slotID, identity, err)
		}
	}
	for _, counter := range escrow.Counters {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_counters
			 (escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason,
			  terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded, count)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			identity, counter.SlotID, counter.Disposition, counter.GhostReason,
			counter.TimeoutKind, counter.TimeoutAction, counter.TimeoutReason,
			counter.Terminal, counter.Phase, counter.SlowReceipt, counter.SlowChunk, counter.ClockDrifted,
			counter.SlowDecode, counter.LogprobsDecoded, counter.Count,
		); err != nil {
			return fmt.Errorf("writing a counter of %s: %w", identity, err)
		}
	}
	for _, stored := range escrow.Nonces {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_nonces
			 (escrow_id, nonce, sent, acknowledged, usage, timeout_kind, timeout_action, timeout_reason,
			  terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			identity, stored.Nonce, stored.Sent, stored.Acknowledged, stored.Usage,
			stored.TimeoutKind, stored.TimeoutAction, stored.TimeoutReason,
			stored.Terminal, stored.Phase, stored.SlowReceipt, stored.SlowChunk, stored.ClockDrifted,
			stored.SlowDecode, stored.LogprobsDecoded,
		); err != nil {
			return fmt.Errorf("writing nonce %d of %s: %w", stored.Nonce, identity, err)
		}
	}
	return nil
}

func slotsWithTotals(escrow EscrowSnapshot) map[uint32]struct{} {
	slots := make(map[uint32]struct{}, len(escrow.Money)+len(escrow.Produced))
	for slotID := range escrow.Money {
		slots[slotID] = struct{}{}
	}
	for slotID := range escrow.Produced {
		slots[slotID] = struct{}{}
	}
	return slots
}
