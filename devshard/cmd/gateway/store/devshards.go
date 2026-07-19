package store

import (
	"context"
	"errors"
	"fmt"
)

// ErrDevshardNotFound is returned by updates/deletes that match no row.
var ErrDevshardNotFound = errors.New("devshard not found")

// DevshardRecord is one row of the devshard registry. Private keys are never
// stored — only the name of the environment variable that holds the key.
type DevshardRecord struct {
	EscrowID          string
	PrivateKeyEnv     string
	Model             string
	Active            bool
	RotationRole      string
	RotationEpoch     int64
	SettlementPending bool
}

// UpsertDevshard inserts or fully replaces the record for its escrow.
func (s *Store) UpsertDevshard(ctx context.Context, record DevshardRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO devshards (escrow_id, private_key_env, model, active, rotation_role, rotation_epoch, settlement_pending)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(escrow_id) DO UPDATE SET
			private_key_env = excluded.private_key_env,
			model = excluded.model,
			active = excluded.active,
			rotation_role = excluded.rotation_role,
			rotation_epoch = excluded.rotation_epoch,
			settlement_pending = excluded.settlement_pending,
			updated_at = datetime('now')`,
		record.EscrowID, record.PrivateKeyEnv, record.Model, record.Active,
		record.RotationRole, record.RotationEpoch, record.SettlementPending)
	if err != nil {
		return fmt.Errorf("upserting devshard %s: %w", record.EscrowID, err)
	}
	return nil
}

// ListDevshards returns every record ordered by escrow id.
func (s *Store) ListDevshards(ctx context.Context) ([]DevshardRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT escrow_id, private_key_env, model, active, rotation_role, rotation_epoch, settlement_pending
		FROM devshards ORDER BY escrow_id`)
	if err != nil {
		return nil, fmt.Errorf("listing devshards: %w", err)
	}
	defer rows.Close()
	var records []DevshardRecord
	for rows.Next() {
		var record DevshardRecord
		if err := rows.Scan(&record.EscrowID, &record.PrivateKeyEnv, &record.Model,
			&record.Active, &record.RotationRole, &record.RotationEpoch, &record.SettlementPending); err != nil {
			return nil, fmt.Errorf("scanning devshard row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating devshards: %w", err)
	}
	return records, nil
}

// SetDevshardActive flips the active flag.
func (s *Store) SetDevshardActive(ctx context.Context, escrowID string, active bool) error {
	return s.updateDevshardFlag(ctx, `UPDATE devshards SET active = ?, updated_at = datetime('now') WHERE escrow_id = ?`, active, escrowID)
}

// SetDevshardSettlementPending flips the settlement_pending flag.
func (s *Store) SetDevshardSettlementPending(ctx context.Context, escrowID string, pending bool) error {
	return s.updateDevshardFlag(ctx, `UPDATE devshards SET settlement_pending = ?, updated_at = datetime('now') WHERE escrow_id = ?`, pending, escrowID)
}

// DeleteDevshard removes the record.
func (s *Store) DeleteDevshard(ctx context.Context, escrowID string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM devshards WHERE escrow_id = ?`, escrowID)
	if err != nil {
		return fmt.Errorf("deleting devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func (s *Store) updateDevshardFlag(ctx context.Context, query string, flagValue bool, escrowID string) error {
	result, err := s.db.ExecContext(ctx, query, flagValue, escrowID)
	if err != nil {
		return fmt.Errorf("updating devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func requireOneRow(result interface{ RowsAffected() (int64, error) }, escrowID string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking affected rows for %s: %w", escrowID, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", escrowID, ErrDevshardNotFound)
	}
	return nil
}
