package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// upsertDevshardStatement lives apart from its caller so a test can read which columns the update
// carries: a field added to DevshardRecord that nobody adds here is inserted once and never updated
// again, and nothing else in the package fails when that happens.
const upsertDevshardStatement = `
		INSERT INTO devshards (escrow_id, private_key_env, model, active, rotation_role, rotation_epoch, settlement_pending, settle_tx_hash, route_prefix)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(escrow_id) DO UPDATE SET
			private_key_env = excluded.private_key_env,
			model = excluded.model,
			active = excluded.active,
			rotation_role = excluded.rotation_role,
			rotation_epoch = excluded.rotation_epoch,
			settle_tx_hash = excluded.settle_tx_hash,
			updated_at = datetime('now')`

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
	SettleTxHash      string
	RoutePrefix       string
}

// UpsertDevshard replaces every field of an existing row except settlement_pending, so an unrelated
// upsert never silently clears a queued settlement; only SetDevshardSettlementPending moves it.
func (s *Store) UpsertDevshard(ctx context.Context, record DevshardRecord) error {
	_, err := s.db.ExecContext(ctx, upsertDevshardStatement,
		record.EscrowID, record.PrivateKeyEnv, record.Model, record.Active,
		record.RotationRole, record.RotationEpoch, record.SettlementPending, record.SettleTxHash,
		record.RoutePrefix)
	if err != nil {
		return fmt.Errorf("upserting devshard %s: %w", record.EscrowID, err)
	}
	return nil
}

// ListDevshards returns every record ordered by escrow id.
func (s *Store) ListDevshards(ctx context.Context) ([]DevshardRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT escrow_id, private_key_env, model, active, rotation_role, rotation_epoch, settlement_pending, settle_tx_hash, route_prefix
		FROM devshards ORDER BY escrow_id`)
	if err != nil {
		return nil, fmt.Errorf("listing devshards: %w", err)
	}
	defer rows.Close()
	var records []DevshardRecord
	for rows.Next() {
		var record DevshardRecord
		if err := rows.Scan(&record.EscrowID, &record.PrivateKeyEnv, &record.Model,
			&record.Active, &record.RotationRole, &record.RotationEpoch, &record.SettlementPending,
			&record.SettleTxHash, &record.RoutePrefix); err != nil {
			return nil, fmt.Errorf("scanning devshard row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating devshards: %w", err)
	}
	return records, nil
}

func (s *Store) SetDevshardActive(ctx context.Context, escrowID string, active bool) error {
	return s.updateDevshardField(ctx, `UPDATE devshards SET active = ?, updated_at = datetime('now') WHERE escrow_id = ?`, active, escrowID)
}

func (s *Store) SetDevshardSettlementPending(ctx context.Context, escrowID string, pending bool) error {
	return s.updateDevshardField(ctx, `UPDATE devshards SET settlement_pending = ?, updated_at = datetime('now') WHERE escrow_id = ?`, pending, escrowID)
}

// ParkForSettlement takes the escrow out of service and marks it pending in one statement. Written as
// two, a crash between them leaves the row inactive and not pending, which no recovery path picks up:
// settlePending looks for pending rows, so the escrow would be out of service and never settled.
func (s *Store) ParkForSettlement(ctx context.Context, escrowID string) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE devshards SET active = 0, settlement_pending = 1, updated_at = datetime('now') WHERE escrow_id = ?`,
		escrowID)
	if err != nil {
		return fmt.Errorf("parking devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func (s *Store) DeleteDevshard(ctx context.Context, escrowID string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM devshards WHERE escrow_id = ?`, escrowID)
	if err != nil {
		return fmt.Errorf("deleting devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func (s *Store) updateDevshardField(ctx context.Context, query string, value any, escrowID string) error {
	result, err := s.db.ExecContext(ctx, query, value, escrowID)
	if err != nil {
		return fmt.Errorf("updating devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func requireOneRow(result sql.Result, escrowID string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking affected rows for %s: %w", escrowID, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", escrowID, ErrDevshardNotFound)
	}
	return nil
}

// SetDevshardSettleTxHash records the transaction a settle broadcast, so a tick that finds the row
// still pending can ask the chain what happened to it instead of building a second one.
func (s *Store) SetDevshardSettleTxHash(ctx context.Context, escrowID, txHash string) error {
	return s.updateDevshardField(ctx, `UPDATE devshards SET settle_tx_hash = ?, updated_at = datetime('now') WHERE escrow_id = ?`, txHash, escrowID)
}
