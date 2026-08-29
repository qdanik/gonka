package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
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
	EscrowID          string `json:"escrow_id"`
	PrivateKeyEnv     string `json:"private_key_env"`
	Model             string `json:"model"`
	Active            bool   `json:"active"`
	RotationRole      string `json:"rotation_role"`
	RotationEpoch     int64  `json:"rotation_epoch"`
	SettlementPending bool   `json:"settlement_pending"`
	SettleTxHash      string `json:"settle_tx_hash"`
	RoutePrefix       string `json:"route_prefix"`
}

// UpsertDevshard replaces every field of an existing row except two. settlement_pending is left alone
// so an unrelated upsert never clears a queued settlement; only SetDevshardSettlementPending moves it.
// route_prefix is left alone because a host binds an escrow to the first version that reaches it and
// refuses every other one for good, so the version an escrow was created under must never change.
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

// DevshardSettleTxHash reads the hash from the row rather than from a record the caller is holding: a
// tick loads its escrows once and several steps settle from that one slice, so a copy taken at the top
// no longer says what an earlier step in the same tick broadcast.
func (s *Store) DevshardSettleTxHash(ctx context.Context, escrowID string) (hash string, broadcastAt time.Time, err error) {
	var stamp string
	err = s.WithRetry(ctx, func() error {
		return s.db.QueryRowContext(ctx,
			`SELECT settle_tx_hash, settle_tx_at FROM devshards WHERE escrow_id = ?`, escrowID).Scan(&hash, &stamp)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("reading settle tx hash for escrow %s: %w", escrowID, err)
	}
	if parsed, parseErr := time.Parse(time.DateTime, stamp); parseErr == nil {
		broadcastAt = parsed.UTC()
	}
	return hash, broadcastAt, nil
}

// SetDevshardRotationRole moves one escrow between roles without touching anything else. A whole-record
// upsert would carry the caller's copy of active and settle_tx_hash back into the row, and a tick loads
// its escrows once: the copy predates whatever an earlier step in the same tick wrote.
func (s *Store) SetDevshardRotationRole(ctx context.Context, escrowID, role string) error {
	return s.updateDevshardField(ctx,
		`UPDATE devshards SET rotation_role = ?, updated_at = datetime('now') WHERE escrow_id = ?`, role, escrowID)
}

// SetDevshardSettleTxHash records the transaction a settle broadcast, so a tick that finds the row
// still pending can ask the chain what happened to it instead of building a second one.
func (s *Store) SetDevshardSettleTxHash(ctx context.Context, escrowID, txHash string) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE devshards SET settle_tx_hash = ?, settle_tx_at = CASE WHEN ? = '' THEN '' ELSE datetime('now') END, updated_at = datetime('now') WHERE escrow_id = ?`,
		txHash, txHash, escrowID)
	if err != nil {
		return fmt.Errorf("updating devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}
