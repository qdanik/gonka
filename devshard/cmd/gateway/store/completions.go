package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"devshard/user"
)

var _ user.InferenceCompletionStore = (*Store)(nil)

// operationContext bounds the completion calls, which take no context of their own; SQLite needs no bound.
func (s *Store) operationContext() (context.Context, context.CancelFunc) {
	if s.operationTimeout <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), s.operationTimeout)
}

// SaveInferenceCompletion records a started nonce before it is sent. Saving the same payload again is a no-op; a different one at the same nonce is user.ErrInferenceCompletionConflict. See README.md, "The completion journal".
func (s *Store) SaveInferenceCompletion(escrowID string, entry user.InferenceCompletion) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encoding completion %s/%d: %w", escrowID, entry.Nonce, err)
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	return s.WithRetry(ctx, func() error {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO inference_completions (escrow_id, nonce, entry_json)
			VALUES (?, ?, ?) ON CONFLICT (escrow_id, nonce) DO NOTHING`, escrowID, entry.Nonce, data); err != nil {
			return fmt.Errorf("saving completion %s/%d: %w", escrowID, entry.Nonce, err)
		}
		var stored []byte
		if err := s.db.QueryRowContext(ctx, `SELECT entry_json FROM inference_completions WHERE escrow_id = ? AND nonce = ?`, escrowID, entry.Nonce).Scan(&stored); err != nil {
			return fmt.Errorf("reading completion %s/%d back: %w", escrowID, entry.Nonce, err)
		}
		return checkCompletionConflict(escrowID, entry, stored)
	})
}

func checkCompletionConflict(escrowID string, entry user.InferenceCompletion, stored []byte) error {
	var previous user.InferenceCompletion
	if err := json.Unmarshal(stored, &previous); err != nil {
		return fmt.Errorf("decoding stored completion %s/%d: %w", escrowID, entry.Nonce, err)
	}
	want, _ := json.Marshal(entry.Payload)
	got, _ := json.Marshal(previous.Payload)
	if !bytes.Equal(want, got) {
		return fmt.Errorf("%w: escrow %s nonce %d", user.ErrInferenceCompletionConflict, escrowID, entry.Nonce)
	}
	return nil
}

// ListInferenceCompletions returns the escrow's entries ordered by nonce.
func (s *Store) ListInferenceCompletions(escrowID string) ([]user.InferenceCompletion, error) {
	ctx, cancel := s.operationContext()
	defer cancel()
	var entries []user.InferenceCompletion
	err := s.WithRetry(ctx, func() error {
		entries = nil
		rows, err := s.db.QueryContext(ctx, `SELECT nonce, entry_json FROM inference_completions WHERE escrow_id = ? ORDER BY nonce`, escrowID)
		if err != nil {
			return fmt.Errorf("listing completions of %s: %w", escrowID, err)
		}
		defer rows.Close()
		for rows.Next() {
			var nonce uint64
			var data []byte
			if err := rows.Scan(&nonce, &data); err != nil {
				return fmt.Errorf("scanning completion row: %w", err)
			}
			entry, err := decodeCompletion(escrowID, nonce, data)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return rows.Err()
	})
	return entries, err
}

func decodeCompletion(escrowID string, nonce uint64, data []byte) (user.InferenceCompletion, error) {
	var entry user.InferenceCompletion
	if err := json.Unmarshal(data, &entry); err != nil {
		return entry, fmt.Errorf("decoding completion %s/%d: %w", escrowID, nonce, err)
	}
	if entry.Nonce != nonce || entry.PreparedAt <= 0 {
		return entry, fmt.Errorf("invalid completion record for escrow %s nonce %d", escrowID, nonce)
	}
	return entry, nil
}

// DeleteInferenceCompletion removes a finished or timed-out entry; an absent one is a no-op.
func (s *Store) DeleteInferenceCompletion(escrowID string, nonce uint64) error {
	ctx, cancel := s.operationContext()
	defer cancel()
	return s.WithRetry(ctx, func() error {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM inference_completions WHERE escrow_id = ? AND nonce = ?`, escrowID, nonce); err != nil {
			return fmt.Errorf("deleting completion %s/%d: %w", escrowID, nonce, err)
		}
		return nil
	})
}
