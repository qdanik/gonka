package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	retryAttempts         = 10
	defaultRetryBackoff   = 200 * time.Millisecond
	sqlitePrimaryCodeMask = 0xFF
)

// WithRetry retries only a locked database, up to retryAttempts. See README.md, "Writing under a lock".
func (s *Store) WithRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := range retryAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lastErr = fn(); lastErr == nil {
			return nil
		}
		if !s.retryable(lastErr) {
			return lastErr
		}
		if attempt < retryAttempts-1 {
			timer := time.NewTimer(s.retryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

// isLockedError is a field on Store because the driver's error type cannot be constructed outside its own package. See README.md, "Writing under a lock".
func isLockedError(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	// Masked to the primary code, because the driver enables extended codes.
	code := sqliteErr.Code() & sqlitePrimaryCodeMask
	return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
}

// isRetryablePostgresError is a serialization failure, a deadlock or an error pgx marks as raised before anything reached the server. A connection lost mid-statement is not retried: the write may have committed, and a statement or lock timeout only repeats the wait.
func isRetryablePostgresError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return pgconn.SafeToRetry(err)
}
