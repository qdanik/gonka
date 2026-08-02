package store

import (
	"context"
	"errors"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const retryAttempts = 10

// WithRetry runs fn up to retryAttempts times, waiting s.retryBackoff between attempts, to ride out a
// transient write lock. It returns immediately on success, aborts with ctx.Err() without sleeping past
// a cancellation, and otherwise returns the last error once every attempt is exhausted.
//
// Only a locked database is retried. Everything else -- a missing row, a constraint -- fails the same
// way on every attempt, so retrying it spends the whole ladder to return the error the first call
// already had, and the caller waits seconds for an answer it could have had at once.
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

// isLockedError reports the two codes a busy database answers with; the driver's own busy_timeout
// absorbs the short waits, so seeing one here means the lock outlived it. It is a field on Store
// rather than a direct call because the driver's error type cannot be constructed outside its own
// package, and the retry ladder's own mechanics -- backoff, cancellation, attempt count -- have to be
// testable without provoking a real cross-process lock.
func isLockedError(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	// Masked to the primary code: with one connection a lock surfaces as the primary form, but the
	// driver enables extended codes, and a comparison that only matches the primary would stop
	// retrying the moment the connection model changed.
	code := sqliteErr.Code() & 0xFF
	return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
}
