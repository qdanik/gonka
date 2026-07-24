package store

import (
	"context"
	"time"
)

const retryAttempts = 10

// WithRetry runs fn up to retryAttempts times, waiting s.retryBackoff between
// attempts, to ride out a transient write lock. It returns immediately on
// success, aborts with ctx.Err() without sleeping past a cancellation, and
// otherwise returns the last error once every attempt is exhausted.
func (s *Store) WithRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := range retryAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lastErr = fn(); lastErr == nil {
			return nil
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
