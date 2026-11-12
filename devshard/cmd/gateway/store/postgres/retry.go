package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

func isRetryable(err error) bool {
	// A connection lost mid-statement is not retried: the write may have committed.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == serializationFailure || pgErr.Code == deadlockDetected
	}
	return pgconn.SafeToRetry(err)
}
