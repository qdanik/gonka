// Package timeoutclaim holds the choice between an error-miss claim and a timeout vote, and the timeout vote's payload. See README.md.
package timeoutclaim

import (
	"context"
	"time"

	"devshard/host"
	"devshard/user"
)

// Handler posts a timeout vote or an error-miss claim for an unfinished nonce; *user.Session satisfies it.
type Handler interface {
	HandleTimeout(ctx context.Context, nonce uint64, sendTime time.Time, payload *host.InferencePayload) (user.TimeoutResult, error)
	HandleErrorMiss(ctx context.Context, nonce uint64, finishTx, responsePayload []byte) (user.TimeoutResult, error)
}

// Resolve claims an error miss when finishTx is non-empty and votes a timeout with payload otherwise.
func Resolve(ctx context.Context, handler Handler, nonce uint64, sendTime time.Time, payload *host.InferencePayload, finishTx, responsePayload []byte) (user.TimeoutResult, error) {
	if len(finishTx) > 0 {
		return handler.HandleErrorMiss(ctx, nonce, finishTx, responsePayload)
	}
	return handler.HandleTimeout(ctx, nonce, sendTime, payload)
}
