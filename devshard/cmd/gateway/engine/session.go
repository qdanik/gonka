package engine

import (
	"context"
	"errors"
	"time"

	"devshard/host"
	"devshard/user"
)

// timeoutHandler is the session's protocol-timeout entry point; *user.Session satisfies it.
type timeoutHandler interface {
	HandleTimeout(ctx context.Context, nonce uint64, sendTime time.Time, payload *host.InferencePayload) (user.TimeoutResult, error)
}

// SessionTimeouts posts the chain vote that settles one escrow's unfinished nonces.
type SessionTimeouts struct {
	handler timeoutHandler
	payload *host.InferencePayload
}

func NewSessionTimeouts(session *user.Session, payload *host.InferencePayload) *SessionTimeouts {
	return &SessionTimeouts{handler: session, payload: payload}
}

// SettleTimeout reads a posted vote as a success. The handler returns a non-nil error on its own
// success path — that error carries "the inference timed out" to the request rather than a failed
// vote — and only its genuine failures wrap a cause, so an unwrapped error beside a reported reason is
// a vote that reached the chain.
func (s *SessionTimeouts) SettleTimeout(ctx context.Context, nonce uint64, sentAt time.Time) (string, error) {
	result, err := s.handler.HandleTimeout(ctx, nonce, sentAt, s.payload)
	if result.Reason != "" && errors.Unwrap(err) == nil {
		return result.Reason, nil
	}
	return result.Reason, err
}
