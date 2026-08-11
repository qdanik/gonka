package engine

import (
	"context"
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

// SettleTimeout reads the handler's own record of whether the timeout reached the escrow state. The
// handler returns a non-nil error on its success path too -- that error carries "the inference timed out"
// to the request -- so the error alone cannot tell a settled vote from an unsettled one.
func (s *SessionTimeouts) SettleTimeout(ctx context.Context, nonce uint64, startedAt time.Time) (string, error) {
	result, err := s.handler.HandleTimeout(ctx, nonce, startedAt, s.payload)
	if result.Applied {
		return result.Reason, nil
	}
	return result.Reason, err
}
