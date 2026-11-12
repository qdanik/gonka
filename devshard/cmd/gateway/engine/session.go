package engine

import (
	"context"
	"time"

	"devshard/gatewayprotocol/timeoutclaim"
	"devshard/host"
	"devshard/user"
)

// The session's own words for a skip: the shared package exports no constant for either.
const (
	sessionOutcomeSkipped    = "skipped"
	sessionDetailNonceClosed = "nonce_closed"
)

// timeoutHandler is the session's protocol-timeout entry point; *user.Session satisfies it.
type timeoutHandler interface {
	timeoutclaim.Handler
	timeoutclaim.FinishSource
	TimeoutDeadline(nonce uint64, sendTime time.Time) (string, time.Time)
	IsNonceCanonicalFinished(nonce uint64) bool
}

// SessionTimeouts posts the chain vote that settles one escrow's unfinished nonces.
type SessionTimeouts struct {
	handler timeoutHandler
	payload *host.InferencePayload
}

func NewSessionTimeouts(session *user.Session, payload *host.InferencePayload) *SessionTimeouts {
	return &SessionTimeouts{handler: session, payload: payload}
}

// VoteDeadline is the moment the handler's own wait would end, read before the wait rather than inside it.
func (s *SessionTimeouts) VoteDeadline(nonce uint64, startedAt time.Time) time.Time {
	_, deadline := s.handler.TimeoutDeadline(nonce, startedAt)
	return deadline
}

// SettleTimeout reads the handler's own record of whether the vote landed: the error alone cannot tell. A vote whose nonce had its Finish applied while it waited is not sent. See README, "Timeout votes".
func (s *SessionTimeouts) SettleTimeout(ctx context.Context, step TimeoutStep) (TimeoutVote, error) {
	if step.Kind == SettleByVote && s.handler.IsNonceCanonicalFinished(step.Nonce) {
		return TimeoutVote{Completeness: step.Proof.completeness()}, user.ErrNonceFinishedWhileWaiting
	}
	result, err := s.settle(ctx, step)
	vote := TimeoutVote{
		Kind:          TimeoutKind(result.Reason),
		Detail:        result.DetailReason,
		Skipped:       result.Outcome == sessionOutcomeSkipped,
		Unjudged:      result.Outcome == user.TimeoutOutcomeVoteCollectionFailed,
		VerifyRejects: result.VerifyRejects,
		Completeness:  step.Proof.completeness(),
	}
	if result.Applied {
		return vote, nil
	}
	return vote, err
}

func (s *SessionTimeouts) settle(ctx context.Context, step TimeoutStep) (user.TimeoutResult, error) {
	var finishTx, responsePayload []byte
	if step.Kind == SettleByMissClaim && step.Proof != nil {
		finishTx, responsePayload = timeoutclaim.FinishTx(step.Proof.finishTx, step.Nonce, s.handler), step.Proof.ResponsePayload
	}
	return timeoutclaim.Resolve(ctx, s.handler, step.Nonce, step.StartedAt, s.payload, finishTx, responsePayload)
}
