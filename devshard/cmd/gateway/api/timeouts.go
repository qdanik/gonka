package api

import (
	"context"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/host"
	"devshard/types"
	"devshard/user"
)

// Poster resolves the vote poster for one escrow. A retired escrow still resolves: its nonces have no other path.
func (s Sessions) Poster(escrowID string, params any) (engine.TimeoutPoster, bool) {
	dispatched, isInference := params.(user.InferenceParams)
	if !isInference {
		return nil, false
	}
	session, held := s.escrows.SettlementSession(escrowID)
	if !held {
		return nil, false
	}
	return escrowVotes{session: session, prompt: dispatched.Prompt}, true
}

type escrowVotes struct {
	session registry.EscrowSession
	prompt  []byte
}

var _ engine.TimeoutPoster = escrowVotes{}

func (v escrowVotes) SettleTimeout(ctx context.Context, nonce uint64, startedAt time.Time) (engine.TimeoutVote, error) {
	payload := timeoutPayload(v.session.SnapshotState(), nonce, v.prompt)
	return engine.NewSessionTimeouts(v.session.UserSession(), payload).SettleTimeout(ctx, nonce, startedAt)
}

// timeoutPayload rebuilds what the host was asked for, reading every field but the prompt back from the record.
func timeoutPayload(escrow types.EscrowState, nonce uint64, prompt []byte) *host.InferencePayload {
	record, committed := escrow.Inferences[nonce]
	if !committed {
		return nil
	}
	return &host.InferencePayload{
		Prompt:      prompt,
		Model:       record.Model,
		InputLength: record.InputLength,
		MaxTokens:   record.MaxTokens,
		StartedAt:   record.StartedAt,
	}
}
