package api

import (
	"context"
	"errors"
	"fmt"
	"io"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/host"
	"devshard/state"
	"devshard/types"
	"devshard/user"
)

// escrows is satisfied by *registry.Registry. See rules.md, "4. Routing and settlement read the escrow set asymmetrically, on purpose".
type escrows interface {
	Acquire(escrowID string) (registry.EscrowSession, func(), bool)
	SettlementSession(escrowID string) (registry.EscrowSession, bool)
}

// Sessions adapts the live escrows to the engine's two outbound boundaries: the dispatch target and the vote poster.
type Sessions struct{ escrows escrows }

func NewSessions(escrows escrows) Sessions { return Sessions{escrows: escrows} }

// Target resolves one escrow per race, because an escrow can rotate out between the pick and the send.
func (s Sessions) Target(escrowID string) (engine.DispatchTarget, func(), bool) {
	session, release, held := s.escrows.Acquire(escrowID)
	if !held {
		return nil, nil, false
	}
	return escrowTarget{session: session.UserSession()}, release, true
}

// dispatchSession is one escrow's session as a race spends it; *user.Session satisfies it.
type dispatchSession interface {
	SendOnly(ctx context.Context, prepared *user.PreparedInference, stream io.Writer, onReceipt func()) (*host.HostResponse, error)
	ProcessResponse(hostIdx int, reply *host.HostResponse, inferenceNonce uint64) error
	IsNonceFinished(nonce uint64) bool
	HostParticipantKeyList() []string
	HostLabel(hostIdx int) string
	RewindHostCatchUp(hostIdx int, cause string) bool
}

type escrowTarget struct{ session dispatchSession }

var _ engine.DispatchTarget = escrowTarget{}

func (t escrowTarget) HostCount() int               { return len(t.session.HostParticipantKeyList()) }
func (t escrowTarget) HostLabel(hostIdx int) string { return t.session.HostLabel(hostIdx) }

func (t escrowTarget) RewindHostCatchUp(hostIdx int, cause string) bool {
	return t.session.RewindHostCatchUp(hostIdx, cause)
}
func (t escrowTarget) NonceFinished(nonce uint64) bool { return t.session.IsNonceFinished(nonce) }

func (t escrowTarget) Send(ctx context.Context, nonce scheduler.Prepared, stream io.Writer, onReceipt func()) (engine.Response, error) {
	prepared, ok := nonce.(*user.PreparedInference)
	if !ok {
		return nil, fmt.Errorf("dispatch nonce is %T, want *user.PreparedInference", nonce)
	}
	// stream is handed on unwrapped: the transport type-asserts http.Flusher on it to flush each SSE line.
	reply, err := t.session.SendOnly(ctx, prepared, stream, onReceipt)
	if reply != nil {
		// Applies for a reply that arrived beside an error too; see rules.md.
		if applyErr := t.session.ProcessResponse(prepared.HostIdx(), reply, prepared.Nonce()); err == nil {
			err = applyErr
		}
	}
	if divergedState(err) {
		err = fmt.Errorf("%w: %w", engine.ErrStateRootDivergence, err)
	}
	if reply == nil {
		return nil, err
	}
	return hostReply{reply: reply}, err
}

// divergedState reads a rejected post state root from either side of the wire: the host's diff or the local hash.
func divergedState(err error) bool {
	return state.IsPostStateRootMismatchError(err) || errors.Is(err, types.ErrStateHashMismatch)
}

type hostReply struct{ reply *host.HostResponse }

var _ engine.Response = hostReply{}

func (r hostReply) Confirmed() bool { return r.reply.ConfirmedAt > 0 }

func (r hostReply) ConfirmedAt() int64 { return r.reply.ConfirmedAt }
