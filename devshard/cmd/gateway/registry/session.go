package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"devshard/cmd/gateway/scheduler"
	"devshard/state"
	"devshard/types"
	"devshard/user"
)

// ghostPrompt is the synthetic MsgStart a burned nonce commits so every nonce the escrow advances
// through has an owner; it is composed into the diff but never sent to a host.
var ghostPrompt = []byte(`{"messages":[{"role":"user","content":"."}],"max_tokens":1}`)

const ghostMaxTokens = 1

// errNonceDeclined leaves the bound nonce unconsumed, so the next caller sees the same nonce.
var errNonceDeclined = errors.New("nonce declined")

// EscrowSession is one escrow's session as the registry uses it. sessionHandle binds a *user.Session
// to its state machine to satisfy it.
type EscrowSession interface {
	ParticipantKeys() []string
	HostParticipantKeyList() []string
	Nonce() uint64
	Phase() types.SessionPhase
	PrepareInferenceFn(chooser user.ParamsForHost) (*user.PreparedInference, error)
	Signatures() map[uint64]map[uint32][]byte
	SnapshotState() types.EscrowState
	Finalize(ctx context.Context) error
	FlushSnapshot() error
	Close() error
	// UserSession is the concrete handle the dispatch boundary needs; one rehydrated read-only has no
	// host clients, so sending through it is a bug.
	UserSession() *user.Session
}

// ServingSessions opens a chain-backed session with host clients (user.NewHTTPSession) — the only kind
// that can dispatch. ReadOnlySessions rehydrates from local storage alone (user.NewLocalSession): no
// chain, no host clients, so it can build a settlement but can neither serve nor finalize.
type SessionFactory func(ctx context.Context, escrowID string) (EscrowSession, error)

type sessionHandle struct {
	*user.Session
	machine *state.StateMachine
}

func NewSessionHandle(session *user.Session, machine *state.StateMachine) EscrowSession {
	return sessionHandle{Session: session, machine: machine}
}

func (h sessionHandle) Phase() types.SessionPhase        { return h.machine.Phase() }
func (h sessionHandle) SnapshotState() types.EscrowState { return h.machine.SnapshotState() }
func (h sessionHandle) UserSession() *user.Session       { return h.Session }

type nonceStream struct {
	session EscrowSession
	model   string
	now     func() time.Time
}

func (s nonceStream) ParticipantKeys() []string { return s.session.ParticipantKeys() }
func (s nonceStream) GroupSize() int            { return len(s.session.HostParticipantKeyList()) }
func (s nonceStream) LatestNonce() uint64       { return s.session.Nonce() }

func (s nonceStream) Advance(decide func(scheduler.HostBinding) scheduler.NonceIntent) (scheduler.Prepared, error) {
	prepared, err := s.session.PrepareInferenceFn(func(binding user.HostBinding) (user.InferenceParams, bool, error) {
		intent := decide(scheduler.HostBinding{
			Nonce:       binding.Nonce,
			HostIdx:     binding.HostIdx,
			Participant: binding.ParticipantKey,
		})
		switch {
		case !intent.Commit:
			return user.InferenceParams{}, false, errNonceDeclined
		case intent.Ghost:
			return s.ghostParams(), false, nil
		}
		params, isInference := intent.Params.(user.InferenceParams)
		if !isInference {
			return user.InferenceParams{}, false, fmt.Errorf("dispatch params are %T, want user.InferenceParams", intent.Params)
		}
		return params, false, nil
	})
	switch {
	case errors.Is(err, errNonceDeclined):
		return nil, nil
	case err != nil:
		return nil, err
	case prepared == nil:
		return nil, nil
	}
	return prepared, nil
}

func (s nonceStream) ghostParams() user.InferenceParams {
	return user.InferenceParams{
		Model:       s.model,
		Prompt:      ghostPrompt,
		InputLength: uint64(len(ghostPrompt)),
		MaxTokens:   ghostMaxTokens,
		StartedAt:   s.now().UnixMilli(),
	}
}
