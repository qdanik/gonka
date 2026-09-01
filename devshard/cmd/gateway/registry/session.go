package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"common/completionapi"

	"devshard/cmd/gateway/scheduler"
	"devshard/state"
	"devshard/types"
	"devshard/user"
)

const ghostMaxTokens = uint64(completionapi.MinTokensFloor)

var (
	// ghostPrompt is composed into the diff, never sent to a host. See routing.md, "Ghost burns".
	ghostPrompt = fmt.Appendf(nil, `{"messages":[{"role":"user","content":"."}],"max_tokens":%d}`, ghostMaxTokens)

	// errNonceDeclined leaves the bound nonce unconsumed, so the next caller sees the same nonce.
	errNonceDeclined = errors.New("nonce declined")
)

// EscrowSession is one escrow's session as the registry uses it; two of its methods carry a trap. See README.md, "The two session kinds".
type EscrowSession interface {
	ParticipantKeys() []string
	HostParticipantKeyList() []string
	Nonce() uint64
	Balance() uint64
	TokenPrice() uint64
	Phase() types.SessionPhase
	PrepareInferenceFn(chooser user.ParamsForHost) (*user.PreparedInference, error)
	Signatures() map[uint64]map[uint32][]byte
	SignedSlots() map[uint64]types.Bitmap128
	SignatureStatus() (entries []user.SignatureStatusEntry, highestQuorum uint64, hasAny bool)
	SnapshotState() types.EscrowState
	SealedInferences() int
	Finalize(ctx context.Context) error
	FlushSnapshot() error
	Close() error
	UserSession() *user.Session
}

// SessionFactory: an escrow with no record must fail wrapping escrow.ErrUnknownEscrow. See README.md, "The two session kinds".
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
func (h sessionHandle) SealedInferences() int            { return len(h.machine.ExportSealedNonces()) }
func (h sessionHandle) UserSession() *user.Session       { return h.Session }

// groupSize is taken once: the group is fixed, and asking the session takes the lock a nonce commit holds.
type nonceStream struct {
	session   EscrowSession
	model     string
	groupSize int
	now       func() time.Time
}

// newNonceStream is the only way to build one, so the cached group size cannot disagree.
func newNonceStream(session EscrowSession, model string, now func() time.Time) nonceStream {
	return nonceStream{session: session, model: model, groupSize: len(session.HostParticipantKeyList()), now: now}
}

func (s nonceStream) ParticipantKeys() []string { return s.session.ParticipantKeys() }
func (s nonceStream) GroupSize() int            { return s.groupSize }
func (s nonceStream) LatestNonce() uint64       { return s.session.Nonce() }
func (s nonceStream) Balance() uint64           { return s.session.Balance() }
func (s nonceStream) TokenPrice() uint64        { return s.session.TokenPrice() }

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

// StartedAt is seconds, not milliseconds, or the refusal deadline goes negative. See host/timeout.go, VerifyRefusedTimeout.
func (s nonceStream) ghostParams() user.InferenceParams {
	return user.InferenceParams{
		Model:       s.model,
		Prompt:      ghostPrompt,
		InputLength: uint64(len(ghostPrompt)),
		MaxTokens:   ghostMaxTokens,
		StartedAt:   s.now().Unix(),
	}
}
