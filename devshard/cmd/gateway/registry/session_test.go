package registry

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"common/completionapi"

	"devshard/cmd/gateway/scheduler"
	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/state"
	"devshard/types"
	"devshard/user"
)

const liveEscrowID = "7"

// newLiveStream builds a real *user.Session over in-memory storage and nil host clients: composing and
// committing a nonce is local work, so the whole peek-decide-commit unit runs without a chain or a host.
func newLiveStream(t *testing.T, slotsPerSigner ...int) (nonceStream, EscrowSession, []types.SlotAssignment) {
	t.Helper()
	signers := make([]*signing.Secp256k1Signer, len(slotsPerSigner))
	for index := range signers {
		signers[index] = testutil.MustGenerateKey(t)
	}
	group := testutil.MakeMultiSlotGroup(signers, slotsPerSigner)
	config := testutil.DefaultConfig(len(group))
	creator := testutil.MustGenerateKey(t)
	verifier := signing.NewSecp256k1Verifier()
	store := testutil.MustMemoryStore(t, liveEscrowID, creator.Address(), config, group, 1_000_000)

	machine, err := state.NewStateMachine(liveEscrowID, config, group, 1_000_000, creator.Address(), verifier, store)
	if err != nil {
		t.Fatalf("NewStateMachine = %v, want nil", err)
	}
	session, err := user.NewSession(machine, creator, liveEscrowID, group, make([]user.HostClient, len(group)), verifier, user.WithStorage(store))
	if err != nil {
		t.Fatalf("NewSession = %v, want nil", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	handle := NewSessionHandle(session, machine)
	return newNonceStream(handle, "qwen", fixedClock()), handle, group
}

func TestAdvanceCommitsTheServeParams(t *testing.T) {
	t.Parallel()
	stream, session, group := newLiveStream(t, 1, 1, 1)
	params := user.InferenceParams{Model: "qwen", Prompt: []byte(`{"messages":[]}`), InputLength: 15, MaxTokens: 64}

	var seen scheduler.HostBinding
	prepared, err := stream.Advance(func(binding scheduler.HostBinding) scheduler.NonceIntent {
		seen = binding
		return scheduler.NonceIntent{Commit: true, Params: params}
	})
	if err != nil {
		t.Fatalf("Advance = %v, want nil", err)
	}
	if prepared == nil {
		t.Fatal("Advance returned no prepared nonce, want one")
	}
	if got, want := prepared.Nonce(), uint64(1); got != want {
		t.Errorf("prepared nonce = %d, want %d", got, want)
	}
	if got, want := prepared.HostIdx(), 1; got != want {
		t.Errorf("prepared host index = %d, want %d (nonce %% group size)", got, want)
	}
	if seen.Nonce != 1 || seen.HostIdx != 1 || seen.Participant != group[1].ValidatorAddress {
		t.Errorf("binding handed to decide = %+v, want nonce 1 on %s", seen, group[1].ValidatorAddress)
	}
	if got, want := session.Nonce(), uint64(1); got != want {
		t.Errorf("session nonce after a commit = %d, want %d", got, want)
	}
}

func TestAdvanceBurnsAGhostNonceWithoutARequest(t *testing.T) {
	t.Parallel()
	stream, session, _ := newLiveStream(t, 1, 1)

	prepared, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
		return scheduler.NonceIntent{Commit: true, Ghost: true}
	})
	if err != nil {
		t.Fatalf("Advance = %v, want nil", err)
	}
	if prepared == nil {
		t.Fatal("a ghost must still commit its nonce, got none")
	}
	if got, want := session.Nonce(), uint64(1); got != want {
		t.Errorf("session nonce after a ghost = %d, want %d", got, want)
	}
}

func TestAdvanceLeavesADeclinedNonceUnconsumed(t *testing.T) {
	t.Parallel()
	stream, session, _ := newLiveStream(t, 1, 1)

	prepared, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
		return scheduler.NonceIntent{}
	})
	if err != nil {
		t.Fatalf("Advance = %v, want nil (a decline is not a failure)", err)
	}
	if prepared != nil {
		t.Errorf("Advance returned %v for a declined nonce, want nil", prepared)
	}
	if got := session.Nonce(); got != 0 {
		t.Errorf("session nonce after a decline = %d, want 0", got)
	}
}

func TestAdvanceRejectsParamsItCannotDispatch(t *testing.T) {
	t.Parallel()
	stream, session, _ := newLiveStream(t, 1, 1)

	_, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
		return scheduler.NonceIntent{Commit: true, Params: "not an inference"}
	})

	if err == nil {
		t.Fatal("Advance = nil, want an error naming the wrong params type")
	}
	if got := session.Nonce(); got != 0 {
		t.Errorf("session nonce after rejected params = %d, want 0", got)
	}
}

func TestAdvanceReportsASessionFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("storage gone")
	session := newFakeSession("hostA")
	session.prepare = func(user.ParamsForHost) (*user.PreparedInference, error) { return nil, failure }
	stream := newNonceStream(session, "qwen", fixedClock())

	_, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
		return scheduler.NonceIntent{Commit: true, Params: user.InferenceParams{}}
	})

	if !errors.Is(err, failure) {
		t.Errorf("Advance = %v, want it to wrap %v", err, failure)
	}
}

func TestStreamReportsTheGroupAndTheLatestNonce(t *testing.T) {
	t.Parallel()
	// Two signers, the first holding two slots: the group is three slots over two participants.
	stream, _, group := newLiveStream(t, 2, 1)

	if got, want := stream.GroupSize(), len(group); got != want {
		t.Errorf("GroupSize() = %d, want %d", got, want)
	}
	if got, want := len(stream.ParticipantKeys()), 2; got != want {
		t.Errorf("len(ParticipantKeys()) = %d, want %d distinct participants", got, want)
	}
	if got := stream.LatestNonce(); got != 0 {
		t.Errorf("LatestNonce() on a fresh session = %d, want 0", got)
	}

	if _, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
		return scheduler.NonceIntent{Commit: true, Ghost: true}
	}); err != nil {
		t.Fatalf("Advance = %v, want nil", err)
	}

	if got := stream.LatestNonce(); got != 1 {
		t.Errorf("LatestNonce() after one commit = %d, want 1", got)
	}
}

func TestGhostParamsCarryTheEscrowModelAndTheInjectedClock(t *testing.T) {
	t.Parallel()
	stream := newNonceStream(newFakeSession("hostA"), "kimi", fixedClock())

	params := stream.ghostParams()

	want := user.InferenceParams{
		Model:       "kimi",
		Prompt:      ghostPrompt,
		InputLength: uint64(len(ghostPrompt)),
		MaxTokens:   ghostMaxTokens,
		StartedAt:   fixedClock()().Unix(),
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("ghostParams() = %+v, want %+v", params, want)
	}
}

func TestSessionHandleReportsThePhaseOfItsOwnStateMachine(t *testing.T) {
	t.Parallel()
	_, session, _ := newLiveStream(t, 1, 1)

	if got, want := session.Phase(), types.PhaseActive; got != want {
		t.Errorf("Phase() = %v, want %v", got, want)
	}
	if session.UserSession() == nil {
		t.Error("UserSession() = nil, want the concrete handle the dispatch boundary needs")
	}
}

// The burn's body and its reservation are one fact written twice. They never meet a host today, but a
// probe that starts being sent would be refused for declaring less than it reserved.
func TestGhostPromptAgreesWithItsReservation(t *testing.T) {
	var body struct {
		MaxTokens uint64 `json:"max_tokens"`
	}
	if err := json.Unmarshal(ghostPrompt, &body); err != nil {
		t.Fatalf("parsing ghostPrompt: %v", err)
	}
	if body.MaxTokens != ghostMaxTokens {
		t.Errorf("ghostPrompt max_tokens = %d, reservation = %d", body.MaxTokens, ghostMaxTokens)
	}
	if ghostMaxTokens < completionapi.MinTokensFloor {
		t.Errorf("ghostMaxTokens = %d, below the floor %d the chain refuses", ghostMaxTokens, completionapi.MinTokensFloor)
	}
}
