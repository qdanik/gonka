package timeoutclaim

import (
	"context"
	"reflect"
	"testing"

	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/state"
	"devshard/user"
)

const escrowID = "7"

func preparedParams() user.InferenceParams {
	return user.InferenceParams{
		Model:            "qwen",
		Prompt:           []byte(`{"messages":[{"role":"user","content":"hello"}],"max_tokens":64}`),
		InputLength:      512,
		MaxTokens:        64,
		ContextTotalHint: 4096,
		StartedAt:        1_700_000_000,
		Stream:           true,
	}
}

func newSession(t *testing.T) (*user.Session, *state.StateMachine) {
	t.Helper()
	signers := []*signing.Secp256k1Signer{testutil.MustGenerateKey(t), testutil.MustGenerateKey(t)}
	group := testutil.MakeGroup(signers)
	config := testutil.DefaultConfig(len(group))
	creator := testutil.MustGenerateKey(t)
	verifier := signing.NewSecp256k1Verifier()
	store := testutil.MustMemoryStore(t, escrowID, creator.Address(), config, group, 1_000_000)
	machine, err := state.NewStateMachine(escrowID, config, group, 1_000_000, creator.Address(), verifier, store)
	if err != nil {
		t.Fatalf("NewStateMachine = %v, want nil", err)
	}
	session, err := user.NewSession(machine, creator, escrowID, group, make([]user.HostClient, len(group)), verifier, user.WithStorage(store))
	if err != nil {
		t.Fatalf("NewSession = %v, want nil", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, machine
}

// Test flow:
//  1. Build a real session over an in-memory state machine and prepare one inference with params whose every field is distinct and non-zero.
//  2. Read the record the state machine committed for the prepared nonce.
//  3. Assert `PayloadFromParams` of the params equals `PayloadFromRecord` of that record with the params' prompt, and that both equal the prepared inference's own payload.
func TestPayloadFromParamsEqualsPayloadFromTheCommittedRecord(t *testing.T) {
	session, machine := newSession(t)
	params := preparedParams()
	prepared, err := session.PrepareInference(context.Background(), params)
	if err != nil {
		t.Fatalf("PrepareInference = %v, want nil", err)
	}
	record, committed := machine.SnapshotState().Inferences[prepared.Nonce()]
	if !committed {
		t.Fatalf("nonce %d has no committed record", prepared.Nonce())
	}

	fromParams := PayloadFromParams(params)
	fromRecord := PayloadFromRecord(*record, params.Prompt)

	if !reflect.DeepEqual(fromParams, fromRecord) {
		t.Errorf("PayloadFromParams = %+v, PayloadFromRecord = %+v, want equal", fromParams, fromRecord)
	}
	want := prepared.Payload()
	if !reflect.DeepEqual(fromParams, want) {
		t.Errorf("PayloadFromParams = %+v, want the prepared inference's payload %+v", fromParams, want)
	}
}
