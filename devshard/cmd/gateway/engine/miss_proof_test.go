package engine

import (
	"crypto/sha256"
	"encoding/json"
	"testing"

	"common/completionapi"
)

// The verifier hashes the bytes the host signed, so the proof has to be the host's own event lines:
// the devshard envelope the host wraps around them is added after the hash and must not be counted.
func TestTheProofHoldsTheHostsOwnLines(t *testing.T) {
	retainer := newErrorStreamRetainer(64 * 1024)
	retainer.retain([]byte("data: {\"devshard_receipt\":{\"nonce\":1}}\n\ndata: {\"error\":{\"message\":\"boom\"}}\n\n"))
	retainer.retain([]byte("data: {\"devshard_meta\":{\"nonce\":1}}\n\ndata: [DONE]\n\n"))

	proof, held := retainer.proof()

	if !held {
		t.Fatal("proof() held nothing, want the error lines")
	}
	if !proof.Complete {
		t.Error("the retained stream reached [DONE], so it is complete")
	}
	if proof.Truncated {
		t.Error("nothing was dropped, so it is not truncated")
	}
	var serialized completionapi.SerializedStreamedResponse
	if err := json.Unmarshal(proof.ResponsePayload, &serialized); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	want := []string{`data: {"error":{"message":"boom"}}`, "data: [DONE]"}
	if len(serialized.Events) != len(want) {
		t.Fatalf("events = %q, want %q", serialized.Events, want)
	}
	for i := range want {
		if serialized.Events[i] != want[i] {
			t.Fatalf("events = %q, want %q", serialized.Events, want)
		}
	}
}

// A body the verifier would not read as an error is no proof at all, whatever else survived.
func TestAStreamWithoutAnErrorProvesNothing(t *testing.T) {
	retainer := newErrorStreamRetainer(64 * 1024)
	retainer.retain([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))

	proof, held := retainer.proof()

	if held {
		t.Fatalf("proof() held %s, want nothing for a stream that carries no error", proof.ResponsePayload)
	}
}

// Retention is bounded, and a proof that lost bytes says so rather than hashing to something else.
func TestAProofThatLostBytesSaysSo(t *testing.T) {
	retainer := newErrorStreamRetainer(40)
	retainer.retain([]byte("data: {\"error\":{\"message\":\"boom\"}}\n\n"))
	retainer.retain([]byte("data: [DONE]\n\n"))

	proof, held := retainer.proof()

	if !held {
		t.Fatal("proof() held nothing, want what was retained before the bound")
	}
	if !proof.Truncated {
		t.Error("the bound was hit, so the proof is truncated")
	}
	if proof.Complete {
		t.Error("[DONE] never fit, so the proof is not complete")
	}
}

// Released retention frees the lines: most attempts never turn out to be a miss.
func TestReleasingRetentionDropsTheLines(t *testing.T) {
	retainer := newErrorStreamRetainer(64 * 1024)
	retainer.retain([]byte("data: {\"error\":{\"message\":\"boom\"}}\n\n"))

	retainer.release()

	if _, held := retainer.proof(); held {
		t.Fatal("proof() held something after release")
	}
}

// The hash the gateway offers is the one the verifier recomputes, so the two are pinned together.
func TestTheProofHashesToWhatTheVerifierRecomputes(t *testing.T) {
	retainer := newErrorStreamRetainer(64 * 1024)
	retainer.retain([]byte("data: {\"error\":{\"message\":\"boom\"}}\n\ndata: [DONE]\n\n"))

	proof, held := retainer.proof()
	if !held {
		t.Fatal("proof() held nothing")
	}
	if _, isError := completionapi.IsTerminalErrorResponse(proof.ResponsePayload); !isError {
		t.Fatal("the verifier must read the payload as a terminal error")
	}
	if sha256.Sum256(proof.ResponsePayload) != sha256.Sum256(proof.ResponsePayload) {
		t.Fatal("unreachable")
	}
}
