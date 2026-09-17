package api

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"devshard/transport"
)

// A host-bound body carries the escrow's catch-up diffs beside the prompt, and the diffs are the gateway's own
// state, so a client cannot be refused for them. A prompt admitted against the whole budget therefore fails at
// send time — after the nonce is committed, which spends the escrow's reserve on nobody. The reserve moves that
// refusal to the boundary, where it costs a 413 and no nonce.
func TestChatLeavesRoomForTheEscrowsCatchUp(t *testing.T) {
	live := newHarness(t)
	promptBytes := 3 * (transport.MaxHostRequestBytes - hostCatchUpReserveBytes/2) / 4
	oversized := fmt.Sprintf(`{"model":"qwen","messages":[{"role":"user","content":%q}]}`, strings.Repeat("x", promptBytes))

	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", oversized, nil)

	if transport.PromptWireBytes(promptBytes) > transport.MaxHostRequestBytes {
		t.Fatalf("the fixture is past the host budget on its own, so it proves nothing about the reserve")
	}
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d (%s), want 413", recorder.Code, recorder.Body.String())
	}
	if got := live.limiter.acquires.Load(); got != 0 {
		t.Fatalf("limiter slots taken: got %d, want 0 — the refusal must precede admission", got)
	}
}
