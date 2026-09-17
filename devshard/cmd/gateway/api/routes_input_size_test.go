package api

import (
	"net/http"
	"testing"
)

// The balance floor prices a request against the escrow the way the chain does, and the chain reserves against
// the body's byte length. The gateway's own input number is that length divided by four, so the two must arrive
// separately and neither may be derived from the other downstream: a floor fed the estimate budgets a quarter of
// what the chain takes. See docs/capacity.md, "The balance floor".
func TestTheRaceIsHandedTheBodysBytesBesideTheTokenEstimate(t *testing.T) {
	live := newHarness(t)

	if recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil); recorder.Code != http.StatusOK {
		t.Fatalf("chat: got %d (%s)", recorder.Code, recorder.Body.String())
	}

	raced := live.inference.raced.Load()
	if raced == nil {
		t.Fatal("the race was never handed a request")
	}
	if raced.InputBytes == 0 {
		t.Fatal("the race was handed no body length, so the balance floor prices the input side at zero")
	}
	if want := (raced.InputBytes + 3) / 4; raced.InputTokens != want {
		t.Fatalf("InputTokens = %d over %d bytes, want %d: the two must describe the same body", raced.InputTokens, raced.InputBytes, want)
	}
}
