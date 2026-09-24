package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/scheduler"
)

// Test flow:
//  1. Build a `raceCoordinator` for a request with a known id, model, escrow, input tokens and bytes.
//  2. Build its request profile with a params string.
//  3. Assert the profile carries the request id, model, escrow, input tokens/bytes and params through unchanged.
func TestAPickCarriesTheRequestItServes(t *testing.T) {
	coordinator := &raceCoordinator{
		request:  raceRequest{Request: Request{RequestID: "request-3", Model: testModel, InputTokens: 12, InputBytes: 47}},
		escrowID: "escrow-1",
	}

	profile := coordinator.requestProfile("params")

	require.Equal(t, scheduler.RequestProfile{
		RequestID: "request-3", Model: testModel, Escrow: "escrow-1", InputTokens: 12, InputBytes: 47, Params: "params",
	}, profile)
}
