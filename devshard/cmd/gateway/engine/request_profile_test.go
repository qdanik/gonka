package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/scheduler"
)

// The scheduler names this id on every burn the pick causes, so every pick of a race must carry it.
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
