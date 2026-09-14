package main

import (
	"testing"

	"devshard/cmd/gateway/internal/logcapture"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/scheduler"
)

// A burned nonce is money spent on nobody; the line is the only place its nonce is named.
func TestABurnedNonceIsLoggedWithTheEscrowItCostAndWhy(t *testing.T) {
	logged := logcapture.Install(t)
	dispatches := tracedDispatches{recorder: metrics.NewDispatchRecorder(metrics.New())}

	dispatches.GhostBurned("escrow-1", scheduler.Burn{
		Nonce: 5, Participant: "gonka1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Reason: "participant_throttled_no_send",
	})

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{
		"escrow", "escrow-1", "nonce", uint64(5), "host", "bbbbbbbb", "reason", "participant_throttled_no_send",
	}})
}

func TestAnEscrowAtItsBurnBudgetSaysSo(t *testing.T) {
	logged := logcapture.Install(t)
	dispatches := tracedDispatches{recorder: metrics.NewDispatchRecorder(metrics.New())}

	dispatches.BurnBudgetExhausted("escrow-1")

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "escrow stopped burning nonces at its budget", Fields: []any{
		"escrow", "escrow-1",
	}})
}
