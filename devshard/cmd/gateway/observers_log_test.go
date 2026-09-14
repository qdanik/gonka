package main

import (
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logcapture"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/scheduler"
)

// A burned nonce is money spent on nobody; the line is the only place its nonce is named.
func TestABurnedNonceIsLoggedWithTheEscrowItCostAndWhy(t *testing.T) {
	logged := logcapture.Install(t)
	events := newTestJournal(t)
	dispatches := tracedDispatches{recorder: metrics.NewDispatchRecorder(metrics.New()), events: events}

	dispatches.GhostBurned("escrow-1", scheduler.Burn{
		Nonce: 5, Participant: "gonka1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Reason: "participant_throttled_no_send",
	})
	events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{
		"escrow", "escrow-1", "nonce", uint64(5), "host", "bbbbbbbb", "reason", "participant_throttled_no_send",
	}})
}

func TestAnEscrowAtItsBurnBudgetSaysSo(t *testing.T) {
	logged := logcapture.Install(t)
	events := newTestJournal(t)
	dispatches := tracedDispatches{recorder: metrics.NewDispatchRecorder(metrics.New()), events: events}

	dispatches.BurnBudgetExhausted("escrow-1")
	events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "escrow stopped burning nonces at its budget", Fields: []any{
		"escrow", "escrow-1",
	}})
}

// A warmup vote reaches the ledger through RecordProbeTimeout, not RecordTimeout, so it never writes the race vote's own line.
func TestAFailedWarmupVoteWritesNoTimeoutVoteLine(t *testing.T) {
	logged := logcapture.Install(t)
	events := newTestJournal(t)
	recorder := metrics.NewRaceRecorder(metrics.New(), time.Now, func() time.Duration { return time.Hour })
	failedVote := engine.TimeoutEvent{
		EscrowID: "escrow-1", Participant: "gonka1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Model: "test-model",
		Nonce: 5, Kind: engine.TimeoutKindRefused, Action: engine.TimeoutActionFailed, Reason: "some_other_reason",
	}

	races := nonceAccountedRaces{recorder: recorder, events: events}
	races.RecordTimeout(failedVote)
	events.Flush()
	if _, found := logged.Find("timeout vote failed"); !found {
		t.Fatal("a failed race vote wrote no line -- this test cannot tell a written line from a missing one")
	}

	probes := probeVotes{recorder: recorder, events: events}
	probes.RecordTimeout(failedVote)
	events.Flush()

	if lines := logged.All(); len(lines) != 1 {
		t.Fatalf("lines after the warmup's own failed vote = %d, want 1: the warmup vote must reach RecordProbeTimeout, not the race's own line", len(lines))
	}
}
