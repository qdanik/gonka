package main

import (
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
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
		Nonce: 5, Participant: "gonka1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Reason: "participant_window_full_no_send",
	})
	events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{
		"escrow", "escrow-1", "nonce", uint64(5), "host", "bbbbbbbb", "reason", "participant_window_full_no_send",
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

// The line is the only record that a request's exclusion lapsed, so its escrow and host must not trade places.
func TestANonceSpentOnAnExcludedHostNamesItsEscrowAndHost(t *testing.T) {
	logged := logcapture.Install(t)
	events := newTestJournal(t)
	dispatches := tracedDispatches{recorder: metrics.NewDispatchRecorder(metrics.New()), events: events}

	dispatches.ExcludedHostServed("escrow-1", "gonka1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "info", Msg: "nonce spent on a host the request excluded", Fields: []any{
		"escrow", "escrow-1", "host", "bbbbbbbb",
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

// phaseNarrator decides what moved and the journal writes it; a first poll that read no epoch announces none.
func TestThePhaseNarratorHandsItsChangesToTheJournal(t *testing.T) {
	logged := logcapture.Install(t)
	events := newTestJournal(t)
	narrator := &phaseNarrator{events: events}

	narrator.observe(chain.PhaseSnapshot{LastError: "fetch epoch info: 503"})
	narrator.observe(chain.PhaseSnapshot{EpochIndex: 7, EpochPhase: chain.EpochPhasePoCGenerate, BlockHeight: 100, RequestsBlocked: true, BlockReason: chain.BlockReasonPoC})
	narrator.observe(chain.PhaseSnapshot{EpochIndex: 7, EpochPhase: chain.EpochPhasePoCGenerate, BlockHeight: 120})
	events.Flush()

	logged.RequireLine(t, logcapture.Entry{Level: "info", Msg: "chain epoch", Fields: []any{
		"epoch", uint64(7), "phase", chain.EpochPhasePoCGenerate, "height", int64(100), "switch_height", int64(0),
	}})
	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "chain blocked requests", Fields: []any{
		"reason", chain.BlockReasonPoC, "epoch", uint64(7), "height", int64(100),
	}})
	logged.RequireLine(t, logcapture.Entry{Level: "info", Msg: "chain unblocked requests", Fields: []any{
		"epoch", uint64(7), "height", int64(120),
	}})
	if recorded := logged.All(); len(recorded) != 3 {
		t.Fatalf("recorded %+v, want exactly three chain lines: the epoch-less first snapshot announces no epoch", recorded)
	}
}
