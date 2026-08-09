package main

import (
	"testing"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/types"
)

func newLedgerForTest(t *testing.T) *nonceAccounting {
	t.Helper()
	service, err := accounting.NewService(accounting.Settings{
		Now: func() time.Time { return time.Unix(0, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	ledger := &nonceAccounting{service: service}
	t.Cleanup(func() { _ = ledger.service.Close() })
	if err := ledger.service.Book.OpenEscrow(accounting.EscrowMetadata{
		EscrowID: "escrow-1",
		Model:    "model-a",
		Slots: []types.SlotAssignment{
			{SlotID: 0, ValidatorAddress: "participant-0"},
			{SlotID: 1, ValidatorAddress: "participant-1"},
		},
	}); err != nil {
		t.Fatalf("OpenEscrow(): %v", err)
	}
	return ledger
}

func dispositionCount(t *testing.T, ledger *nonceAccounting, want accounting.Disposition) uint64 {
	t.Helper()
	var total uint64
	for _, record := range ledger.service.Book.Query(accounting.QueryFilter{}) {
		total += record.Dispositions[want]
	}
	return total
}

// The losing attempt of a won race did real work the client never received. Counting it as used would
// hide exactly the overscheduling this ledger exists to measure.
func TestALosingAttemptOfAWonRaceIsCountedAsUnused(t *testing.T) {
	ledger := newLedgerForTest(t)
	sent := time.Unix(100, 0)

	ledger.recordRace(engine.RaceOutcome{
		EscrowID:    "escrow-1",
		Succeeded:   true,
		WinnerNonce: 4,
		Attempts: []engine.AttemptOutcome{
			{Nonce: 4, SendTime: sent, NonceFinished: true, Terminal: engine.TerminalWon},
			{Nonce: 5, SendTime: sent, NonceFinished: true, Terminal: engine.TerminalLost},
		},
	})

	if used := dispositionCount(t, ledger, accounting.DispositionFinishedUsed); used != 1 {
		t.Fatalf("finished_used = %d, want the winner alone", used)
	}
	if unused := dispositionCount(t, ledger, accounting.DispositionFinishedUnused); unused != 1 {
		t.Fatalf("finished_unused = %d, want the losing attempt", unused)
	}
}

// A race nobody won leaves usage unknowable: the client may have been streamed part of an answer
// before everything failed, so claiming the work went unused would be a guess.
func TestAFinishedAttemptOfALostRaceIsCountedAsUnknown(t *testing.T) {
	ledger := newLedgerForTest(t)

	ledger.recordRace(engine.RaceOutcome{
		EscrowID:  "escrow-1",
		Succeeded: false,
		Attempts: []engine.AttemptOutcome{
			{Nonce: 4, SendTime: time.Unix(100, 0), NonceFinished: true, Terminal: engine.TerminalLost},
		},
	})

	if unknown := dispositionCount(t, ledger, accounting.DispositionFinishedUsageUnknown); unknown != 1 {
		t.Fatalf("finished_usage_unknown = %d, want the finished attempt", unknown)
	}
}

// A timeout arrives after its race and must name its escrow: a nonce is unique only within one.
func TestATimeoutClassifiesTheNonceItsRaceLeftPending(t *testing.T) {
	ledger := newLedgerForTest(t)

	ledger.recordRace(engine.RaceOutcome{
		EscrowID:  "escrow-1",
		Succeeded: false,
		Attempts:  []engine.AttemptOutcome{{Nonce: 4, SendTime: time.Unix(100, 0)}},
	})
	if pending := dispositionCount(t, ledger, accounting.DispositionUnfinishedExecution); pending != 0 {
		t.Fatalf("unfinished_execution = %d, want none before the timeout settles", pending)
	}

	ledger.recordTimeout(engine.TimeoutEvent{
		EscrowID: "escrow-1", Nonce: 4, Kind: "execution",
		Action: engine.TimeoutActionCompleted, Reason: "none",
	})

	if settled := dispositionCount(t, ledger, accounting.DispositionUnfinishedExecution); settled != 1 {
		t.Fatalf("unfinished_execution = %d, want the nonce classified by its timeout", settled)
	}
}

func TestABurnedNonceIsCountedAgainstTheSlotTheChainAssignsIt(t *testing.T) {
	ledger := newLedgerForTest(t)

	ledger.recordGhost("escrow-1", 5, "participant_throttled_no_send")

	// Two slots, so nonce 5 belongs to slot 1 and to nobody else.
	for _, record := range ledger.service.Book.Query(accounting.QueryFilter{}) {
		want := uint64(0)
		if record.Participant == "participant-1" {
			want = 1
		}
		if got := record.Dispositions[accounting.DispositionGhost]; got != want {
			t.Fatalf("%s ghosts = %d, want %d", record.Participant, got, want)
		}
	}
}

// Every emitter is called from a path that must not care whether accounting is configured.
func TestADisabledLedgerAcceptsEveryFactWithoutPanicking(t *testing.T) {
	var ledger *nonceAccounting

	ledger.recordGhost("escrow-1", 1, "poc_unavailable_host")
	ledger.recordRace(engine.RaceOutcome{EscrowID: "escrow-1"})
	ledger.recordTimeout(engine.TimeoutEvent{EscrowID: "escrow-1"})
	ledger.start(t.Context(), nil)
	if collectors := ledger.collectors(); collectors != nil {
		t.Fatalf("collectors() = %v, want none while disabled", collectors)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

// The ledger exports itself through the gateway's own metrics endpoint and serves nothing of its own.
func TestAnEnabledLedgerExportsItselfAndADisabledOneIsNotBuilt(t *testing.T) {
	ledger := openNonceAccounting(
		config.NonceAccounting{Enabled: true, SnapshotSeconds: 300},
		t.TempDir(), nil, func() time.Time { return time.Unix(0, 0).UTC() },
	)
	if ledger == nil {
		t.Fatal("openNonceAccounting() returned nothing for an enabled ledger")
	}
	t.Cleanup(func() { _ = ledger.Close() })
	if collectors := ledger.collectors(); len(collectors) != 1 {
		t.Fatalf("collectors() = %d, want the ledger exported", len(collectors))
	}

	disabled := openNonceAccounting(
		config.NonceAccounting{SnapshotSeconds: 300},
		t.TempDir(), nil, func() time.Time { return time.Unix(0, 0).UTC() },
	)
	if disabled != nil {
		t.Fatal("openNonceAccounting() built a disabled ledger")
	}
}

func counterCount(t *testing.T, ledger *nonceAccounting, match func(accounting.CounterKey) bool) uint64 {
	t.Helper()
	var total uint64
	for _, record := range ledger.service.Book.Query(accounting.QueryFilter{}) {
		for _, counter := range record.Counters {
			if match(counter.CounterKey) {
				total += counter.Count
			}
		}
	}
	return total
}

// Decode speed is derived here, from stamps the engine reports, so nothing but a race driven end to end
// proves the derivation is wired to the ledger at all.
func TestASlowDecodeReachesTheLedgerAsAFactAboutTheHost(t *testing.T) {
	sent := time.Unix(100, 0)
	firstContent := sent.Add(time.Second)

	cases := []struct {
		name      string
		lastChunk time.Time
		tokens    int64
		want      uint64
	}{
		{
			name:      "a hundred tokens over ten seconds is a hundred milliseconds each",
			lastChunk: firstContent.Add(10 * time.Second), tokens: 100, want: 1,
		},
		{
			name:      "the same tokens over one second is ten milliseconds each",
			lastChunk: firstContent.Add(time.Second), tokens: 100,
		},
		{name: "a host that reported no tokens is not judged", lastChunk: firstContent.Add(time.Minute)},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ledger := newLedgerForTest(t)

			ledger.recordRace(engine.RaceOutcome{
				EscrowID: "escrow-1", Succeeded: true, WinnerNonce: 4,
				Attempts: []engine.AttemptOutcome{{
					Nonce: 4, SendTime: sent, NonceFinished: true, Terminal: engine.TerminalWon,
					FirstContent: firstContent, LastChunk: testCase.lastChunk,
					UsageCompletionTokens: testCase.tokens,
				}},
			})

			slow := counterCount(t, ledger, func(key accounting.CounterKey) bool { return key.SlowDecode })
			if slow != testCase.want {
				t.Fatalf("counters marked slow_decode = %d, want %d", slow, testCase.want)
			}
		})
	}
}
