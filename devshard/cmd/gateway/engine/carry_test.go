package engine

import (
	"bytes"
	"strconv"
	"sync"
	"testing"

	"devshard/cmd/gateway/config"
)

func testBudget(attempt, participant, global int64) *carryBudget {
	return newCarryBudget(config.Stream{
		ClassifyMaxAttemptBytes:     attempt,
		ClassifyMaxParticipantBytes: participant,
		ClassifyMaxGlobalBytes:      global,
	})
}

func assertUsage(t *testing.T, budget *carryBudget, participant string, wantParticipant, wantGlobal int64) {
	t.Helper()
	participantBytes, globalBytes := budget.counterFor(participant).Load(), budget.global.Load()
	if participantBytes != wantParticipant || globalBytes != wantGlobal {
		t.Fatalf("usage(%q) = (%d, %d), want (%d, %d)", participant, participantBytes, globalBytes, wantParticipant, wantGlobal)
	}
}

func TestCarryBudgetGlobalTripRollsBackParticipantCharge(t *testing.T) {
	budget := testBudget(1<<20, 1<<20, 32)
	counter := budget.counterFor("alice")

	if !budget.reserve(counter, 24) {
		t.Fatal("first reserve rejected")
	}
	assertUsage(t, budget, "alice", 24, 24)

	if budget.reserve(counter, 16) {
		t.Fatal("reserve past the global cap was accepted")
	}
	assertUsage(t, budget, "alice", 24, 24)
}

func TestCarryBudgetParticipantTripLeavesGlobalUntouched(t *testing.T) {
	budget := testBudget(1<<20, 32, 1<<20)
	alice, bob := budget.counterFor("alice"), budget.counterFor("bob")

	if !budget.reserve(alice, 24) {
		t.Fatal("first reserve rejected")
	}
	if budget.reserve(alice, 16) {
		t.Fatal("reserve past the participant cap was accepted")
	}
	assertUsage(t, budget, "alice", 24, 24)

	if !budget.reserve(bob, 24) {
		t.Fatal("a second participant was starved by the first's cap")
	}
	assertUsage(t, budget, "bob", 24, 48)
}

func TestCarryBufferReassemblesAcrossChunks(t *testing.T) {
	budget := testBudget(1<<20, 1<<20, 1<<20)
	buffer := newCarryBuffer(budget, "alice")

	if parseable, dropped := buffer.Take([]byte(`data: {"x":1}`)); parseable != nil || dropped {
		t.Fatalf("Take(head) = (%q, %v), want (nil, false)", parseable, dropped)
	}
	assertUsage(t, budget, "alice", 13, 13)

	parseable, dropped := buffer.Take([]byte("\n\ndata: par"))
	if dropped {
		t.Fatal("Take(tail) reported a drop")
	}
	if want := "data: {\"x\":1}\n\n"; string(parseable) != want {
		t.Fatalf("parseable = %q, want %q", parseable, want)
	}
	if want := "data: par"; string(buffer.Tail()) != want {
		t.Fatalf("Tail() = %q, want %q", buffer.Tail(), want)
	}
	assertUsage(t, budget, "alice", 9, 9)

	buffer.Release()
	assertUsage(t, budget, "alice", 0, 0)
	buffer.Release()
	assertUsage(t, budget, "alice", 0, 0)
}

func TestCarryBufferCapsTripIndependently(t *testing.T) {
	oversized := bytes.Repeat([]byte("x"), 24)
	tests := []struct {
		name   string
		budget *carryBudget
	}{
		{name: "attempt", budget: testBudget(8, 1<<20, 1<<20)},
		{name: "participant", budget: testBudget(1<<20, 8, 1<<20)},
		{name: "global", budget: testBudget(1<<20, 1<<20, 8)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			buffer := newCarryBuffer(testCase.budget, "alice")

			parseable, dropped := buffer.Take(oversized)
			if !dropped {
				t.Fatal("cap trip was not reported")
			}
			if !bytes.Equal(parseable, oversized) {
				t.Fatalf("parseable = %q, want the raw chunk", parseable)
			}
			assertUsage(t, testCase.budget, "alice", 0, 0)

			if _, dropped := buffer.Take(oversized); dropped {
				t.Fatal("a second cap trip was reported for the same attempt")
			}

			parseable, dropped = buffer.Take([]byte("ok\n"))
			if dropped {
				t.Fatal("reassembly did not resume after the trip")
			}
			if string(parseable) != "ok\n" {
				t.Fatalf("parseable = %q, want %q", parseable, "ok\n")
			}
		})
	}
}

func TestCarryBufferRetainedTailShrinkRefunds(t *testing.T) {
	budget := testBudget(1<<20, 1<<20, 1<<20)
	buffer := newCarryBuffer(budget, "alice")

	if _, dropped := buffer.Take([]byte("0123456789")); dropped {
		t.Fatal("Take(head) reported a drop")
	}
	assertUsage(t, budget, "alice", 10, 10)

	if _, dropped := buffer.Take([]byte("\nab")); dropped {
		t.Fatal("Take(tail) reported a drop")
	}
	assertUsage(t, budget, "alice", 2, 2)
}

func TestCarryBudgetConcurrentAttemptsSettleToZero(t *testing.T) {
	budget := testBudget(1<<10, 1<<12, 1<<14)
	var attempts sync.WaitGroup
	for worker := range 16 {
		attempts.Go(func() {
			buffer := newCarryBuffer(budget, "participant-"+strconv.Itoa(worker%4))
			defer buffer.Release()
			for round := range 64 {
				buffer.Take(bytes.Repeat([]byte("x"), round))
				buffer.Take([]byte("payload\n\ntail"))
			}
		})
	}
	attempts.Wait()

	for participant := range 4 {
		assertUsage(t, budget, "participant-"+strconv.Itoa(participant), 0, 0)
	}
}
