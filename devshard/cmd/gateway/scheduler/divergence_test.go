package scheduler

import (
	"testing"
	"time"
)

func TestReplayCreditIsSpentOncePerParticipantAndEscrow(t *testing.T) {
	var credit replayCredit
	now := time.Unix(1_700_000_000, 0)

	if !credit.spend("escrow-1", "host-a", now) {
		t.Fatal("the first divergence must find the replay unspent")
	}
	if credit.spend("escrow-1", "host-a", now.Add(time.Second)) {
		t.Error("the second divergence must find it spent")
	}
	if !credit.spend("escrow-1", "host-b", now) {
		t.Error("another participant keeps its own replay")
	}
	if !credit.spend("escrow-2", "host-a", now) {
		t.Error("the same participant keeps a replay on another escrow")
	}
}

// Requests to one participant overlap, so a send that started before the rewind never exercised the
// replayed state and must not hand the credit back.
func TestOnlyASendStartedAfterTheRewindRestoresTheReplay(t *testing.T) {
	rewoundAt := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name    string
		sentAt  time.Time
		restore bool
	}{
		{"a send started after the rewind", rewoundAt.Add(time.Second), true},
		{"a send already in flight when it happened", rewoundAt.Add(-time.Second), false},
		{"a send started in the same instant", rewoundAt, false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var credit replayCredit
			credit.spend("escrow-1", "host-a", rewoundAt)

			credit.restore("escrow-1", "host-a", testCase.sentAt)

			if got := credit.spend("escrow-1", "host-a", rewoundAt.Add(time.Hour)); got != testCase.restore {
				t.Errorf("replay available again = %v, want %v", got, testCase.restore)
			}
		})
	}
}

func TestForgettingAnEscrowDropsItsCredits(t *testing.T) {
	var credit replayCredit
	now := time.Unix(1_700_000_000, 0)
	credit.spend("escrow-1", "host-a", now)

	credit.forget("escrow-1")

	if !credit.spend("escrow-1", "host-a", now) {
		t.Error("a retired escrow must leave no credit behind")
	}
}
