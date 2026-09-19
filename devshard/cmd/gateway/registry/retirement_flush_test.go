package registry

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"devshard/types"
)

func sessionHoldingGossip(pending int) *fakeSession {
	session := newFakeSession("hostA")
	for range pending {
		session.pendingTxs = append(session.pendingTxs, &types.DevshardTx{})
	}
	return session
}

// A last diff costs a nonce; an escrow holding nothing gossiped must never be asked to spend one.
func TestAnEscrowWithNothingPendingRetiresWithoutADiff(t *testing.T) {
	t.Parallel()
	session := sessionHoldingGossip(0)
	narrator := &recordingEscrowNarrator{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Membership:      newRecordingMembership(),
		Narrator:        narrator,
		Now:             fixedClock(),
	})
	t.Cleanup(func() { registry.Close() })
	if err := registry.Add(context.Background(), "1", "model-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(): %v", err)
	}

	if calls := session.pendingDiffCalls.Load(); calls != 0 {
		t.Fatalf("pending diffs sent = %d, want none for an escrow holding nothing", calls)
	}
	if slices.ContainsFunc(narrator.recorded(), func(line string) bool { return strings.HasPrefix(line, "flushed") }) {
		t.Fatalf("narrated %v, want nothing said about a diff that was never sent", narrator.recorded())
	}
}

// The gossiped transactions reach a diff while the session can still send one, and before the ledger
// takes the reading it will never take again.
func TestARetiringEscrowCarriesItsGossipBeforeTheLedgerReadsIt(t *testing.T) {
	t.Parallel()
	session := sessionHoldingGossip(3)
	var order []string
	session.onPendingDiff = func() { order = append(order, "diff") }
	narrator := &recordingEscrowNarrator{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Membership:      newRecordingMembership(),
		Narrator:        narrator,
		Retiring:        func(string, EscrowSession) { order = append(order, "ledger") },
		Now:             fixedClock(),
	})
	t.Cleanup(func() { registry.Close() })
	if err := registry.Add(context.Background(), "1", "model-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(): %v", err)
	}

	if calls := session.pendingDiffCalls.Load(); calls != 1 {
		t.Fatalf("pending diffs sent = %d, want exactly one last diff", calls)
	}
	assertSameOrder(t, order, []string{"diff", "ledger"})
	assertNarrated(t, narrator, "flushed 1 pending 3 error <nil>")
}

// A host that will not take the last diff loses the gossip, not the retirement: the session still closes.
func TestAFailedLastDiffIsNarratedAndTheEscrowStillCloses(t *testing.T) {
	t.Parallel()
	session := sessionHoldingGossip(2)
	session.pendingDiffErr = errors.New("host unreachable")
	narrator := &recordingEscrowNarrator{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Membership:      newRecordingMembership(),
		Narrator:        narrator,
		Now:             fixedClock(),
	})
	t.Cleanup(func() { registry.Close() })
	if err := registry.Add(context.Background(), "1", "model-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(): %v", err)
	}

	if closes := session.closeCalls.Load(); closes != 1 {
		t.Fatalf("session closes = %d, want the retirement to finish despite the failed diff", closes)
	}
	assertNarrated(t, narrator, "flushed 1 pending 2 error host unreachable")
}

func assertSameOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func assertNarrated(t *testing.T, narrator *recordingEscrowNarrator, want string) {
	t.Helper()
	if !slices.Contains(narrator.recorded(), want) {
		t.Fatalf("narrated %v, want %q among them", narrator.recorded(), want)
	}
}
