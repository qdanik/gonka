package registry

import (
	"context"
	"testing"

	"devshard/types"
)

func holdRegistry(t *testing.T) *Registry {
	t.Helper()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"5": settleableSession(9)}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "5", "qwen")
	return registry
}

func TestAnEscrowOnHoldIsNoCandidate(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)

	registry.SetOnHold("5", true)

	if candidates := registry.Candidates("qwen"); len(candidates) != 0 {
		t.Errorf("Candidates = %d, want 0 while on hold", len(candidates))
	}
	if _, routable := registry.Routable("5"); routable {
		t.Error("Routable = true, want false while on hold")
	}
	if !registry.OnHold("5") {
		t.Error("OnHold = false, want true")
	}
}

func TestAnEscrowOnHoldStaysLiveForEverythingButRouting(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)
	registry.SetOnHold("5", true)

	session, release, held := registry.Acquire("5")
	if !held || session == nil {
		t.Fatal("Acquire refused an escrow on hold; the timeout sweep would skip it")
	}
	release()
	if _, held := registry.SettlementSession("5"); !held {
		t.Error("SettlementSession refused an escrow on hold; owed votes could not post")
	}
	if _, held := registry.Held("5"); !held {
		t.Error("Held refused an escrow on hold")
	}
	states := registry.Snapshot()
	if len(states) != 1 || !states[0].OnHold {
		t.Errorf("Snapshot = %+v, want one escrow reported on hold", states)
	}
}

func TestADrainingEscrowIsNotReportedOnHold(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)
	registry.SetOnHold("5", true)
	_, release, held := registry.Acquire("5")
	if !held {
		t.Fatal("Acquire refused an escrow on hold")
	}
	defer release()

	if err := registry.Retire("5"); err != nil {
		t.Fatalf("Retire = %v, want nil", err)
	}

	states := registry.Snapshot()
	if len(states) != 1 || states[0].Accepting || states[0].InFlight != 1 {
		t.Fatalf("Snapshot = %+v, want one draining escrow with its request in flight", states)
	}
	if states[0].OnHold {
		t.Error("Snapshot reports a draining escrow on hold; the on-hold gauge would stay 1 until it drains")
	}
}

func TestResumingPutsTheEscrowBackAmongTheCandidates(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)
	registry.SetOnHold("5", true)

	registry.SetOnHold("5", false)

	if candidates := registry.Candidates("qwen"); len(candidates) != 1 {
		t.Errorf("Candidates = %d, want 1 after resuming", len(candidates))
	}
}

func TestAddOnHoldPublishesTheEscrowOnHold(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"5": settleableSession(9)}).open,
		Now:             fixedClock(),
	})

	if err := registry.AddOnHold(context.Background(), "5", "qwen"); err != nil {
		t.Fatalf("AddOnHold = %v, want nil", err)
	}

	if candidates := registry.Candidates("qwen"); len(candidates) != 0 {
		t.Errorf("Candidates = %d, want 0: a row on hold must not route after a restart", len(candidates))
	}
	if !registry.OnHold("5") {
		t.Error("OnHold = false, want true")
	}
}

func TestSettingTheHoldOnAnUnknownEscrowDoesNothing(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)

	registry.SetOnHold("404", true)

	if registry.OnHold("404") {
		t.Error("OnHold(404) = true for an escrow that is not live")
	}
}

func TestFundsSplitsWhatTheEscrowHolds(t *testing.T) {
	t.Parallel()
	session := settleableSession(9)
	session.escrowState.Balance = 100
	session.escrowState.Inferences = map[uint64]*types.InferenceRecord{
		1: {Status: types.StatusPending, ReservedCost: 10},
		2: {Status: types.StatusStarted, ReservedCost: 20},
		3: {Status: types.StatusChallenged, ReservedCost: 50, ActualCost: 30},
		4: {Status: types.StatusFinished, ReservedCost: 70, ActualCost: 40},
	}
	registry := New(Deps{ServingSessions: newSessions(map[string]*fakeSession{"5": session}).open, Now: fixedClock()})
	mustAdd(t, registry, "5", "qwen")

	balance, reserved, challenged, known := registry.Funds("5")

	if !known || reserved != 30 || challenged != 30 {
		t.Fatalf("Funds = %d, %d, %d, %v; want reserved 30, challenged 30", balance, reserved, challenged, known)
	}
}
