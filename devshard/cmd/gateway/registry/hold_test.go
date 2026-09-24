package registry

import (
	"context"
	"testing"
	"time"

	"devshard/types"
	"devshard/user"
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

// Test flow:
//  1. Build a registry with one escrow and put it on hold.
//  2. Assert `Candidates` returns none for that model.
//  3. Assert `Routable` reports the escrow as not routable.
//  4. Assert `OnHold` reports true.
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

// Test flow:
//  1. Build a registry with one escrow and put it on hold.
//  2. Acquire and release the escrow, asserting the acquire succeeds.
//  3. Assert `SettlementSession` and `ResumeCandidate` both still find the escrow.
//  4. Assert the snapshot reports one escrow with OnHold true.
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
	if _, held := registry.ResumeCandidate("5"); !held {
		t.Error("ResumeCandidate refused an escrow on hold")
	}
	states := registry.Snapshot()
	if len(states) != 1 || !states[0].OnHold {
		t.Errorf("Snapshot = %+v, want one escrow reported on hold", states)
	}
}

// Test flow:
//  1. Build a registry with one escrow on hold, then acquire it so a request is in flight.
//  2. Retire the escrow while the request is still running.
//  3. Assert the snapshot reports one draining escrow: not accepting, with one request in flight.
//  4. Assert that draining escrow no longer reports OnHold.
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

// Test flow:
//  1. Build a registry with one escrow, put it on hold, then take it off hold.
//  2. Assert `Candidates` reports the escrow again after resuming.
func TestResumingPutsTheEscrowBackAmongTheCandidates(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)
	registry.SetOnHold("5", true)

	registry.SetOnHold("5", false)

	if candidates := registry.Candidates("qwen"); len(candidates) != 1 {
		t.Errorf("Candidates = %d, want 1 after resuming", len(candidates))
	}
}

// Test flow:
//  1. Build a registry and add one escrow directly via `AddOnHold`.
//  2. Assert `Candidates` reports none: a row added on hold must not route after a restart.
//  3. Assert `OnHold` reports true.
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

// Test flow:
//  1. Build a registry with one escrow.
//  2. Call `SetOnHold` for an escrow ID that is not live.
//  3. Assert `OnHold` for that unknown ID still reports false.
func TestSettingTheHoldOnAnUnknownEscrowDoesNothing(t *testing.T) {
	t.Parallel()
	registry := holdRegistry(t)

	registry.SetOnHold("404", true)

	if registry.OnHold("404") {
		t.Error("OnHold(404) = true for an escrow that is not live")
	}
}

// Test flow:
//  1. Build a registry with one escrow whose state carries a balance and four inferences: pending, started, challenged, and finished.
//  2. Read `Funds` for that escrow.
//  3. Assert reserved is 30 (the pending and started reserved costs) and challenged is 30 (the challenged inference's actual cost).
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

// Test flow:
//  1. Publish an escrow whose session holds in-flight records, varying their statuses and timestamps.
//  2. Ask when every reservation could have come back.
//  3. Assert a pending record counts from its start plus the refusal window, a finished one not at all, and a started or disputed one leaves the answer unbounded.
func TestReservationsReturnByWaitsOnlyForWhatTheRefusalWindowReturns(t *testing.T) {
	config := types.SessionConfig{RefusalTimeout: 60, ExecutionTimeout: 1800}
	refusal := 60*time.Second + user.TimeoutBuffer
	cases := []struct {
		name         string
		inferences   map[uint64]*types.InferenceRecord
		wantReturnBy time.Time
		wantBounded  bool
	}{
		{name: "nothing in flight", inferences: nil, wantReturnBy: time.Time{}, wantBounded: true},
		{name: "the latest pending record", inferences: map[uint64]*types.InferenceRecord{
			1: {Status: types.StatusPending, StartedAt: 1000},
			2: {Status: types.StatusPending, StartedAt: 1500},
		}, wantReturnBy: time.Unix(1500, 0).Add(refusal), wantBounded: true},
		{name: "a finished record is not waited for", inferences: map[uint64]*types.InferenceRecord{1: {Status: types.StatusFinished, StartedAt: 9000, ConfirmedAt: 9000}}, wantReturnBy: time.Time{}, wantBounded: true},
		{name: "a started record is left to the sweep", inferences: map[uint64]*types.InferenceRecord{
			1: {Status: types.StatusPending, StartedAt: 1500},
			2: {Status: types.StatusStarted, StartedAt: 900, ConfirmedAt: 1000},
		}, wantBounded: false},
		{name: "a disputed record has no deadline", inferences: map[uint64]*types.InferenceRecord{1: {Status: types.StatusChallenged}}, wantBounded: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			session := newFakeSession("hostA")
			session.escrowState = types.EscrowState{Config: config, Inferences: testCase.inferences}
			registry := New(Deps{ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open, Now: fixedClock()})
			mustAdd(t, registry, "1", "qwen")

			returnBy, bounded := registry.ReservationsReturnBy("1")

			if bounded != testCase.wantBounded {
				t.Fatalf("ReservationsReturnBy(1) bounded = %t, want %t", bounded, testCase.wantBounded)
			}
			if bounded && !returnBy.Equal(testCase.wantReturnBy) {
				t.Fatalf("ReservationsReturnBy(1) = %v, want %v", returnBy, testCase.wantReturnBy)
			}
		})
	}
}

// Test flow:
//  1. Build a registry that publishes no escrow.
//  2. Ask when an unknown escrow's reservations could have come back.
//  3. Assert the answer is unbounded, so no hold expires on an escrow the registry does not hold.
func TestReservationsReturnByIsUnboundedForAnUnknownEscrow(t *testing.T) {
	registry := New(Deps{ServingSessions: newSessions(map[string]*fakeSession{}).open, Now: fixedClock()})

	if _, bounded := registry.ReservationsReturnBy("missing"); bounded {
		t.Fatal("ReservationsReturnBy(missing) bounded = true, want false")
	}
}
