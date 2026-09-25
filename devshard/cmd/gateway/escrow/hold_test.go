package escrow

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
)

type fakeHoldGate struct {
	mu       sync.Mutex
	onHold   map[string]bool
	verdicts map[string]HoldVerdict
	returnBy map[string]time.Time
}

func newFakeHoldGate() *fakeHoldGate {
	return &fakeHoldGate{onHold: map[string]bool{}, verdicts: map[string]HoldVerdict{}, returnBy: map[string]time.Time{}}
}

func (g *fakeHoldGate) ReservationsReturnBy(escrowID string) (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	returnBy, bounded := g.returnBy[escrowID]
	return returnBy, bounded
}

func (g *fakeHoldGate) SetOnHold(escrowID string, onHold bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onHold[escrowID] = onHold
}

func (g *fakeHoldGate) Verdict(escrowID string, _ uint64) HoldVerdict {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.verdicts[escrowID]
}

func (g *fakeHoldGate) Funds(string) (uint64, uint64, uint64, bool) { return 50, 400, 300, true }

func (g *fakeHoldGate) isOnHold(escrowID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.onHold[escrowID]
}

func holdManager(t *testing.T, testStore *fakeStore, txClient *fakeTxClient, gate *fakeHoldGate) *Manager {
	t.Helper()
	manager := depletionManager(t, testStore, txClient)
	manager.holds = gate
	manager.config = holderWithHold(t, true)
	return manager
}

func currentEpochRecord(id string) store.DevshardRecord {
	record := activeRecord(id, "model-a")
	record.RotationEpoch = int64(servingSnapshot().EpochIndex)
	return record
}

func depleteOnce(t *testing.T, manager *Manager, testStore *fakeStore, escrowID string, reason scheduler.ExhaustionReason) error {
	t.Helper()
	manager.OnBalanceExhausted(escrowID, reason)
	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards: %v", err)
	}
	return manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards)
}

// Test flow:
//  1. Store one active record at the current epoch and build a manager with the hold enabled and a working createEscrowFn.
//  2. Deplete the record once with reason "balance_floor" via `depleteOnce`.
//  3. Assert the record stays active, goes on hold, and is not marked for settlement.
//  4. Assert the hold gate was told the escrow is on hold.
//  5. Assert one replacement escrow was created.
func TestABalanceDepletedEscrowGoesOnHoldAndIsReplaced(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = currentEpochRecord("1")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	gate := newFakeHoldGate()
	manager := holdManager(t, testStore, txClient, gate)

	if err := depleteOnce(t, manager, testStore, "1", "balance_floor"); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	record := testStore.devshards["1"]
	if !record.Active || !record.OnHold || record.SettlementPending {
		t.Fatalf("record = %+v, want active, on hold, not parked", record)
	}
	if !gate.isOnHold("1") {
		t.Error("registry was not told the escrow is on hold")
	}
	if txClient.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 replacement", txClient.createCalls)
	}
}

// Test flow:
//  1. Store one active record and build a manager with the hold enabled and a working createEscrowFn.
//  2. Deplete the record once with reason `scheduler.ExhaustionNonceCap` via `depleteOnce`.
//  3. Assert the record is parked, not held, even though holding is enabled.
//  4. Assert one replacement escrow was created since the model is now short.
func TestANonceCappedEscrowIsParkedEvenWithTheHoldOn(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := holdManager(t, testStore, txClient, newFakeHoldGate())

	if err := depleteOnce(t, manager, testStore, "1", scheduler.ExhaustionNonceCap); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if txClient.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1: the model is short once the escrow leaves", txClient.createCalls)
	}
}

// Test flow:
//  1. Store one active record and build a manager with the hold enabled.
//  2. Mark the record balance-exhausted with reason "balance_floor", then deplete it again via `depleteOnce` with reason `scheduler.ExhaustionNonceCap`.
//  3. Assert the record is parked, showing the nonce-cap reason wins over the earlier balance reason in the same tick.
func TestNonceCapWinsOverABalanceReasonInTheSameTick(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	manager := holdManager(t, testStore, &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}, newFakeHoldGate())
	manager.OnBalanceExhausted("1", "balance_floor")

	if err := depleteOnce(t, manager, testStore, "1", scheduler.ExhaustionNonceCap); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
}

// Test flow:
//  1. Store one record at the current epoch and a second active record, both for the same model, and build a manager with the hold enabled.
//  2. Deplete the first record with reason "balance_floor" via `depleteOnce`.
//  3. Assert no replacement escrow was created, since the second record still meets the target of 1.
//  4. Assert the first record is on hold.
func TestAResumedEscrowThatDepletesAgainGetsNoSecondReplacement(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = currentEpochRecord("1")
	testStore.devshards["2"] = activeRecord("2", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := holdManager(t, testStore, txClient, newFakeHoldGate())

	if err := depleteOnce(t, manager, testStore, "1", "balance_floor"); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	if txClient.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: escrow 2 still meets the target of 1", txClient.createCalls)
	}
	if !testStore.devshards["1"].OnHold {
		t.Error("escrow 1 is not on hold")
	}
}

// Test flow:
//  1. Store one record already on hold and a second active record, both for the same model, and build a manager with the hold enabled.
//  2. Deplete the second record with reason "balance_floor" via `depleteOnce`.
//  3. Assert the second record is parked, since the model already has one escrow on hold at the hold cap.
func TestPastTheCapADepletedEscrowIsParked(t *testing.T) {
	testStore := newFakeStore()
	held := activeRecord("1", "model-a")
	held.OnHold = true
	testStore.devshards["1"] = held
	testStore.devshards["2"] = activeRecord("2", "model-a")
	manager := holdManager(t, testStore, &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}, newFakeHoldGate())

	if err := depleteOnce(t, manager, testStore, "2", "balance_floor"); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	assertParked(t, testStore, "2")
}

// Test flow:
//  1. Store one record already on hold and build a manager with the hold enabled and a working createEscrowFn.
//  2. Deplete the record with reason "insufficient_balance" via `depleteOnce`.
//  3. Assert the record stays active and on hold, unchanged.
//  4. Assert no replacement escrow was created.
func TestAnEscrowAlreadyOnHoldIsLeftAlone(t *testing.T) {
	testStore := newFakeStore()
	held := activeRecord("1", "model-a")
	held.OnHold = true
	testStore.devshards["1"] = held
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := holdManager(t, testStore, txClient, newFakeHoldGate())

	if err := depleteOnce(t, manager, testStore, "1", "insufficient_balance"); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	if record := testStore.devshards["1"]; !record.Active || !record.OnHold {
		t.Fatalf("record = %+v, want still on hold", record)
	}
	if txClient.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0", txClient.createCalls)
	}
}

// Test flow:
//  1. Store two active records for the same model and build a manager with the hold disabled.
//  2. Deplete the first record with reason "balance_floor" via `depleteOnce`.
//  3. Assert the first record is parked.
//  4. Assert one replacement escrow was created, matching the pre-hold behavior of replacing every depletion.
func TestWithTheHoldOffDepletionParksAsBefore(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	testStore.devshards["2"] = activeRecord("2", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := holdManager(t, testStore, txClient, newFakeHoldGate())
	manager.config = holderWithHold(t, false)

	if err := depleteOnce(t, manager, testStore, "1", "balance_floor"); err != nil {
		t.Fatalf("checkDepletion = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if txClient.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1: with the hold off every depletion is replaced, as today", txClient.createCalls)
	}
}

// Test flow:
//  1. Build one record on hold and one serving record, both for model-a at epoch 3.
//  2. Call countActive for model-a, roleRegular, epoch 3.
//  3. Assert it counts only the serving record, since an escrow on hold is not part of the serving set.
func TestTheBridgeCountsOnlyServingEscrows(t *testing.T) {
	held := activeRecord("1", "model-a")
	held.OnHold = true
	held.RotationEpoch = 3
	serving := activeRecord("2", "model-a")
	serving.RotationEpoch = 3

	if got := countActive([]store.DevshardRecord{held, serving}, "model-a", roleRegular, 3); got != 1 {
		t.Fatalf("countActive = %d, want 1: an escrow on hold is not part of the serving set", got)
	}
}

func heldRecord(id string, epoch int64) store.DevshardRecord {
	record := activeRecord(id, "model-a")
	record.OnHold = true
	record.RotationEpoch = epoch
	return record
}

func resumeTick(t *testing.T, manager *Manager, testStore *fakeStore) ([]store.DevshardRecord, error) {
	t.Helper()
	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards: %v", err)
	}
	return manager.resumeHeld(context.Background(), servingSnapshot(), depletionModels(), devshards)
}

func findRecord(t *testing.T, devshards []store.DevshardRecord, escrowID string) store.DevshardRecord {
	t.Helper()
	for _, record := range devshards {
		if record.EscrowID == escrowID {
			return record
		}
	}
	t.Fatalf("escrow %s is missing from the returned devshards", escrowID)
	return store.DevshardRecord{}
}

// Test flow:
//  1. Store a held record at the current epoch, and set the hold gate to report it on hold with a `HoldResume` verdict.
//  2. Call `resumeTick`.
//  3. Assert resumeTick returns no error.
//  4. Assert the stored record is now active and off hold, the hold gate no longer reports it on hold, and the returned devshards slice reflects the same resumed state.
func TestAnEscrowWhoseMoneyCameBackResumes(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	gate := newFakeHoldGate()
	gate.onHold["1"] = true
	gate.verdicts["1"] = HoldResume
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)

	devshards, err := resumeTick(t, manager, testStore)
	if err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	if record := testStore.devshards["1"]; !record.Active || record.OnHold {
		t.Fatalf("record = %+v, want serving", record)
	}
	if gate.isOnHold("1") {
		t.Error("registry still has the escrow on hold")
	}
	if record := findRecord(t, devshards, "1"); !record.Active || record.OnHold {
		t.Errorf("returned record = %+v, want serving", record)
	}
}

// Test flow:
//  1. Put a current-epoch escrow on hold whose balance is still short, varying when its reservations could all have come back and whether any is left to the sweep or a dispute.
//  2. Run one resume tick at a clock of zero.
//  3. Assert the hold expires only once every reservation could have come back and a tick has passed, narrated once with the money still held, and never while a reservation has no deadline.
func TestAHoldExpiresOnceItsReservationsCouldAllHaveComeBack(t *testing.T) {
	now := time.Unix(0, 0)
	cases := []struct {
		name        string
		returnBy    time.Time
		bounded     bool
		wantExpired bool
	}{
		{name: "every reservation was due a tick ago", returnBy: now.Add(-TickInterval), bounded: true, wantExpired: true},
		{name: "nothing is in flight", returnBy: time.Time{}, bounded: true, wantExpired: true},
		{name: "the last reservation is due now", returnBy: now, bounded: true, wantExpired: false},
		{name: "a reservation is still in its window", returnBy: now.Add(time.Minute), bounded: true, wantExpired: false},
		{name: "a reservation is left to the sweep or a dispute", bounded: false, wantExpired: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testStore := newFakeStore()
			testStore.devshards["1"] = heldRecord("1", int64(servingSnapshot().EpochIndex))
			gate := newFakeHoldGate()
			gate.onHold["1"] = true
			gate.verdicts["1"] = HoldKeep
			if testCase.bounded {
				gate.returnBy["1"] = testCase.returnBy
			}
			manager := holdManager(t, testStore, &fakeTxClient{}, gate)
			narrator := &recordingLifecycleNarrator{}
			manager.narrator = narrator

			if _, err := resumeTick(t, manager, testStore); err != nil {
				t.Fatalf("resumeHeld = %v, want nil", err)
			}

			if !testCase.wantExpired {
				if record := testStore.devshards["1"]; !record.Active || !record.OnHold {
					t.Fatalf("record = %+v, want still on hold", record)
				}
				return
			}
			assertParked(t, testStore, "1")
			if !slices.Contains(narrator.recorded(), "hold expired 1: balance 50 reserved 400") {
				t.Errorf("narrated %v, want what was still held when the hold expired", narrator.recorded())
			}
			if slices.ContainsFunc(narrator.recorded(), func(note string) bool { return strings.HasPrefix(note, "hold ended") }) {
				t.Errorf("narrated %v, want the expiry told once, not also as a hold ending", narrator.recorded())
			}
		})
	}
}

// Test flow:
//  1. Put an escrow on hold whose reservations could all have come back an hour ago, once with its money back and once from a past epoch.
//  2. Run one resume tick at a clock of zero.
//  3. Assert the escrow with its money back resumes and the past-epoch one ends as epoch_passed: expiry is asked last.
func TestAnEarlierHoldRuleWinsOverExpiry(t *testing.T) {
	servingEpoch := int64(servingSnapshot().EpochIndex)
	cases := []struct {
		name        string
		epoch       int64
		verdict     HoldVerdict
		wantServing bool
		wantNote    string
	}{
		{name: "the money came back", epoch: servingEpoch, verdict: HoldResume, wantServing: true},
		{name: "the epoch passed", epoch: servingEpoch - 1, verdict: HoldKeep, wantNote: "hold ended 1: epoch_passed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testStore := newFakeStore()
			testStore.devshards["1"] = heldRecord("1", testCase.epoch)
			gate := newFakeHoldGate()
			gate.onHold["1"] = true
			gate.verdicts["1"] = testCase.verdict
			gate.returnBy["1"] = time.Unix(0, 0).Add(-time.Hour)
			manager := holdManager(t, testStore, &fakeTxClient{}, gate)
			narrator := &recordingLifecycleNarrator{}
			manager.narrator = narrator

			if _, err := resumeTick(t, manager, testStore); err != nil {
				t.Fatalf("resumeHeld = %v, want nil", err)
			}

			if testCase.wantServing {
				if record := testStore.devshards["1"]; !record.Active || record.OnHold {
					t.Fatalf("record = %+v, want serving", record)
				}
				return
			}
			assertParked(t, testStore, "1")
			if !slices.Contains(narrator.recorded(), testCase.wantNote) {
				t.Errorf("narrated %v, want %q", narrator.recorded(), testCase.wantNote)
			}
		})
	}
}

// Test flow:
//  1. Store a held record at the current epoch, with a hold gate that gives it no verdict (still short of money).
//  2. Call `resumeTick` and assert it returns no error.
//  3. Assert the stored record and the hold gate both still report the escrow on hold.
//  4. Assert the returned devshards slice also reports the escrow on hold.
func TestAnEscrowStillShortOfMoneyStaysOnHold(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	gate := newFakeHoldGate()
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)

	devshards, err := resumeTick(t, manager, testStore)
	if err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	if !testStore.devshards["1"].OnHold || !gate.isOnHold("1") {
		t.Fatal("an escrow still short of money left hold")
	}
	if !findRecord(t, devshards, "1").OnHold {
		t.Error("returned record left hold")
	}
}

// Test flow:
//  1. Store a record held since the previous epoch, with a hold gate returning `HoldResume`.
//  2. Call `resumeTick` and assert it returns no error.
//  3. Assert the record is parked rather than resumed.
//  4. Assert the returned devshards slice shows the record inactive, off hold, and marked SettlementPending.
func TestAHoldFromAnEarlierEpochIsParkedForSettlement(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch-1)
	gate := newFakeHoldGate()
	gate.verdicts["1"] = HoldResume
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)

	devshards, err := resumeTick(t, manager, testStore)
	if err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if record := findRecord(t, devshards, "1"); record.Active || record.OnHold || !record.SettlementPending {
		t.Errorf("returned record = %+v, want parked", record)
	}
}

// Test flow:
//  1. Store a held record at the current epoch, with a hold gate returning `HoldNonceSpent`.
//  2. Call `resumeTick` and assert it returns no error.
//  3. Assert the record is parked.
func TestANonceSpentHoldIsParked(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	gate := newFakeHoldGate()
	gate.verdicts["1"] = HoldNonceSpent
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)

	if _, err := resumeTick(t, manager, testStore); err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
}

// Test flow:
//  1. Store a held record at the current epoch and build a manager with the hold disabled.
//  2. Call `resumeTick` and assert it returns no error.
//  3. Assert the record is parked even though nothing changed about its funds.
func TestTurningTheHoldOffParksEveryEscrowOnHold(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	manager := holdManager(t, testStore, &fakeTxClient{}, newFakeHoldGate())
	manager.config = holderWithHold(t, false)

	if _, err := resumeTick(t, manager, testStore); err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
}

// Test flow:
//  1. Store one held record and one active record, but seed the hold gate with the opposite flags (record 2 marked on hold).
//  2. Call `resumeTick` and assert it returns no error.
//  3. Assert the hold gate ends up matching the rows' own state: record 1 on hold, record 2 serving.
func TestTheTickReSyncsTheRegistryFromTheRows(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	testStore.devshards["2"] = activeRecord("2", "model-a")
	gate := newFakeHoldGate()
	gate.onHold["2"] = true
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)

	if _, err := resumeTick(t, manager, testStore); err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	if !gate.isOnHold("1") || gate.isOnHold("2") {
		t.Fatalf("registry flags = %v, want the rows' own: 1 on hold, 2 serving", gate.onHold)
	}
}

// Test flow:
//  1. Store a record with Active set to false, with a hold gate returning `HoldResume`.
//  2. Call `resumeTick` and assert it returns no error.
//  3. Assert the record is still inactive, since an operator's deactivation must not be reversed by a resume verdict.
func TestAnInactiveRowIsNeverResumed(t *testing.T) {
	testStore := newFakeStore()
	record := activeRecord("1", "model-a")
	record.Active = false
	testStore.devshards["1"] = record
	gate := newFakeHoldGate()
	gate.verdicts["1"] = HoldResume
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)

	if _, err := resumeTick(t, manager, testStore); err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	if testStore.devshards["1"].Active {
		t.Fatal("an escrow an operator deactivated came back")
	}
}

// Test flow:
//  1. Build a devshards slice containing one held record and pass it directly to resumeHeld, with a hold gate returning `HoldResume`.
//  2. Assert resumeHeld returns no error.
//  3. Assert the caller's own slice element is left with OnHold still true, proving resumeHeld does not rewrite it in place.
func TestResumeHeldLeavesTheCallersSliceUntouched(t *testing.T) {
	testStore := newFakeStore()
	epoch := int64(servingSnapshot().EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	gate := newFakeHoldGate()
	gate.verdicts["1"] = HoldResume
	manager := holdManager(t, testStore, &fakeTxClient{}, gate)
	devshards := []store.DevshardRecord{heldRecord("1", epoch)}

	if _, err := manager.resumeHeld(context.Background(), servingSnapshot(), depletionModels(), devshards); err != nil {
		t.Fatalf("resumeHeld = %v, want nil", err)
	}

	if !devshards[0].OnHold {
		t.Fatal("resumeHeld rewrote the caller's slice in place")
	}
}

// Test flow:
//  1. Store a held record and a serving record, both for model-a at the current epoch, with a hold gate returning `HoldResume` for the held one; build a manager with rotation, hold enabled and settlement disabled.
//  2. Mark the serving record balance-exhausted, then call tick and assert it returns no error.
//  3. Assert no replacement escrow was created, since the resumed record meets the target of 1 within the same tick.
//  4. Assert the first record is now active and serving, and the second is active but on hold.
func TestAnEscrowResumedInTheTickCountsAsServingForADepletionInTheSameTick(t *testing.T) {
	testStore := newFakeStore()
	snapshot := servingSnapshot()
	epoch := int64(snapshot.EpochIndex)
	testStore.devshards["1"] = heldRecord("1", epoch)
	serving := activeRecord("2", "model-a")
	serving.RotationEpoch = epoch
	testStore.devshards["2"] = serving
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	gate := newFakeHoldGate()
	gate.onHold["1"] = true
	gate.verdicts["1"] = HoldResume
	configuration := config.Defaults()
	configuration.Rotation.Enabled = true
	configuration.Rotation.HoldEnabled = true
	configuration.Rotation.SettlementEnabled = false
	configuration.Rotation.ModelsJSON = `[{"model_id":"model-a","target_count":1,"reserve_count":0,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
	deps := testManagerDeps(t, testStore, txClient, &fakeSnapshotSource{snapshot: snapshot}, &configuration)
	deps.Holds = gate
	manager := mustManager(t, deps)
	manager.OnBalanceExhausted("2", "balance_floor")

	if err := manager.tick(context.Background()); err != nil {
		t.Fatalf("tick = %v, want nil", err)
	}

	if txClient.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: escrow 1 resumed this tick and meets the target of 1", txClient.createCalls)
	}
	if record := testStore.devshards["1"]; !record.Active || record.OnHold {
		t.Errorf("escrow 1 = %+v, want serving", record)
	}
	if record := testStore.devshards["2"]; !record.Active || !record.OnHold {
		t.Errorf("escrow 2 = %+v, want on hold", record)
	}
}

// Test flow:
//  1. Store a held record for model-a at the current epoch, with a hold gate returning `HoldResume`; build a manager with rotation, hold enabled and settlement disabled, target count 2.
//  2. Mark the same record balance-exhausted (a stale report), then call tick and assert it returns no error.
//  3. Assert the record is active and off hold, and the hold gate no longer reports it on hold.
//  4. Assert no replacement escrow was created, since a stale depletion report must not fund one.
func TestALeftoverDepletionReportDoesNotPutAResumedEscrowBackOnHold(t *testing.T) {
	testStore := newFakeStore()
	snapshot := servingSnapshot()
	testStore.devshards["1"] = heldRecord("1", int64(snapshot.EpochIndex))
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	gate := newFakeHoldGate()
	gate.onHold["1"] = true
	gate.verdicts["1"] = HoldResume
	configuration := config.Defaults()
	configuration.Rotation.Enabled = true
	configuration.Rotation.HoldEnabled = true
	configuration.Rotation.SettlementEnabled = false
	configuration.Rotation.ModelsJSON = `[{"model_id":"model-a","target_count":2,"reserve_count":0,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
	deps := testManagerDeps(t, testStore, txClient, &fakeSnapshotSource{snapshot: snapshot}, &configuration)
	deps.Holds = gate
	manager := mustManager(t, deps)
	manager.OnBalanceExhausted("1", "insufficient_balance")

	if err := manager.tick(context.Background()); err != nil {
		t.Fatalf("tick = %v, want nil", err)
	}

	if record := testStore.devshards["1"]; !record.Active || record.OnHold {
		t.Errorf("escrow 1 = %+v, want serving", record)
	}
	if gate.isOnHold("1") {
		t.Error("registry has the resumed escrow on hold again")
	}
	if txClient.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: a stale report must not fund a replacement", txClient.createCalls)
	}
}

func chainEpochFn(epoch uint64) func(context.Context, string) (chain.EscrowInfo, bool, error) {
	return func(_ context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
		return chain.EscrowInfo{EscrowID: escrowID, EpochIndex: epoch}, true, nil
	}
}

func failingChainEpochFn(context.Context, string) (chain.EscrowInfo, bool, error) {
	return chain.EscrowInfo{}, false, errors.New("chain unreachable")
}

// Test flow:
//  1. Store a held record whose RotationEpoch is 0 (the row has no epoch of its own), with a hold gate returning `HoldResume`; vary the tx client's getEscrowFn, whether rotation is off, and the models list across cases (chain reports current epoch, an older epoch, errors, reports no epoch, rotation off, model missing from the list).
//  2. Call resumeHeld directly.
//  3. Assert resumeHeld returns no error.
//  4. For a case expected to resume, assert the record is active, off hold, and its RotationEpoch is still 0 (the resolved epoch is never written back).
//  5. For a case expected to end the hold, assert the record is parked and the narrator recorded the expected "hold ended" reason.
func TestAHoldIsSettledAgainstTheChainsEpochWhenTheRowHasNone(t *testing.T) {
	currentEpoch := servingSnapshot().EpochIndex
	cases := []struct {
		name         string
		getEscrowFn  func(context.Context, string) (chain.EscrowInfo, bool, error)
		rotationOff  bool
		models       []ModelConfig
		wantResumed  bool
		wantNarrated string
	}{
		{name: "the chain reports the current epoch", getEscrowFn: chainEpochFn(currentEpoch), models: depletionModels(), wantResumed: true},
		{name: "the chain reports an older epoch", getEscrowFn: chainEpochFn(currentEpoch - 1), models: depletionModels(), wantNarrated: "hold ended 1: epoch_passed"},
		{name: "the chain errors", getEscrowFn: failingChainEpochFn, models: depletionModels(), wantNarrated: "hold ended 1: epoch_unknown"},
		{name: "the chain has no epoch for it", getEscrowFn: chainEpochFn(0), models: depletionModels(), wantNarrated: "hold ended 1: epoch_unknown"},
		{name: "rotation is off", getEscrowFn: chainEpochFn(currentEpoch), rotationOff: true, models: depletionModels(), wantNarrated: "hold ended 1: rotation_off"},
		{name: "the model left the models list", getEscrowFn: chainEpochFn(currentEpoch), models: nil, wantNarrated: "hold ended 1: rotation_off"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testStore := newFakeStore()
			testStore.devshards["1"] = heldRecord("1", 0)
			gate := newFakeHoldGate()
			gate.onHold["1"] = true
			gate.verdicts["1"] = HoldResume
			manager := holdManager(t, testStore, &fakeTxClient{getEscrowFn: testCase.getEscrowFn}, gate)
			if testCase.rotationOff {
				configuration := config.Defaults()
				configuration.Rotation.HoldEnabled = true
				manager.config = config.NewHolder(&configuration)
			}
			narrator := &recordingLifecycleNarrator{}
			manager.narrator = narrator
			devshards, err := testStore.ListDevshards(context.Background())
			if err != nil {
				t.Fatalf("ListDevshards: %v", err)
			}

			if _, err := manager.resumeHeld(context.Background(), servingSnapshot(), testCase.models, devshards); err != nil {
				t.Fatalf("resumeHeld = %v, want nil", err)
			}

			if testCase.wantResumed {
				if record := testStore.devshards["1"]; !record.Active || record.OnHold {
					t.Fatalf("record = %+v, want serving", record)
				}
				if record := testStore.devshards["1"]; record.RotationEpoch != 0 {
					t.Errorf("RotationEpoch = %d, want 0: the resolved epoch must not be written back", record.RotationEpoch)
				}
				return
			}
			assertParked(t, testStore, "1")
			if !slices.Contains(narrator.recorded(), testCase.wantNarrated) {
				t.Errorf("narrated %v, want %q", narrator.recorded(), testCase.wantNarrated)
			}
		})
	}
}

// Test flow:
//  1. Store one active record and vary the tx client's getEscrowFn across cases: chain reports the current epoch, an older epoch, or errors.
//  2. Deplete the record with reason "balance_floor" via `depleteOnce`.
//  3. Assert checkDepletion returns no error.
//  4. For the case where the epoch resolves to current, assert the record is on hold with RotationEpoch still 0; otherwise assert the record is parked.
func TestADepletedEscrowWhoseEpochCannotBeResolvedIsParkedNotHeld(t *testing.T) {
	cases := []struct {
		name        string
		getEscrowFn func(context.Context, string) (chain.EscrowInfo, bool, error)
		wantHeld    bool
	}{
		{name: "the chain reports the current epoch", getEscrowFn: chainEpochFn(servingSnapshot().EpochIndex), wantHeld: true},
		{name: "the chain reports an older epoch", getEscrowFn: chainEpochFn(servingSnapshot().EpochIndex - 1)},
		{name: "the chain errors", getEscrowFn: failingChainEpochFn},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testStore := newFakeStore()
			testStore.devshards["1"] = activeRecord("1", "model-a")
			txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999), getEscrowFn: testCase.getEscrowFn}
			manager := holdManager(t, testStore, txClient, newFakeHoldGate())

			if err := depleteOnce(t, manager, testStore, "1", "balance_floor"); err != nil {
				t.Fatalf("checkDepletion = %v, want nil", err)
			}

			if !testCase.wantHeld {
				assertParked(t, testStore, "1")
				return
			}
			if record := testStore.devshards["1"]; !record.Active || !record.OnHold || record.RotationEpoch != 0 {
				t.Fatalf("record = %+v, want on hold with RotationEpoch still 0", record)
			}
		})
	}
}

// Test flow:
//  1. Build newModelCounts over one serving regular, one serving reserve and one on hold, for one model.
//  2. Assert serving is 1 and onHold is 1: a reserve does not stand in for a regular in the target check.
func TestAReserveDoesNotCountAsServing(t *testing.T) {
	counts := newModelCounts([]store.DevshardRecord{
		{EscrowID: "regular", Model: "model-a", Active: true, RotationRole: roleRegular},
		{EscrowID: "reserve", Model: "model-a", Active: true, RotationRole: RoleReserve},
		{EscrowID: "held", Model: "model-a", Active: true, OnHold: true, RotationRole: roleRegular},
	})

	if counts.serving["model-a"] != 1 || counts.onHold["model-a"] != 1 {
		t.Fatalf("serving = %d, onHold = %d, want 1 and 1", counts.serving["model-a"], counts.onHold["model-a"])
	}
}
