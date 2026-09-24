package escrow

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
)

type fakeHoldGate struct {
	mu       sync.Mutex
	onHold   map[string]bool
	verdicts map[string]HoldVerdict
}

func newFakeHoldGate() *fakeHoldGate {
	return &fakeHoldGate{onHold: map[string]bool{}, verdicts: map[string]HoldVerdict{}}
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
	configuration.Rotation.ModelsJSON = `[{"model_id":"model-a","target_count":1,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
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
	configuration.Rotation.ModelsJSON = `[{"model_id":"model-a","target_count":2,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
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
