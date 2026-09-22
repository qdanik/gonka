package escrow

import (
	"context"
	"errors"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
	"devshard/signing"
)

// A snapshot that carries chain data: replacement is keyed by the epoch that funded it.
func servingSnapshot() chain.PhaseSnapshot {
	return chain.PhaseSnapshot{EpochIndex: 9, BlockHeight: 100}
}

func workingCreateEscrowFn(newEscrowID uint64) func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
	return func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("tx-" + modelID); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{EscrowID: newEscrowID}, nil
	}
}

func failingCreateEscrowFn() func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
	return func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		return chain.CreateEscrowResult{}, errors.New("account info: rpc unavailable")
	}
}

func unconfirmedCreateEscrowFn() func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
	return func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("tx-" + modelID); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{}, errors.New("waiting for tx: context deadline exceeded")
	}
}

func depletionManager(t *testing.T, testStore *fakeStore, txClient *fakeTxClient) *Manager {
	t.Helper()
	return &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		breaker:          newCreateBreaker(),
		now:              func() time.Time { return time.Unix(0, 0) },
		config:           holderWithSettlementEnabled(false),
	}
}

func activeRecord(id, model string) store.DevshardRecord {
	return store.DevshardRecord{EscrowID: id, Model: model, PrivateKeyEnv: "MODEL_A_KEY", Active: true, RotationRole: roleRegular}
}

// assertParked pins the retirement outcome when settlement is off: the row survives (it carries the
// only key that can settle the escrow) and is marked for a later settle.
func assertParked(t *testing.T, testStore *fakeStore, escrowID string) {
	t.Helper()
	record, ok := testStore.devshards[escrowID]
	if !ok {
		t.Fatalf("escrow %s is gone from the registry; its private_key_env is the only way to settle it", escrowID)
	}
	if record.Active {
		t.Errorf("escrow %s is still active after retirement", escrowID)
	}
	if !record.SettlementPending {
		t.Errorf("escrow %s is not marked settlement-pending, so nothing will ever settle it", escrowID)
	}
}

func depletionModels() []ModelConfig {
	return []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
}

func depletionTick(t *testing.T, manager *Manager, testStore *fakeStore, escrowID string) error {
	t.Helper()
	manager.OnBalanceExhausted(escrowID, "nonce_cap")
	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards: %v", err)
	}
	return manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards)
}

func TestOnBalanceExhaustedMarksAndDedups(t *testing.T) {
	manager := &Manager{}
	manager.OnBalanceExhausted("1", "test")
	manager.OnBalanceExhausted("1", "test")
	manager.OnBalanceExhausted("2", "test")

	if len(manager.depleted.reasons) != 2 || manager.depleted.reasons["1"] != "test" || manager.depleted.reasons["2"] != "test" {
		t.Fatalf("depletedMarks = %v, want {1,2} deduped", manager.depleted.reasons)
	}
}

func TestCheckDepletionReplacesMarkedEscrowThenClearsMark(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	manager.OnBalanceExhausted("1", "test")
	if err := manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1 replacement created", txClient.createCalls)
	}
	assertParked(t, testStore, "1")
	if _, ok := testStore.devshards["999"]; !ok {
		t.Fatal("replacement escrow 999 not registered")
	}
	if len(manager.depleted.reasons) != 0 {
		t.Fatalf("depletedMarks = %v, want cleared after a successful replacement", manager.depleted.reasons)
	}
	if err := manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards); err != nil {
		t.Fatalf("second checkDepletion() = %v, want nil", err)
	}
	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d after a second tick with no new marks, want still 1", txClient.createCalls)
	}
}

func TestCheckDepletionUnmarkedEscrowIsLeftAlone(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	if err := manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}
	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d, want 0 without a depletion mark", txClient.createCalls)
	}
	if record := testStore.devshards["1"]; !record.Active {
		t.Fatal("unmarked escrow was parked")
	}
}

// An exhausted escrow fails every request routed to it, and its idle in-flight count is what makes the
// load score prefer it, so it must stop taking traffic even where no replacement can be created.
func TestCheckDepletionParksEscrowWhoseModelHasNoReplacementConfigured(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	manager.OnBalanceExhausted("1", "test")
	otherModelOnly := []ModelConfig{{ModelID: "model-b", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_B_KEY"}}
	if err := manager.checkDepletion(context.Background(), servingSnapshot(), otherModelOnly, devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d, want 0: no replacement is configured for model-a", txClient.createCalls)
	}
}

func TestADepletedEscrowLeavesServiceBeforeItsReplacementIsCreated(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	routing := &fakeSettlementSource{}
	var savedActiveAtCreate, routedAtCreate bool
	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			record, _ := testStore.snapshotDevshard("1")
			savedActiveAtCreate, routedAtCreate = record.Active, !routing.retired
			return workingCreateEscrowFn(999)(ctx, signer, amount, modelID, onPrepared)
		},
	}
	manager := depletionManager(t, testStore, txClient)
	manager.settlementSource = routing

	if err := depletionTick(t, manager, testStore, "1"); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1 replacement created", txClient.createCalls)
	}
	if savedActiveAtCreate {
		t.Error("the replacement was created while the depleted escrow was still saved active")
	}
	if routedAtCreate {
		t.Error("the replacement was created while the depleted escrow was still routed")
	}
}

func TestAReplacementThatNeverConfirmedIsNotBroadcastAgain(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: unconfirmedCreateEscrowFn()}
	manager := depletionManager(t, testStore, txClient)

	if err := depletionTick(t, manager, testStore, "1"); err == nil {
		t.Fatal("first checkDepletion() = nil, want the unconfirmed create surfaced")
	}
	if err := depletionTick(t, manager, testStore, "1"); err != nil {
		t.Fatalf("second checkDepletion() = %v, want nil", err)
	}

	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1: a depleted escrow gets one replacement, however many ticks see it", txClient.createCalls)
	}
}

func TestAReplacementThatLandedWithoutConfirmationIsRegisteredByReconcile(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{
		createEscrowFn: unconfirmedCreateEscrowFn(),
		getTxEscrowIDFn: func(ctx context.Context, txHash string) (uint64, bool, error) {
			return 999, true, nil
		},
	}
	manager := depletionManager(t, testStore, txClient)
	if err := depletionTick(t, manager, testStore, "1"); err == nil {
		t.Fatal("checkDepletion() = nil, want the unconfirmed create surfaced")
	}

	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile() = %v, want nil", err)
	}

	replacement, registered := testStore.devshards["999"]
	if !registered || !replacement.Active {
		t.Fatalf("replacement = %+v, registered %v; want escrow 999 registered and active", replacement, registered)
	}
}

func TestADepletedEscrowLeavesServiceWhenItsReplacementFails(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: failingCreateEscrowFn()}
	manager := depletionManager(t, testStore, txClient)

	if err := depletionTick(t, manager, testStore, "1"); err == nil {
		t.Fatal("checkDepletion() = nil, want the failed replacement surfaced")
	}

	assertParked(t, testStore, "1")
}

func TestAFailureToStopRoutingAParkedEscrowIsSurfaced(t *testing.T) {
	routingFailure := errors.New("flushing session snapshot: disk full")
	testCases := []struct {
		name            string
		models          []ModelConfig
		createEscrowFn  func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error)
		wantCreateCalls int
	}{
		{name: "replacement created", models: depletionModels(), createEscrowFn: workingCreateEscrowFn(999), wantCreateCalls: 1},
		{name: "replacement failed", models: depletionModels(), createEscrowFn: failingCreateEscrowFn(), wantCreateCalls: 1},
		{name: "no replacement configured", models: nil, createEscrowFn: workingCreateEscrowFn(999), wantCreateCalls: 0},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testStore := newFakeStore()
			testStore.devshards["1"] = activeRecord("1", "model-a")
			txClient := &fakeTxClient{createEscrowFn: testCase.createEscrowFn}
			manager := depletionManager(t, testStore, txClient)
			manager.settlementSource = &fakeSettlementSource{retireErr: routingFailure}
			manager.OnBalanceExhausted("1", "nonce_cap")

			err := manager.checkDepletion(context.Background(), servingSnapshot(), testCase.models, []store.DevshardRecord{testStore.devshards["1"]})

			if !errors.Is(err, routingFailure) {
				t.Errorf("checkDepletion() = %v, want the routing failure surfaced", err)
			}
			if txClient.createCalls != testCase.wantCreateCalls {
				t.Errorf("createCalls = %d, want %d", txClient.createCalls, testCase.wantCreateCalls)
			}
			assertParked(t, testStore, "1")
		})
	}
}

func TestNoReplacementIsCreatedWhileADepletedEscrowCannotBeParked(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	testStore.setActiveErr = errors.New("database is locked")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	routing := &fakeSettlementSource{}
	manager := depletionManager(t, testStore, txClient)
	manager.settlementSource = routing

	if err := depletionTick(t, manager, testStore, "1"); err == nil {
		t.Fatal("checkDepletion() = nil, want the failed park surfaced")
	}

	if txClient.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 while the escrow is still saved active", txClient.createCalls)
	}
	if routing.retired {
		t.Error("the escrow left routing although its park was never saved")
	}
}

func TestADepletedEscrowIsReplacedOnceTheStoreRecovers(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	testStore.setActiveErr = errors.New("database is locked")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := depletionManager(t, testStore, txClient)
	if err := depletionTick(t, manager, testStore, "1"); err == nil {
		t.Fatal("first checkDepletion() = nil, want the failed park surfaced")
	}
	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d before the store recovered, want 0", txClient.createCalls)
	}
	testStore.setActiveErr = nil

	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards: %v", err)
	}
	if err := manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1 once the park succeeds", txClient.createCalls)
	}
}

func TestAnEscrowAlreadyOutOfServiceGetsNoReplacement(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = parkedRecord("1")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := depletionManager(t, testStore, txClient)

	manager.OnBalanceExhausted("1", "nonce_cap")
	if err := manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), []store.DevshardRecord{activeRecord("1", "model-a")}); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d, want 0: the escrow left service before this tick acted on it", txClient.createCalls)
	}
}

// A depleted temp replaced by another temp is coverage the next bridge immediately retires, so the
// model loses the escrow the replacement was meant to preserve.
func TestADepletedTempIsReplacedByARegular(t *testing.T) {
	testStore := newFakeStore()
	depleted := activeRecord("1", "model-a")
	depleted.RotationRole = roleTemp
	testStore.devshards["1"] = depleted
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	manager := depletionManager(t, testStore, txClient)

	if err := depletionTick(t, manager, testStore, "1"); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	if got := testStore.devshards["999"].RotationRole; got != roleRegular {
		t.Fatalf("replacement role = %q, want %q", got, roleRegular)
	}
}

// The picker rediscovers an exhausted escrow on every request; the operator hears it once.
func TestExhaustionIsAnnouncedOncePerTick(t *testing.T) {
	t.Parallel()
	var marks markSet

	first := marks.mark("escrow-1")
	second := marks.mark("escrow-1")
	other := marks.mark("escrow-2")

	if !first || second {
		t.Fatalf("mark returned %v then %v, want the first to be new and the second not", first, second)
	}
	if !other {
		t.Fatal("a different escrow was treated as already marked")
	}
	if drained := marks.drain(); len(drained) != 2 {
		t.Fatalf("drain returned %d escrows, want both", len(drained))
	}
	if marks.mark("escrow-1") != true {
		t.Fatal("a drained escrow was not announceable again on the next tick")
	}
}

func TestADepletedEscrowIsNotReplacedBeforeTheChainIsKnown(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	routing := &fakeSettlementSource{}
	manager := depletionManager(t, testStore, txClient)
	manager.settlementSource = routing
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	manager.OnBalanceExhausted("1", "test")
	err := manager.checkDepletion(context.Background(), chain.PhaseSnapshot{}, depletionModels(), devshards)

	if err == nil {
		t.Fatal("a replacement was created under a snapshot with no epoch: no epoch counts it, so the next bridge funds a whole set on top of it")
	}
	if txClient.createCalls != 0 {
		t.Errorf("createCalls = %d, want none before the chain is known", txClient.createCalls)
	}
	if record := testStore.devshards["1"]; !record.Active {
		t.Error("the escrow was parked before the chain is known, so no later tick replaces it")
	}
	if routing.retired {
		t.Error("the escrow left routing before the chain is known, so no later tick replaces it")
	}
	if err := manager.checkDepletion(context.Background(), servingSnapshot(), depletionModels(), devshards); err != nil {
		t.Fatalf("checkDepletion() once the chain is known = %v, want nil", err)
	}
	if txClient.createCalls != 1 {
		t.Errorf("createCalls = %d once the chain is known, want 1: the refused escrow stays marked for the next tick", txClient.createCalls)
	}
}
