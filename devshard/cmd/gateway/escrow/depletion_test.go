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

func workingCreateEscrowFn(newEscrowID uint64) func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
	return func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("tx-" + modelID); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{EscrowID: newEscrowID}, nil
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

func TestOnBalanceExhaustedMarksAndDedups(t *testing.T) {
	m := &Manager{}
	m.OnBalanceExhausted("1")
	m.OnBalanceExhausted("1")
	m.OnBalanceExhausted("2")

	if len(m.depleted.keys) != 2 || !m.depleted.keys["1"] || !m.depleted.keys["2"] {
		t.Fatalf("depletedMarks = %v, want {1,2} deduped", m.depleted.keys)
	}
}

func TestCheckDepletionReplacesMarkedEscrowThenClearsMark(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	m := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	m.OnBalanceExhausted("1")
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, depletionModels(), devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1 replacement created", txClient.createCalls)
	}
	assertParked(t, testStore, "1")
	if _, ok := testStore.devshards["999"]; !ok {
		t.Fatal("replacement escrow 999 not registered")
	}
	if len(m.depleted.keys) != 0 {
		t.Fatalf("depletedMarks = %v, want cleared after a successful replacement", m.depleted.keys)
	}
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, depletionModels(), devshards); err != nil {
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
	m := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, depletionModels(), devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}
	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d, want 0 without a depletion mark", txClient.createCalls)
	}
	if record := testStore.devshards["1"]; !record.Active {
		t.Fatal("unmarked escrow was retired")
	}
}

// An exhausted escrow fails every request routed to it, and its idle in-flight count is what makes the
// load score prefer it, so it must stop taking traffic even where no replacement can be created.
func TestCheckDepletionRetiresEscrowWhoseModelHasNoReplacementConfigured(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	m := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	m.OnBalanceExhausted("1")
	otherModelOnly := []ModelConfig{{ModelID: "model-b", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_B_KEY"}}
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, otherModelOnly, devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d, want 0: no replacement is configured for model-a", txClient.createCalls)
	}
}

func TestCheckDepletionFailedReplacementKeepsMarkForNextTick(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			return chain.CreateEscrowResult{}, errors.New("broadcast setup failed")
		},
	}
	m := depletionManager(t, testStore, txClient)
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	m.OnBalanceExhausted("1")
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, depletionModels(), devshards); err == nil {
		t.Fatal("checkDepletion() = nil, want the replacement failure surfaced")
	}

	if !m.depleted.keys["1"] {
		t.Fatal("mark for escrow 1 was dropped by a failed replacement; nothing would ever retry it")
	}
	if record := testStore.devshards["1"]; !record.Active {
		t.Fatal("depleted escrow was retired even though no replacement exists")
	}
}

func TestReplaceDepletedCreatesReplacementBeforeRetiringOld(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	log := &callLog{}
	testStore.calls = log
	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			log.record("CreateReplacement")
			if err := onPrepared("tx-" + modelID); err != nil {
				return chain.CreateEscrowResult{}, err
			}
			return chain.CreateEscrowResult{EscrowID: 999}, nil
		},
	}
	m := depletionManager(t, testStore, txClient)
	model := depletionModels()[0]

	if err := m.replaceDepleted(context.Background(), testStore.devshards["1"], model, chain.PhaseSnapshot{}); err != nil {
		t.Fatalf("replaceDepleted() = %v, want nil", err)
	}

	createIndex, deactivateIndex := -1, -1
	for i, name := range log.snapshot() {
		switch name {
		case "CreateReplacement":
			createIndex = i
		case "ParkForSettlement":
			if deactivateIndex == -1 {
				deactivateIndex = i
			}
		}
	}
	if createIndex == -1 || deactivateIndex == -1 || createIndex > deactivateIndex {
		t.Fatalf("call log = %v, want the replacement created before the old escrow is retired", log.snapshot())
	}
}

func TestReplaceDepletedCreateFailureLeavesOldEscrow(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			return chain.CreateEscrowResult{}, errors.New("broadcast setup failed")
		},
	}
	m := depletionManager(t, testStore, txClient)
	model := depletionModels()[0]

	if err := m.replaceDepleted(context.Background(), testStore.devshards["1"], model, chain.PhaseSnapshot{}); err == nil {
		t.Fatal("replaceDepleted() = nil, want error when the replacement create fails")
	}
	got, ok := testStore.devshards["1"]
	if !ok || !got.Active {
		t.Fatalf("depleted escrow = %+v ok=%v, want it kept active until a replacement exists", got, ok)
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
	m := depletionManager(t, testStore, txClient)

	if err := m.replaceDepleted(context.Background(), depleted, depletionModels()[0], chain.PhaseSnapshot{}); err != nil {
		t.Fatalf("replaceDepleted() = %v, want nil", err)
	}

	if got := testStore.devshards["999"].RotationRole; got != roleRegular {
		t.Fatalf("replacement role = %q, want %q", got, roleRegular)
	}
}
