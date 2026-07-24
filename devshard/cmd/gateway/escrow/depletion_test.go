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
		tx:      txClient,
		store:   testStore,
		signer:  &fakeSignerSource{signer: testSigner(t)},
		breaker: newCreateBreaker(),
		now:     func() time.Time { return time.Unix(0, 0) },
		config:  holderWithSettlementEnabled(false),
	}
}

func activeRecord(id, model string) store.DevshardRecord {
	return store.DevshardRecord{EscrowID: id, Model: model, PrivateKeyEnv: "MODEL_A_KEY", Active: true, RotationRole: roleRegular}
}

func TestOnBalanceExhaustedMarksAndDedups(t *testing.T) {
	m := &Manager{}
	m.OnBalanceExhausted("1")
	m.OnBalanceExhausted("1")
	m.OnBalanceExhausted("2")

	if len(m.depletedMarks) != 2 || !m.depletedMarks["1"] || !m.depletedMarks["2"] {
		t.Fatalf("depletedMarks = %v, want {1,2} deduped", m.depletedMarks)
	}
}

func TestCheckDepletionReactiveReplacesMarkedEscrowThenClearsMark(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)}
	m := depletionManager(t, testStore, txClient)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	m.OnBalanceExhausted("1")
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1 replacement created", txClient.createCalls)
	}
	if _, ok := testStore.devshards["1"]; ok {
		t.Fatal("depleted escrow 1 still present, want retired (deleted, settlement off)")
	}
	if _, ok := testStore.devshards["999"]; !ok {
		t.Fatal("replacement escrow 999 not registered")
	}
	if len(m.depletedMarks) != 0 {
		t.Fatalf("depletedMarks = %v, want cleared after processing", m.depletedMarks)
	}
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("second checkDepletion() = %v, want nil", err)
	}
	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d after a second tick with no new marks, want still 1", txClient.createCalls)
	}
}

func TestCheckDepletionProactiveBudgetBoundsProbeCount(t *testing.T) {
	testStore := newFakeStore()
	devshards := make([]store.DevshardRecord, 0, 2*depletionScanBudget)
	for i := range 2 * depletionScanBudget {
		id := escrowIDForIndex(i)
		devshards = append(devshards, activeRecord(id, "model-a"))
	}
	log := &callLog{}
	txClient := &fakeTxClient{
		calls: log,
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			return chain.EscrowInfo{Balance: 5}, true, nil
		},
	}
	m := depletionManager(t, testStore, txClient)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}

	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}

	probes := 0
	for _, name := range log.snapshot() {
		if name == "GetEscrow" {
			probes++
		}
	}
	if probes != depletionScanBudget {
		t.Fatalf("probed %d escrows in one tick, want exactly the budget %d (never the whole %d-escrow set)", probes, depletionScanBudget, 2*depletionScanBudget)
	}
}

func TestCheckDepletionProactiveCursorAdvancesToNextBatch(t *testing.T) {
	testStore := newFakeStore()
	devshards := make([]store.DevshardRecord, 0, 2*depletionScanBudget)
	for i := range 2 * depletionScanBudget {
		devshards = append(devshards, activeRecord(escrowIDForIndex(i), "model-a"))
	}
	var probed []string
	txClient := &fakeTxClient{
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			probed = append(probed, escrowID)
			return chain.EscrowInfo{Balance: 5}, true, nil
		},
	}
	m := depletionManager(t, testStore, txClient)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}

	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("first checkDepletion() = %v", err)
	}
	firstBatch := append([]string(nil), probed...)
	probed = nil
	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("second checkDepletion() = %v", err)
	}

	if len(firstBatch) != depletionScanBudget || len(probed) != depletionScanBudget {
		t.Fatalf("batch sizes = %d and %d, want %d each", len(firstBatch), len(probed), depletionScanBudget)
	}
	first := make(map[string]bool, len(firstBatch))
	for _, id := range firstBatch {
		first[id] = true
	}
	for _, id := range probed {
		if first[id] {
			t.Fatalf("second tick re-probed %s from the first batch; the cursor must advance to a fresh batch", id)
		}
	}
}

func TestCheckDepletionProactiveReplacesOnlyZeroBalance(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["drained"] = activeRecord("drained", "model-a")
	testStore.devshards["funded"] = activeRecord("funded", "model-a")
	txClient := &fakeTxClient{
		createEscrowFn: workingCreateEscrowFn(999),
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			if escrowID == "drained" {
				return chain.EscrowInfo{Balance: 0}, true, nil
			}
			return chain.EscrowInfo{Balance: 7}, true, nil
		},
	}
	m := depletionManager(t, testStore, txClient)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	devshards := []store.DevshardRecord{testStore.devshards["drained"], testStore.devshards["funded"]}

	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil", err)
	}
	if _, ok := testStore.devshards["drained"]; ok {
		t.Fatal("zero-balance escrow not retired, want replaced")
	}
	if _, ok := testStore.devshards["funded"]; !ok {
		t.Fatal("funded escrow was retired, want left untouched")
	}
}

func TestCheckDepletionProactiveGetEscrowErrorKeepsEscrow(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	txClient := &fakeTxClient{
		createEscrowFn: workingCreateEscrowFn(999),
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			return chain.EscrowInfo{}, false, errors.New("endpoint unreachable")
		},
	}
	m := depletionManager(t, testStore, txClient)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	devshards := []store.DevshardRecord{testStore.devshards["1"]}

	if err := m.checkDepletion(context.Background(), chain.PhaseSnapshot{}, models, devshards); err != nil {
		t.Fatalf("checkDepletion() = %v, want nil (a probe error is not a depletion)", err)
	}
	if _, ok := testStore.devshards["1"]; !ok {
		t.Fatal("escrow retired on a probe error, want kept (ambiguity is not depletion)")
	}
	if txClient.createCalls != 0 {
		t.Fatalf("createCalls = %d, want 0 (no replacement on a probe error)", txClient.createCalls)
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
	model := ModelConfig{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}

	if err := m.replaceDepleted(context.Background(), testStore.devshards["1"], model, chain.PhaseSnapshot{}); err != nil {
		t.Fatalf("replaceDepleted() = %v, want nil", err)
	}

	createIndex, deleteIndex := -1, -1
	for i, name := range log.snapshot() {
		switch name {
		case "CreateReplacement":
			createIndex = i
		case "DeleteDevshard":
			deleteIndex = i
		}
	}
	if createIndex == -1 || deleteIndex == -1 || createIndex > deleteIndex {
		t.Fatalf("call log = %v, want CreateReplacement before DeleteDevshard (never zero coverage)", log.snapshot())
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
	model := ModelConfig{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}

	if err := m.replaceDepleted(context.Background(), testStore.devshards["1"], model, chain.PhaseSnapshot{}); err == nil {
		t.Fatal("replaceDepleted() = nil, want error when the replacement create fails")
	}
	got, ok := testStore.devshards["1"]
	if !ok || !got.Active {
		t.Fatalf("depleted escrow = %+v ok=%v, want it kept active until a replacement exists", got, ok)
	}
}

func escrowIDForIndex(i int) string {
	return "esc-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
}
