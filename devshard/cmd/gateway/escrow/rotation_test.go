package escrow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
	"devshard/signing"

	"devshard/cmd/gateway/internal/logcapture"
)

// succeedingCreateEscrowFn always succeeds, assigning sequential escrow ids/tx hashes from startID.
func succeedingCreateEscrowFn(startID uint64) func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
	nextID := startID
	return func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		id := nextID
		nextID++
		txHash := fmt.Sprintf("TX-%d", id)
		if err := onPrepared(txHash); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{EscrowID: id, TxHash: txHash}, nil
	}
}

func servedSnapshot(epoch uint64, blockHeight int64, modelID string) chain.PhaseSnapshot {
	return chain.PhaseSnapshot{
		EpochIndex:         epoch,
		BlockHeight:        blockHeight,
		FullWeightsByModel: map[string]map[string]float64{modelID: {"participant": 1}},
	}
}

func newRotationManager(t *testing.T, testStore *fakeStore, txClient *fakeTxClient, settlementEnabled bool) *Manager {
	t.Helper()
	return &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		breaker:          newCreateBreaker(),
		now:              func() time.Time { return time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC) },
		config:           holderWithSettlementEnabled(settlementEnabled),
	}
}

func failOnCreate(t *testing.T) func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
	return func(context.Context, *signing.Secp256k1Signer, uint64, string, func(string) error) (chain.CreateEscrowResult, error) {
		t.Fatal("createEscrow must not be called")
		return chain.CreateEscrowResult{}, nil
	}
}

func TestEnsureToTargetCreatesExactlyTheShortfall(t *testing.T) {
	testStore := newFakeStore()
	existing := store.DevshardRecord{EscrowID: "existing-1", Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 5}
	testStore.devshards[existing.EscrowID] = existing
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(100)}
	m := newRotationManager(t, testStore, txClient, false)
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}
	snapshot := servedSnapshot(5, 100, "model-a")

	created, err := m.ensureToTarget(context.Background(), roleTemp, 3, model, snapshot, []store.DevshardRecord{existing})
	if err != nil {
		t.Fatalf("ensureToTarget(): %v", err)
	}
	if created != 2 {
		t.Fatalf("created = %d, want 2 (target 3 minus 1 existing)", created)
	}
	if txClient.createCalls != 2 {
		t.Fatalf("createCalls = %d, want 2", txClient.createCalls)
	}
}

func TestEnsureToTargetAlreadyAtOrOverTarget(t *testing.T) {
	tests := []struct {
		name     string
		existing int
		target   int
	}{
		{name: "exactly at target", existing: 2, target: 2},
		{name: "over target", existing: 3, target: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testStore := newFakeStore()
			devshards := make([]store.DevshardRecord, 0, tt.existing)
			for i := 0; i < tt.existing; i++ {
				record := store.DevshardRecord{EscrowID: fmt.Sprintf("existing-%d", i), Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 5}
				devshards = append(devshards, record)
			}
			m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
			model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}
			snapshot := servedSnapshot(5, 100, "model-a")

			created, err := m.ensureToTarget(context.Background(), roleTemp, tt.target, model, snapshot, devshards)
			if err != nil {
				t.Fatalf("ensureToTarget(): %v", err)
			}
			if created != 0 {
				t.Fatalf("created = %d, want 0", created)
			}
		})
	}
}

// At/over target must short-circuit before the breaker is even consulted: gated() has a
// side effect (it decrements a cooldown tick), so a no-op call must never spend one.
func TestEnsureToTargetAtTargetDoesNotConsumeBreakerCooldown(t *testing.T) {
	testStore := newFakeStore()
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	m.breaker.recordFailure("model-a", roleTemp) // cooldown=1: a short-circuited call must leave it untouched
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}
	snapshot := chain.PhaseSnapshot{EpochIndex: 5, BlockHeight: 100} // no weights: cold-start, never treated as unserved
	existing := []store.DevshardRecord{{EscrowID: "existing-1", Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 5}}

	created, err := m.ensureToTarget(context.Background(), roleTemp, 1, model, snapshot, existing)
	if err != nil {
		t.Fatalf("ensureToTarget(): %v", err)
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0", created)
	}
	if !m.breaker.gated("model-a", roleTemp) {
		t.Fatal("breaker cooldown was consumed by an at-target call that should have short-circuited before reaching the breaker")
	}
}

func TestEnsureToTargetSkipsWhenModelNotServedByNetwork(t *testing.T) {
	testStore := newFakeStore()
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}
	// the network serves only model-b: known=true, served=false for model-a.
	snapshot := servedSnapshot(5, 100, "model-b")

	created, err := m.ensureToTarget(context.Background(), roleTemp, 3, model, snapshot, nil)
	if err != nil {
		t.Fatalf("ensureToTarget(): %v", err)
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0", created)
	}
}

func TestEnsureToTargetReportsSuppressionWhenBreakerGated(t *testing.T) {
	testStore := newFakeStore()
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	m.breaker.recordFailure("model-a", roleTemp)
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}
	snapshot := servedSnapshot(5, 100, "model-a")

	created, err := m.ensureToTarget(context.Background(), roleTemp, 3, model, snapshot, nil)
	if !errors.Is(err, errCreateSuppressed) {
		t.Fatalf("ensureToTarget() = %v, want errCreateSuppressed: a gated create is not a success the bridge may retire against", err)
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0", created)
	}
}

func TestEnsureToTargetStopsOnFirstErrorAndGatesBreaker(t *testing.T) {
	testStore := newFakeStore()
	broadcastErr := errors.New("broadcast rejected")
	txClient := &fakeTxClient{createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("TX-FAIL"); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{}, broadcastErr
	}}
	m := newRotationManager(t, testStore, txClient, false)
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}
	snapshot := servedSnapshot(5, 100, "model-a")

	created, err := m.ensureToTarget(context.Background(), roleTemp, 3, model, snapshot, nil)
	if err == nil {
		t.Fatal("ensureToTarget() error = nil, want the broadcast failure surfaced")
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0", created)
	}
	if txClient.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1 (stop at the first error)", txClient.createCalls)
	}

	created2, err2 := m.ensureToTarget(context.Background(), roleTemp, 3, model, snapshot, nil)
	if !errors.Is(err2, errCreateSuppressed) {
		t.Fatalf("second ensureToTarget() = %v, want errCreateSuppressed (the now-gated breaker)", err2)
	}
	if created2 != 0 {
		t.Fatalf("created on the gated call = %d, want 0", created2)
	}
	if txClient.createCalls != 1 {
		t.Fatalf("createCalls after the gated call = %d, want still 1", txClient.createCalls)
	}
}

func TestPrepareBridgeTempReachesTargetRetiresRegulars(t *testing.T) {
	testStore := newFakeStore()
	regularOne := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	regularTwo := store.DevshardRecord{EscrowID: "reg-2", Model: "model-a", Active: true, RotationRole: "", PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regularOne.EscrowID] = regularOne
	testStore.devshards[regularTwo.EscrowID] = regularTwo
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(200)}
	m := newRotationManager(t, testStore, txClient, false)
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 3, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 500, "model-a")
	devshards := []store.DevshardRecord{regularOne, regularTwo}

	if err := m.prepareBridge(context.Background(), snapshot, models, devshards); err != nil {
		t.Fatalf("prepareBridge(): %v", err)
	}

	assertParked(t, testStore, "reg-1")
	assertParked(t, testStore, "reg-2")
	tempCount := 0
	for _, record := range testStore.devshards {
		if record.Model == "model-a" && record.RotationRole == roleTemp {
			tempCount++
		}
	}
	if tempCount != 1 {
		t.Fatalf("temp escrows for model-a = %d, want 1 (TempCount target)", tempCount)
	}
	status, ok := testStore.rotationStatuses["model-a|"+roleTemp]
	if !ok {
		t.Fatal("no rotation status saved for model-a/temp")
	}
	if !status.Completed || status.Stage != stagePrepareTemp {
		t.Fatalf("rotation status = %+v, want Completed=true Stage=%q", status, stagePrepareTemp)
	}
}

func TestPrepareBridgeTempCreateFailsPromotesRegularsInstead(t *testing.T) {
	testStore := newFakeStore()
	regular := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regular.EscrowID] = regular
	broadcastErr := errors.New("broadcast rejected")
	txClient := &fakeTxClient{createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("TX-FAIL"); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{}, broadcastErr
	}}
	m := newRotationManager(t, testStore, txClient, false)
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 3, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 500, "model-a")
	devshards := []store.DevshardRecord{regular}

	if err := m.prepareBridge(context.Background(), snapshot, models, devshards); err == nil {
		t.Fatal("prepareBridge() error = nil, want the create failure surfaced")
	}

	got, ok := testStore.devshards["reg-1"]
	if !ok {
		t.Fatal("reg-1 missing after a failed prepareBridge, want it kept (promoted, not retired)")
	}
	if !got.Active {
		t.Fatal("reg-1.Active = false, want true (promote, not retire)")
	}
	if got.RotationRole != roleTemp {
		t.Fatalf("reg-1.RotationRole = %q, want %q (promoted)", got.RotationRole, roleTemp)
	}
	status, ok := testStore.rotationStatuses["model-a|"+roleTemp]
	if !ok {
		t.Fatal("no rotation status saved for model-a/temp")
	}
	if status.Completed {
		t.Fatal("rotation status Completed = true, want false")
	}
	if status.CreateError == "" {
		t.Fatal("rotation status CreateError is empty, want the create failure message")
	}
}

// The breaker is gated only after creation has been failing, so a gated pass has no replacement to
// show for the escrows it would retire. Retiring them there settles away the coverage that is left.
func TestPrepareBridgeGatedBreakerPromotesRegularsInsteadOfRetiringThem(t *testing.T) {
	testStore := newFakeStore()
	regular := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regular.EscrowID] = regular
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	m.breaker.recordFailure("model-a", roleTemp)
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 500, "model-a")
	devshards := []store.DevshardRecord{regular}

	err := m.prepareBridge(context.Background(), snapshot, models, devshards)

	got, ok := testStore.devshards["reg-1"]
	if !ok || !got.Active {
		t.Fatalf("reg-1 = %+v ok=%v, want it kept serving: no temp was created to take over from it", got, ok)
	}
	if got.RotationRole != roleTemp {
		t.Fatalf("reg-1.RotationRole = %q, want %q (promoted by the degrade path)", got.RotationRole, roleTemp)
	}
	if !errors.Is(err, errCreateSuppressed) {
		t.Fatalf("prepareBridge() = %v, want the suppressed create surfaced", err)
	}
}

func TestFinishBridgeGatedBreakerKeepsTempsInsteadOfRetiringThem(t *testing.T) {
	testStore := newFakeStore()
	temp := store.DevshardRecord{EscrowID: "temp-1", Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 9, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[temp.EscrowID] = temp
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	m.breaker.recordFailure("model-a", roleRegular)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 700, "model-a")
	devshards := []store.DevshardRecord{temp}

	err := m.finishBridge(context.Background(), snapshot, models, devshards)

	got, ok := testStore.devshards["temp-1"]
	if !ok || !got.Active {
		t.Fatalf("temp-1 = %+v ok=%v, want it kept serving: no regular was created to take over from it", got, ok)
	}
	if !errors.Is(err, errCreateSuppressed) {
		t.Fatalf("finishBridge() = %v, want the suppressed create surfaced", err)
	}
}

func TestPrepareBridgeOneModelFailureDoesNotStopOthers(t *testing.T) {
	testStore := newFakeStore()
	broadcastErr := errors.New("broadcast rejected")
	txClient := &fakeTxClient{createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if modelID == "model-a" {
			if err := onPrepared("TX-A-FAIL"); err != nil {
				return chain.CreateEscrowResult{}, err
			}
			return chain.CreateEscrowResult{}, broadcastErr
		}
		if err := onPrepared("TX-B-OK"); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{EscrowID: 900, TxHash: "TX-B-OK"}, nil
	}}
	m := newRotationManager(t, testStore, txClient, false)
	models := []ModelConfig{
		{ModelID: "model-a", TempCount: 1, TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"},
		{ModelID: "model-b", TempCount: 1, TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_B_KEY"},
	}
	snapshot := chain.PhaseSnapshot{EpochIndex: 9, BlockHeight: 500, FullWeightsByModel: map[string]map[string]float64{
		"model-a": {"p": 1}, "model-b": {"p": 1},
	}}

	if err := m.prepareBridge(context.Background(), snapshot, models, nil); err == nil {
		t.Fatal("prepareBridge() error = nil, want model-a's failure surfaced")
	}

	statusB, ok := testStore.rotationStatuses["model-b|"+roleTemp]
	if !ok {
		t.Fatal("no rotation status saved for model-b, want it processed despite model-a's failure")
	}
	if !statusB.Completed {
		t.Fatalf("model-b rotation status = %+v, want Completed=true", statusB)
	}
	if statusA := testStore.rotationStatuses["model-a|"+roleTemp]; statusA.Completed {
		t.Fatal("model-a rotation status Completed = true, want false")
	}
}

func TestPrepareBridgeSurfacesSaveRotationStatusFailure(t *testing.T) {
	testStore := newFakeStore()
	testStore.saveRotationStatusErr = errors.New("status store unavailable")
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(400)}
	m := newRotationManager(t, testStore, txClient, false)
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 500, "model-a")

	if err := m.prepareBridge(context.Background(), snapshot, models, nil); err == nil {
		t.Fatal("prepareBridge() error = nil, want the rotation-status save failure surfaced even though rotation itself succeeded")
	}
}

func TestPrepareBridgeSettlementEnabledSettlesRegularsBeforeRetiring(t *testing.T) {
	testStore := newFakeStore()
	regular := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regular.EscrowID] = regular
	log := &callLog{}
	testStore.calls = log
	txClient := &fakeTxClient{
		createEscrowFn: succeedingCreateEscrowFn(500),
		calls:          log,
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}
	m := &Manager{
		tx: txClient, store: testStore, signer: &fakeSignerSource{signer: testSigner(t)},
		breaker: newCreateBreaker(), now: time.Now,
		config:           holderWithSettlementEnabled(true),
		settlementSource: &fakeSettlementSource{busy: false, calls: log},
	}
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 500, "model-a")
	devshards := []store.DevshardRecord{regular}

	if err := m.prepareBridge(context.Background(), snapshot, models, devshards); err != nil {
		t.Fatalf("prepareBridge(): %v", err)
	}

	settled := false
	for _, name := range log.snapshot() {
		if name == "SettleEscrow" {
			settled = true
		}
	}
	if !settled {
		t.Fatal("SettleEscrow never called, want retire() to settle the regular escrow on chain when settlement is enabled")
	}
	if _, ok := testStore.devshards["reg-1"]; ok {
		t.Fatal("reg-1 still present after a successful settle+retire")
	}
}

func TestFinishBridgeActiveTempPresentCreatesRegularsAndRetiresTemps(t *testing.T) {
	testStore := newFakeStore()
	temp := store.DevshardRecord{EscrowID: "temp-1", Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 5, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[temp.EscrowID] = temp
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(300)}
	m := newRotationManager(t, testStore, txClient, false)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 2, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 700, "model-a")
	devshards := []store.DevshardRecord{temp}

	if err := m.finishBridge(context.Background(), snapshot, models, devshards); err != nil {
		t.Fatalf("finishBridge(): %v", err)
	}

	assertParked(t, testStore, "temp-1")
	regularCount := 0
	for _, record := range testStore.devshards {
		if record.Model == "model-a" && record.RotationRole == roleRegular && record.RotationEpoch == 9 {
			regularCount++
		}
	}
	if regularCount != 2 {
		t.Fatalf("regular escrows for model-a at epoch 9 = %d, want 2 (TargetCount)", regularCount)
	}
	status, ok := testStore.rotationStatuses["model-a|"+roleRegular]
	if !ok {
		t.Fatal("no rotation status saved for model-a/regular")
	}
	if !status.Completed || status.Stage != stageFinishRegular {
		t.Fatalf("rotation status = %+v, want Completed=true Stage=%q", status, stageFinishRegular)
	}
}

func TestFinishBridgeSkipsModelWithNoActiveTemp(t *testing.T) {
	testStore := newFakeStore()
	regular := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regular.EscrowID] = regular
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 2, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}
	snapshot := servedSnapshot(9, 700, "model-a")
	devshards := []store.DevshardRecord{regular}

	if err := m.finishBridge(context.Background(), snapshot, models, devshards); err != nil {
		t.Fatalf("finishBridge(): %v", err)
	}

	if _, ok := testStore.rotationStatuses["model-a|"+roleRegular]; ok {
		t.Fatal("rotation status saved for a model with nothing to finish, want none")
	}
	if _, ok := testStore.devshards["reg-1"]; !ok {
		t.Fatal("reg-1 removed by finishBridge, want it untouched (no temp existed to finish)")
	}
}

func TestPromoteRegularsToTempRelabelsActiveRegularsOnly(t *testing.T) {
	testStore := newFakeStore()
	regularOne := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular}
	regularEmpty := store.DevshardRecord{EscrowID: "reg-2", Model: "model-a", Active: true, RotationRole: ""}
	alreadyTemp := store.DevshardRecord{EscrowID: "temp-1", Model: "model-a", Active: true, RotationRole: roleTemp}
	otherModel := store.DevshardRecord{EscrowID: "reg-3", Model: "model-b", Active: true, RotationRole: roleRegular}
	inactive := store.DevshardRecord{EscrowID: "reg-4", Model: "model-a", Active: false, RotationRole: roleRegular}
	for _, record := range []store.DevshardRecord{regularOne, regularEmpty, alreadyTemp, otherModel, inactive} {
		testStore.devshards[record.EscrowID] = record
	}
	m := newRotationManager(t, testStore, &fakeTxClient{}, false)
	model := ModelConfig{ModelID: "model-a"}
	devshards := []store.DevshardRecord{regularOne, regularEmpty, alreadyTemp, otherModel, inactive}

	promoted, err := m.promoteRegularsToTemp(context.Background(), model, devshards)
	if err != nil {
		t.Fatalf("promoteRegularsToTemp(): %v", err)
	}
	if promoted != 2 {
		t.Fatalf("promoted = %d, want 2", promoted)
	}
	if got := testStore.devshards["reg-1"].RotationRole; got != roleTemp {
		t.Fatalf("reg-1.RotationRole = %q, want %q", got, roleTemp)
	}
	if got := testStore.devshards["reg-2"].RotationRole; got != roleTemp {
		t.Fatalf("reg-2.RotationRole = %q, want %q", got, roleTemp)
	}
	if got := testStore.devshards["reg-3"].RotationRole; got != roleRegular {
		t.Fatalf("reg-3 (other model) RotationRole = %q, want unchanged %q", got, roleRegular)
	}
	if got := testStore.devshards["reg-4"].RotationRole; got != roleRegular {
		t.Fatalf("reg-4 (inactive) RotationRole = %q, want unchanged %q", got, roleRegular)
	}
}

func TestPromoteRegularsToTempContinuesPastErrorReturnsFirst(t *testing.T) {
	testStore := newFakeStore()
	regularOne := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular}
	regularTwo := store.DevshardRecord{EscrowID: "reg-2", Model: "model-a", Active: true, RotationRole: roleRegular}
	testStore.devshards[regularOne.EscrowID] = regularOne
	testStore.devshards[regularTwo.EscrowID] = regularTwo
	writeFailure := errors.New("store unavailable")
	testStore.upsertDevshardErrByID = map[string]error{"reg-1": writeFailure}
	m := newRotationManager(t, testStore, &fakeTxClient{}, false)
	model := ModelConfig{ModelID: "model-a"}
	devshards := []store.DevshardRecord{regularOne, regularTwo}

	promoted, err := m.promoteRegularsToTemp(context.Background(), model, devshards)
	if err == nil {
		t.Fatal("promoteRegularsToTemp() error = nil, want reg-1's write failure surfaced")
	}
	if !errors.Is(err, writeFailure) {
		t.Fatalf("promoteRegularsToTemp() error = %v, want it to wrap %v", err, writeFailure)
	}
	if promoted != 1 {
		t.Fatalf("promoted = %d, want 1 (reg-2 still promoted despite reg-1's failure)", promoted)
	}
	if got := testStore.devshards["reg-2"].RotationRole; got != roleTemp {
		t.Fatalf("reg-2.RotationRole = %q, want %q (continued past reg-1's failure)", got, roleTemp)
	}
	if got := testStore.devshards["reg-1"].RotationRole; got != roleRegular {
		t.Fatalf("reg-1.RotationRole = %q, want unchanged %q (write failed)", got, roleRegular)
	}
}

// A retire that fails for a reason the next tick will not fix must reach the tick's error. Draining and
// an in-flight settlement are the two that resolve themselves; anything else is a bridge that silently
// never completes, visible only as Completed=false in a row nobody watches.
func TestDeferredRetireSeparatesNotYetFromFailed(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		deferred bool
	}{
		{name: "a draining escrow is retried", err: ErrDevshardBusy, deferred: true},
		{name: "a settlement already running is retried", err: ErrSettlementInFlight, deferred: true},
		{name: "wrapped, still retried", err: fmt.Errorf("retiring escrow 7: %w", ErrDevshardBusy), deferred: true},
		{name: "a missing signing key is not", err: errors.New("resolving signer for escrow 7"), deferred: false},
		{name: "an unknown escrow is not", err: ErrUnknownEscrow, deferred: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := deferredRetire(testCase.err); got != testCase.deferred {
				t.Fatalf("deferredRetire(%v) = %v, want %v", testCase.err, got, testCase.deferred)
			}
		})
	}
}

// A rotation that creates nothing because the chain serves no such model is a decision, not an idle
// tick. Unwritten, an operator looking for the escrow that never appeared finds no reason anywhere.
func TestASkippedRotationIsWrittenDown(t *testing.T) {
	entries := logcapture.Install(t)
	manager := &Manager{}
	snapshot := chain.PhaseSnapshot{
		EpochIndex:            4,
		FullWeightsByModel:    map[string]map[string]float64{"other-model": {"gonka1host": 1}},
		CurrentWeightsByModel: map[string]map[string]float64{"other-model": {"gonka1host": 1}},
	}

	created, err := manager.ensureToTarget(context.Background(), roleRegular, 1,
		ModelConfig{ModelID: "qwen"}, snapshot, nil)

	if err != nil || created != 0 {
		t.Fatalf("ensureToTarget = (%d, %v), want (0, nil)", created, err)
	}
	entry, found := entries.Find("rotation skipped, the network serves no such model")
	if !found {
		t.Fatalf("the skip left no trace: %+v", entries.All())
	}
	if got := logcapture.Field(entry, "model"); got != "qwen" {
		t.Fatalf("model field = %v, want the model that was skipped", got)
	}
}
