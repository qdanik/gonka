package escrow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"
	"devshard/signing"
)

type recordingLifecycleNarrator struct {
	mu    sync.Mutex
	calls []string
}

func (n *recordingLifecycleNarrator) note(format string, values ...any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, fmt.Sprintf(format, values...))
}

func (n *recordingLifecycleNarrator) recorded() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.calls...)
}

func (n *recordingLifecycleNarrator) EscrowCreated(escrowID, model, role string, epoch uint64, txHash string) {
	n.note("created %s %s %s epoch %d tx %s", escrowID, model, role, epoch, txHash)
}

func (n *recordingLifecycleNarrator) EscrowRecovered(escrowID, model, role string, epoch uint64, txHash string) {
	n.note("recovered %s %s %s epoch %d tx %s", escrowID, model, role, epoch, txHash)
}

func (n *recordingLifecycleNarrator) CommitmentCleared(txHash, model, role string, epoch uint64, reason string) {
	n.note("cleared %s %s %s epoch %d: %s", txHash, model, role, epoch, reason)
}

func (n *recordingLifecycleNarrator) EscrowGoneFromChain(escrowID string) {
	n.note("gone from chain %s", escrowID)
}

func (n *recordingLifecycleNarrator) EscrowMarkedForReplacement(escrowID, reason string) {
	n.note("marked for replacement %s: %s", escrowID, reason)
}

func (n *recordingLifecycleNarrator) EscrowDepletedWithoutReplacement(escrowID, model string) {
	n.note("depleted without replacement %s %s", escrowID, model)
}

func (n *recordingLifecycleNarrator) RotationSkipped(model, role string, epoch uint64) {
	n.note("rotation skipped %s %s epoch %d", model, role, epoch)
}

func (n *recordingLifecycleNarrator) RegularsPromotedToTemp(model string, epoch uint64, promoted int) {
	n.note("promoted %s epoch %d: %d", model, epoch, promoted)
}

func (n *recordingLifecycleNarrator) BridgePrepared(model string, epoch uint64, created, retired int) {
	n.note("bridge prepared %s epoch %d: created %d retired %d", model, epoch, created, retired)
}

func (n *recordingLifecycleNarrator) BridgeFinished(model string, epoch uint64, created, retired int) {
	n.note("bridge finished %s epoch %d: created %d retired %d", model, epoch, created, retired)
}

func (n *recordingLifecycleNarrator) EscrowParked(escrowID string) { n.note("parked %s", escrowID) }

func (n *recordingLifecycleNarrator) EscrowSettled(escrowID, model, txHash, settler string) {
	n.note("settled %s %s tx %s settler %s", escrowID, model, txHash, settler)
}

func (n *recordingLifecycleNarrator) SettlementReconciled(escrowID, txHash string) {
	n.note("reconciled %s tx %s", escrowID, txHash)
}

func (n *recordingLifecycleNarrator) SettledRecordDropped(escrowID string) {
	n.note("record dropped %s", escrowID)
}

func (n *recordingLifecycleNarrator) EscrowTickFailed(err error) { n.note("tick failed: %v", err) }

func (n *recordingLifecycleNarrator) TimeoutsSwept(due, applied, failed int) {
	n.note("swept due %d applied %d failed %d", due, applied, failed)
}

// The id is the text every other line names the escrow by, not the chain's number.
func TestACreatedEscrowIsNarratedWithItsIDAsText(t *testing.T) {
	narrator := &recordingLifecycleNarrator{}
	txClient := &fakeTxClient{createEscrowFn: func(_ context.Context, _ *signing.Secp256k1Signer, _ uint64, _ string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("TX-HAPPY"); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{EscrowID: 42, TxHash: "TX-HAPPY"}, nil
	}}
	m := &Manager{
		tx: txClient, store: newFakeStore(), signer: &fakeSignerSource{signer: testSigner(t)},
		breaker: newCreateBreaker(), now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		narrator: narrator,
	}

	_, err := m.createEscrow(context.Background(), ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}, roleTemp, 7, 500)

	require.NoError(t, err)
	require.Equal(t, []string{"created 42 model-a temp epoch 7 tx TX-HAPPY"}, narrator.recorded())
}

func TestReconcileNarratesWhatItRecoveredAndWhatItCleared(t *testing.T) {
	fixedNow := time.Date(2026, 3, 3, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name            string
		commitment      store.Commitment
		getTxEscrowIDFn func(ctx context.Context, txHash string) (uint64, bool, error)
		want            []string
	}{
		{
			name:            "a create that landed while the gateway was down",
			commitment:      store.Commitment{TxHash: "TXA", Model: "model-a", Role: roleTemp, Epoch: 3, PrivateKeyEnv: "KEY_A", CreatedAt: fixedNow},
			getTxEscrowIDFn: func(context.Context, string) (uint64, bool, error) { return 55, true, nil },
			want:            []string{"recovered 55 model-a temp epoch 3 tx TXA"},
		},
		{
			name:            "a transaction that committed and created nothing",
			commitment:      store.Commitment{TxHash: "TXB", Model: "model-a", Role: roleTemp, CreatedAt: fixedNow},
			getTxEscrowIDFn: func(context.Context, string) (uint64, bool, error) { return 0, false, nil },
			want:            []string{"cleared TXB model-a temp epoch 0: " + commitmentClearedNoEscrow},
		},
		{
			name:            "a transaction past its window",
			commitment:      store.Commitment{TxHash: "TXD", Model: "model-a", Role: roleTemp, CreatedAt: fixedNow.Add(-commitmentReconcileGrace - time.Second)},
			getTxEscrowIDFn: func(context.Context, string) (uint64, bool, error) { return 0, false, chain.ErrTxNotFound },
			want:            []string{"cleared TXD model-a temp epoch 0: " + commitmentClearedCannotLand},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testStore := newFakeStore()
			require.NoError(t, testStore.SaveCommitment(context.Background(), testCase.commitment))
			narrator := &recordingLifecycleNarrator{}
			m := &Manager{
				tx: &fakeTxClient{getTxEscrowIDFn: testCase.getTxEscrowIDFn}, store: testStore,
				breaker: newCreateBreaker(), now: func() time.Time { return fixedNow }, narrator: narrator,
			}

			require.NoError(t, m.reconcile(context.Background()))

			require.Equal(t, testCase.want, narrator.recorded())
		})
	}
}

func TestAnEscrowGoneFromChainIsNarrated(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true}
	narrator := &recordingLifecycleNarrator{}
	m := &Manager{
		tx: &fakeTxClient{getEscrowFn: func(context.Context, string) (chain.EscrowInfo, bool, error) {
			return chain.EscrowInfo{}, false, nil
		}},
		store: testStore, settlementSource: &fakeSettlementSource{}, narrator: narrator,
	}

	require.NoError(t, m.TriggerEscrowCheck(context.Background(), "1"))

	require.Equal(t, []string{"gone from chain 1"}, narrator.recorded())
}

func TestADepletedEscrowIsNarratedOnceWhenMarked(t *testing.T) {
	narrator := &recordingLifecycleNarrator{}
	m := &Manager{narrator: narrator}

	m.OnBalanceExhausted("1", "nonce_cap")
	m.OnBalanceExhausted("1", "nonce_cap")

	require.Equal(t, []string{"marked for replacement 1: nonce_cap"}, narrator.recorded())
}

func TestADepletedEscrowWithNoReplacementIsNarratedAfterItIsParked(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = activeRecord("1", "model-a")
	manager := depletionManager(t, testStore, &fakeTxClient{createEscrowFn: workingCreateEscrowFn(999)})
	narrator := &recordingLifecycleNarrator{}
	manager.narrator = narrator
	devshards := []store.DevshardRecord{testStore.devshards["1"]}
	otherModelOnly := []ModelConfig{{ModelID: "model-b", TargetCount: 1, Amount: 1000, PrivateKeyEnv: "MODEL_B_KEY"}}

	manager.OnBalanceExhausted("1", "test")
	require.NoError(t, manager.checkDepletion(context.Background(), servingSnapshot(), otherModelOnly, devshards))

	require.Equal(t, []string{
		"marked for replacement 1: test",
		"parked 1",
		"depleted without replacement 1 model-a",
	}, narrator.recorded())
}

// A rotation that creates nothing on purpose is a decision; the narration is the only place its reason appears.
func TestASkippedRotationIsNarrated(t *testing.T) {
	narrator := &recordingLifecycleNarrator{}
	manager := &Manager{narrator: narrator}
	snapshot := chain.PhaseSnapshot{
		EpochIndex:            4,
		FullWeightsByModel:    map[string]map[string]float64{"other-model": {"gonka1host": 1}},
		CurrentWeightsByModel: map[string]map[string]float64{"other-model": {"gonka1host": 1}},
	}

	created, err := manager.ensureToTarget(context.Background(), roleRegular, 1, ModelConfig{ModelID: "qwen"}, snapshot, nil)

	require.NoError(t, err)
	require.Zero(t, created)
	require.Equal(t, []string{"rotation skipped qwen regular epoch 4"}, narrator.recorded())
}

func TestAFailedBridgePreparationNarratesThePromotion(t *testing.T) {
	testStore := newFakeStore()
	regular := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regular.EscrowID] = regular
	txClient := &fakeTxClient{createEscrowFn: func(_ context.Context, _ *signing.Secp256k1Signer, _ uint64, _ string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		if err := onPrepared("TX-FAIL"); err != nil {
			return chain.CreateEscrowResult{}, err
		}
		return chain.CreateEscrowResult{}, errors.New("broadcast rejected")
	}}
	m := newRotationManager(t, testStore, txClient, false)
	narrator := &recordingLifecycleNarrator{}
	m.narrator = narrator
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 3, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}

	require.Error(t, m.prepareBridge(context.Background(), servedSnapshot(9, 500, "model-a"), models, []store.DevshardRecord{regular}))

	require.Equal(t, []string{"promoted model-a epoch 9: 1"}, narrator.recorded())
}

func TestAPreparedBridgeNarratesWhatItCreatedAndRetired(t *testing.T) {
	testStore := newFakeStore()
	regularOne := store.DevshardRecord{EscrowID: "reg-1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	regularTwo := store.DevshardRecord{EscrowID: "reg-2", Model: "model-a", Active: true, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[regularOne.EscrowID] = regularOne
	testStore.devshards[regularTwo.EscrowID] = regularTwo
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(200)}, false)
	narrator := &recordingLifecycleNarrator{}
	m.narrator = narrator
	models := []ModelConfig{{ModelID: "model-a", TempCount: 1, TargetCount: 3, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}

	require.NoError(t, m.prepareBridge(context.Background(), servedSnapshot(9, 500, "model-a"), models, []store.DevshardRecord{regularOne, regularTwo}))

	require.Equal(t, []string{
		"created 200 model-a temp epoch 9 tx TX-200",
		"parked reg-1",
		"parked reg-2",
		"bridge prepared model-a epoch 9: created 1 retired 2",
	}, narrator.recorded())
}

func TestAFinishedBridgeNarratesWhatItCreatedAndRetired(t *testing.T) {
	testStore := newFakeStore()
	temp := store.DevshardRecord{EscrowID: "temp-1", Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 5, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[temp.EscrowID] = temp
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(300)}, false)
	narrator := &recordingLifecycleNarrator{}
	m.narrator = narrator
	models := []ModelConfig{{ModelID: "model-a", TargetCount: 2, Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}}

	require.NoError(t, m.finishBridge(context.Background(), servedSnapshot(9, 700, "model-a"), models, []store.DevshardRecord{temp}))

	require.Equal(t, []string{
		"created 300 model-a regular epoch 9 tx TX-300",
		"created 301 model-a regular epoch 9 tx TX-301",
		"parked temp-1",
		"bridge finished model-a epoch 9: created 2 retired 1",
	}, narrator.recorded())
}

func TestASettledRetirementIsNarratedFromParkToDrop(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "5", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	txClient := &fakeTxClient{settleEscrowFn: func(_ context.Context, _ *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
		return chain.SettleEscrowResult{EscrowID: input.EscrowID, TxHash: "SETTLE-TX", Settler: "gonka1settler"}, nil
	}}
	narrator := &recordingLifecycleNarrator{}
	m := &Manager{
		tx: txClient, store: testStore, signer: &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{}, config: holderWithSettlementEnabled(true), narrator: narrator,
	}

	require.NoError(t, m.retire(context.Background(), record))

	require.Equal(t, []string{
		"parked 5",
		"settled 5 model-a tx SETTLE-TX settler gonka1settler",
		"record dropped 5",
	}, narrator.recorded())
}

func TestASettleAlreadyOnChainIsNarratedAsReconciled(t *testing.T) {
	record := store.DevshardRecord{EscrowID: "7", Model: "model-a", SettlementPending: true, SettleTxHash: "SETTLE-TX"}
	testStore := newFakeStore()
	testStore.devshards[record.EscrowID] = record
	narrator := &recordingLifecycleNarrator{}
	m := &Manager{
		tx:    &fakeTxClient{txCommittedFn: func(string) (bool, error) { return true, nil }},
		store: testStore, signer: &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{}, narrator: narrator,
	}

	_, err := m.settle(context.Background(), record, false)

	require.NoError(t, err)
	require.Equal(t, []string{"parked 7", "reconciled 7 tx SETTLE-TX"}, narrator.recorded())
}

func TestAFailedTickIsNarratedWithItsError(t *testing.T) {
	testStore := newFakeStore()
	testStore.listDevshardsErr = errors.New("store unavailable")
	cfg := config.Defaults()
	narrator := &recordingLifecycleNarrator{}
	deps := testManagerDeps(t, testStore, &fakeTxClient{}, &fakeSnapshotSource{}, &cfg)
	deps.Narrator = narrator
	m := mustManager(t, deps)

	m.runTick(context.Background())

	require.Equal(t, []string{"tick failed: store unavailable"}, narrator.recorded())
}

func TestASweepThatFoundVotesOwedIsNarrated(t *testing.T) {
	cfg := config.Defaults()
	cfg.TimeoutSweep.BudgetPerTick = 3
	narrator := &recordingLifecycleNarrator{}
	deps := testManagerDeps(t, newFakeStore(), &fakeTxClient{}, &fakeSnapshotSource{}, &cfg)
	deps.Timeouts = &recordingSweeper{}
	deps.Narrator = narrator
	m := mustManager(t, deps)

	m.sweepTimeouts(context.Background())
	m.sweepWork.Wait()

	require.Equal(t, []string{"swept due 1 applied 1 failed 0"}, narrator.recorded())
}
