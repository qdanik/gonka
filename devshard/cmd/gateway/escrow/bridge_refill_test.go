package escrow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/funding"
	"devshard/cmd/gateway/store"
	"devshard/signing"
	"devshard/types"
)

type refillBridge struct {
	manager   *Manager
	store     *fakeStore
	devshards []store.DevshardRecord
	snapshot  chain.PhaseSnapshot
	attempts  *int
}

func newRefillBridge(t *testing.T, refill int, create createFn) refillBridge {
	t.Helper()
	attempts := 0
	counting := func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
		attempts++
		return create(ctx, signer, amount, modelID, onPrepared)
	}
	manager, testStore, devshards := bridgeSizingManager(t, 0, counting)
	manager.planner.models[bridgeSizingModel].bridgeRefill = refill
	manager.planner.models[bridgeSizingModel].bucket = funding.NewBucket(time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC))
	snapshot := servedSnapshot(9, 500, bridgeSizingModel)
	label := int64(bridgeLabel(snapshot))
	for _, escrowID := range []string{"temp-1", "temp-2", "temp-3", "temp-4"} {
		record := store.DevshardRecord{EscrowID: escrowID, Model: bridgeSizingModel, Active: true, RotationRole: roleTemp, RotationEpoch: label, PrivateKeyEnv: "MODEL_A_KEY", Amount: 1}
		testStore.devshards[escrowID] = record
		devshards = append(devshards, record)
	}
	return refillBridge{manager: manager, store: testStore, devshards: devshards, snapshot: snapshot, attempts: &attempts}
}

func (bridge refillBridge) prepare(t *testing.T, maxUnsettled int) {
	t.Helper()
	require.NoError(t, bridge.manager.prepareBridge(context.Background(), bridge.snapshot, bridgeSizingModels(maxUnsettled), bridge.devshards))
}

func (bridge refillBridge) park(escrowID string) {
	bridge.store.devshards[escrowID] = store.DevshardRecord{EscrowID: escrowID, Model: bridgeSizingModel, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
}

// Test flow:
//  1. Seat four temps of the bridge label, the full temp count, with a last decision that asks for two more.
//  2. Call `prepareBridge`.
//  3. Assert two creates were attempted, six temps exist and both regulars are retired.
func TestTheBridgeFundsTheTempsTheLastDecisionAskedForBeyondTheTempCount(t *testing.T) {
	bridge := newRefillBridge(t, 2, succeedingCreateEscrowFn(200))

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Equal(t, 2, *bridge.attempts, "creates attempted")
	require.Equal(t, 6, countTemps(bridge.store), "temps")
	require.False(t, bridge.store.devshards["reg-1"].Active, "regular retired")
	require.False(t, bridge.store.devshards["reg-2"].Active, "regular retired")
}

// Test flow:
//  1. Seat four temps of the bridge label with a last decision that asks for two more.
//  2. Call `prepareBridge` twice without a new decision in between.
//  3. Assert the first call created two and the second created nothing.
func TestTheBridgeSpendsTheDecisionsRefillOnce(t *testing.T) {
	bridge := newRefillBridge(t, 2, succeedingCreateEscrowFn(200))

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)
	bridge.devshards = nil
	for _, record := range bridge.store.devshards {
		bridge.devshards = append(bridge.devshards, record)
	}
	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Equal(t, 2, *bridge.attempts, "creates attempted")
	require.Equal(t, 6, countTemps(bridge.store), "temps")
}

// Test flow:
//  1. Seat four temps of the bridge label with a last decision that asks for two more, and a full bucket.
//  2. Call `prepareBridge`.
//  3. Assert two creates landed and the bucket lost exactly two tokens.
func TestTheBridgeSpendsBucketTokensOnTheTempsTheRefillFunded(t *testing.T) {
	bridge := newRefillBridge(t, 2, succeedingCreateEscrowFn(200))
	fresh := funding.NewBucket(time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC))

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Equal(t, 2, *bridge.attempts, "creates attempted")
	require.Equal(t, fresh.Available()-2, bridge.manager.planner.models[bridgeSizingModel].bucket.Available(), "bucket tokens left")
}

// Test flow:
//  1. Seat four temps of the bridge label already at the ceiling the next epoch's regulars leave, with a last decision that asks for two more, and a full bucket.
//  2. Call `prepareBridge`.
//  3. Assert nothing was created and the bucket kept every token.
func TestTheBridgeSpendsNoBucketTokenWhenTheCeilingStopsTheRefill(t *testing.T) {
	bridge := newRefillBridge(t, 2, failOnCreate(t))
	fresh := funding.NewBucket(time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC))

	bridge.prepare(t, 9)

	require.Zero(t, *bridge.attempts, "creates attempted")
	require.Equal(t, fresh.Available(), bridge.manager.planner.models[bridgeSizingModel].bucket.Available(), "bucket tokens left")
}

// Test flow:
//  1. Seat four temps of the bridge label with no refill asked for.
//  2. Call `prepareBridge`.
//  3. Assert no create was attempted, whatever money the temps hold.
func TestTheBridgeCreatesNothingWithoutARefillWhenTheTempCountIsMet(t *testing.T) {
	bridge := newRefillBridge(t, 0, failOnCreate(t))

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Zero(t, *bridge.attempts, "creates attempted")
	require.Equal(t, 4, countTemps(bridge.store), "temps")
}

// Test flow:
//  1. Seat four temps of the bridge label, one open temp commitment for the label and a last decision that asks for two more.
//  2. Call `prepareBridge`.
//  3. Assert two creates were attempted: the plan netted the open commitment already, so the bridge adds the two on top of it.
func TestTheBridgeCountsAnOpenTempCommitmentAgainstTheRefill(t *testing.T) {
	bridge := newRefillBridge(t, 2, succeedingCreateEscrowFn(200))
	open := store.Commitment{TxHash: "TX-OPEN", Model: bridgeSizingModel, Role: roleTemp, Epoch: bridgeLabel(bridge.snapshot), PrivateKeyEnv: "MODEL_A_KEY"}
	require.NoError(t, bridge.store.SaveCommitment(context.Background(), open))

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Equal(t, 2, *bridge.attempts, "creates attempted")
}

// Test flow:
//  1. Seat four temps of the bridge label and four parked rows the chain still holds, with a last decision that asks for three more and a max_unsettled of twelve.
//  2. Call `prepareBridge`.
//  3. Assert two creates were attempted, the room the unsettled rows leave, and not the three asked for.
func TestTheBridgeHoldsTheRefillToTheRoomUnderMaxUnsettled(t *testing.T) {
	bridge := newRefillBridge(t, 3, succeedingCreateEscrowFn(200))
	for _, escrowID := range []string{"parked-1", "parked-2", "parked-3", "parked-4"} {
		bridge.park(escrowID)
	}
	bridge.devshards = nil
	for _, record := range bridge.store.devshards {
		bridge.devshards = append(bridge.devshards, record)
	}

	bridge.prepare(t, 12)

	require.Equal(t, 2, *bridge.attempts, "creates attempted")
}

// Test flow:
//  1. Seat four temps of the bridge label with a last decision that asks for five more and a max_unsettled that lets the bridge hold seven temps.
//  2. Call `prepareBridge`.
//  3. Assert three creates were attempted: the ceiling that keeps room for the next epoch's regulars and reserves still holds.
func TestTheBridgeKeepsTheCeilingThatLeavesRoomForTheNextEpoch(t *testing.T) {
	bridge := newRefillBridge(t, 5, succeedingCreateEscrowFn(200))

	bridge.prepare(t, 12)

	require.Equal(t, 3, *bridge.attempts, "creates attempted")
	require.Equal(t, 7, countTemps(bridge.store), "temps")
}

// Test flow:
//  1. Seat four temps of the bridge label with a last decision that asks for two more, and a snapshot whose network serves no such model.
//  2. Call `prepareBridge`.
//  3. Assert no create was attempted and the four temps and both regulars are left as they were.
func TestTheRefillSkipsAModelTheNetworkDoesNotServe(t *testing.T) {
	bridge := newRefillBridge(t, 2, failOnCreate(t))
	bridge.snapshot = servedSnapshot(9, 500, "other-model")

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Zero(t, *bridge.attempts, "creates attempted")
	require.Equal(t, 4, countTemps(bridge.store), "temps")
}

// Test flow:
//  1. Seat four temps of the bridge label with a last decision that asks for two more and an armed temp create breaker.
//  2. Call `prepareBridge`.
//  3. Assert no create was attempted, the call returns nil and the regulars are retired, as with any top-up the breaker stops.
func TestTheRefillRespectsTheTempCreateBreaker(t *testing.T) {
	bridge := newRefillBridge(t, 2, failOnCreate(t))
	bridge.manager.breaker.recordFailure(bridgeSizingModel, roleTemp)

	bridge.prepare(t, bridgeSizingRoomyMaxUnsettled)

	require.Zero(t, *bridge.attempts, "creates attempted")
	require.False(t, bridge.store.devshards["reg-1"].Active, "regular retired")
}

// Test flow:
//  1. Decide one in-window tick of a model whose temps are all starved and whose demand needs more money, with a full bucket.
//  2. Read the refill the bridge takes, then read it again.
//  3. Assert the first read is the decision's refill, above zero, the second is zero, and the decision spent no bucket token.
func TestTheBridgeTakesTheDecisionsRefillOnceWithoutSpendingTheBucket(t *testing.T) {
	planner := fundingPlanner{models: map[string]*modelFunding{}, escrows: map[string]*escrowFunding{}}
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	reading := guaranteeReading(false, false)
	reading.state.InBridgeWindow, reading.state.TempCount, reading.state.MoneyShort = true, 1, true
	reading.state.Escrows[0].Temp, reading.state.Escrows[0].Label = true, 8

	_, first, _ := planner.decide("qwen", reading, true, now)
	taken := planner.takeBridgeRefill("qwen")
	again := planner.takeBridgeRefill("qwen")

	require.Positive(t, first.BridgeRefill, "decision refill")
	require.Equal(t, first.BridgeRefill, taken, "taken refill")
	require.Zero(t, again, "second read")
	fresh := funding.NewBucket(now)
	require.Equal(t, fresh.Available(), planner.models["qwen"].bucket.Available(), "bucket tokens left")
}

func drainedBridgeManager(t *testing.T, tempCount int, mutate func(snapshot *chain.PhaseSnapshot, cfg *config.Config)) (*Manager, *fakeTxClient, *fakeStore) {
	t.Helper()
	testStore := newFakeStore()
	snapshot := planningSnapshot()
	snapshot.EpochSwitchBlockHeight, snapshot.FullWeightsByModel = snapshot.BlockHeight+200, map[string]map[string]float64{"qwen": {"participant": 1}}
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	cfg.Rotation.ModelsJSON = fmt.Sprintf(`[{"model_id":"qwen","target_count":1,"temp_count":%d,"reserve_count":0,"amount":1000000,"private_key_env":"K","max_unsettled":10}]`, tempCount)
	mutate(&snapshot, &cfg)
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(800)}
	deps := testManagerDeps(t, testStore, txClient, &fakeSnapshotSource{snapshot: snapshot}, &cfg)
	deps.ChainFacts = &fakeTxClient{getEscrowFn: func(_ context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
		return chain.EscrowInfo{EscrowID: escrowID, EpochIndex: 7, Balance: 1_000_000}, true, nil
	}}
	drained := EscrowMoney{Config: types.SessionConfig{RefusalTimeout: 60, ExecutionTimeout: 1800}, Balance: 40_000, TokenPrice: 1, FeePerNonce: 10}
	funds := fakeFunds{}
	deps.Funds = funds
	for index := 1; index <= tempCount; index++ {
		escrowID := fmt.Sprint(index)
		funds[escrowID] = drained
		testStore.devshards[escrowID] = store.DevshardRecord{EscrowID: escrowID, Model: "qwen", Active: true, RotationRole: roleTemp, RotationEpoch: 8, PrivateKeyEnv: "K", Amount: 1_000_000}
	}
	return mustManager(t, deps), txClient, testStore
}

// Test flow:
//  1. Build a planning manager in the bridge window with two temps of the bridge label, the full temp count, each holding less than a slot, and a chain that reports every new escrow at the model's amount.
//  2. Tick once, so the planner reads the drained temps and the bridge refills them in the same tick, then tick twice more.
//  3. Assert the first tick created two temps, the two full escrows the guarantee wants, and the second and third created nothing: the new temps' stored amounts cover the shortfall.
func TestDrainedTempsInTheBridgeWindowAreRefilledOnceAcrossTicks(t *testing.T) {
	manager, txClient, _ := drainedBridgeManager(t, 2, func(*chain.PhaseSnapshot, *config.Config) {})

	require.NoError(t, manager.tick(context.Background()))
	afterFirst := txClient.createCalls
	require.NoError(t, manager.tick(context.Background()))
	afterSecond := txClient.createCalls
	require.NoError(t, manager.tick(context.Background()))

	require.Equal(t, 2, afterFirst, "creates after the first tick")
	require.Equal(t, 2, afterSecond, "creates after the second tick")
	require.Equal(t, 2, txClient.createCalls, "creates after the third tick")
}

// Test flow:
//  1. Build a bridge window with one drained temp, the full temp count, during an epoch PoC that blocks requests, then the same with requests served in the PoC.
//  2. Tick once.
//  3. Assert both runs created exactly one temp: the bridge refills in a PoC as it creates there, and never past the temp count.
func TestDrainedTempsAreRefilledDuringAnEpochPoCUpToTheTempCount(t *testing.T) {
	for _, mode := range []string{config.PoCModeOff, config.PoCModeRelaxed} {
		t.Run(mode, func(t *testing.T) {
			manager, txClient, testStore := drainedBridgeManager(t, 1, func(snapshot *chain.PhaseSnapshot, cfg *config.Config) {
				snapshot.RequestsBlocked, snapshot.BlockReason = true, chain.BlockReasonPoC
				cfg.Modes.PoCMode = mode
			})

			require.NoError(t, manager.tick(context.Background()))
			require.NoError(t, manager.tick(context.Background()))

			require.Equal(t, 1, txClient.createCalls, "creates")
			require.Len(t, testStore.devshards, 2)
		})
	}
}

// Test flow:
//  1. Build a bridge window with one drained temp, a temp count of one and a recorded demand that needs three fresh escrows.
//  2. Tick once.
//  3. Assert the bridge created three temps, four in all: the demand-driven target is not held to the refill's bucket or temp count.
func TestTheBridgeKeepsTheDemandDrivenTempCountBeyondTheRefill(t *testing.T) {
	manager, txClient, testStore := drainedBridgeManager(t, 1, func(*chain.PhaseSnapshot, *config.Config) {})
	manager.planner.modelLocked("qwen", manager.now()).demand.Record(1_800_000)

	require.NoError(t, manager.tick(context.Background()))
	require.NoError(t, manager.tick(context.Background()))

	require.Equal(t, 3, txClient.createCalls, "creates")
	require.Len(t, testStore.devshards, 4)
}

// Test flow:
//  1. Build a bridge window with two models of one temp each, the qwen temp drained and stored with a refill of one, and llama with no stored refill and no temps.
//  2. Call `refillBridge`.
//  3. Assert exactly one create landed, for qwen, and llama has no devshard: the second pass skips a model with no pending refill.
func TestTheSecondPassPreparesOnlyTheModelsWithAPendingRefill(t *testing.T) {
	manager, txClient, testStore := drainedBridgeManager(t, 1, func(snapshot *chain.PhaseSnapshot, cfg *config.Config) {
		snapshot.Models["llama"] = chain.ModelParams{MaxModelLen: 8192}
		snapshot.FullWeightsByModel["llama"] = map[string]float64{"participant": 1}
		cfg.Rotation.ModelsJSON = `[{"model_id":"qwen","target_count":1,"temp_count":1,"reserve_count":0,"amount":1000000,"private_key_env":"K","max_unsettled":10},{"model_id":"llama","target_count":1,"temp_count":1,"reserve_count":0,"amount":1000000,"private_key_env":"K","max_unsettled":10}]`
	})
	manager.planner.modelLocked("qwen", manager.now()).bridgeRefill = 1
	manager.planner.modelLocked("llama", manager.now())

	require.NoError(t, manager.refillBridge(context.Background()))

	require.Equal(t, 1, txClient.createCalls, "creates")
	created := map[string]int{}
	for _, record := range testStore.devshards {
		created[record.Model]++
	}
	require.Equal(t, map[string]int{"qwen": 2}, created, "devshards by model")
}
