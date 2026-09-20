package escrow

import (
	"context"
	"errors"
	"testing"

	"devshard/cmd/gateway/store"
)

func busyManager(t *testing.T, record store.DevshardRecord, log *callLog) (*Manager, *fakeStore) {
	t.Helper()
	testStore := newFakeStore()
	testStore.devshards[record.EscrowID] = record
	testStore.calls = log
	txClient := settlingTxClient()
	txClient.calls = log
	txClient.settleTxHash = "SETTLE-TX"
	return &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{busy: true, calls: log},
	}, testStore
}

// The operator's override: the escrow is closed now, and the requests still in flight are paid for and
// failed. Finalize shuts the door on new nonces, and the live records take the settlement default.
func TestForcedSettleProceedsWithRequestsStillInFlight(t *testing.T) {
	record := store.DevshardRecord{EscrowID: "11", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	log := &callLog{}
	manager, testStore := busyManager(t, record, log)

	if _, err := manager.settle(context.Background(), record, true); err != nil {
		t.Fatalf("forced settle() = %v, want nil", err)
	}

	if !log.contains("SettleEscrow") {
		t.Errorf("call log = %v, want the broadcast to have happened", log.snapshot())
	}
	if settled := testStore.devshards[record.EscrowID]; settled.SettlementPending {
		t.Error("SettlementPending = true, want it cleared by the confirmed broadcast")
	}
}

// Unforced, the refusal stands: the escrow is parked and drains, and nothing is broadcast.
func TestUnforcedSettleStillDefersWhileBusy(t *testing.T) {
	record := store.DevshardRecord{EscrowID: "12", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	log := &callLog{}
	manager, _ := busyManager(t, record, log)

	_, err := manager.settle(context.Background(), record, false)

	if !errors.Is(err, ErrDevshardBusy) {
		t.Fatalf("settle() = %v, want ErrDevshardBusy", err)
	}
	if log.contains("SettleEscrow") {
		t.Error("a busy escrow must not be broadcast without force")
	}
}

// Force overrides a busy escrow, never a second settlement of the same one: that guard stops a double
// broadcast rather than reporting load.
func TestForcedSettleStillRefusesAConcurrentSettlement(t *testing.T) {
	record := store.DevshardRecord{EscrowID: "13", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	log := &callLog{}
	manager, _ := busyManager(t, record, log)
	leave, alreadyRunning := manager.settlements.enter(record.EscrowID)
	if alreadyRunning {
		t.Fatal("setup: the set must be free before the test claims it")
	}
	defer leave()

	_, err := manager.settle(context.Background(), record, true)

	if !errors.Is(err, ErrSettlementInFlight) {
		t.Fatalf("forced settle() = %v, want ErrSettlementInFlight", err)
	}
	if log.contains("SettleEscrow") {
		t.Error("force must not cross the double-broadcast guard")
	}
}
