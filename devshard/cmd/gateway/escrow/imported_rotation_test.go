package escrow

import (
	"context"
	"testing"

	"devshard/cmd/gateway/store"
)

// Test flow:
//  1. Persist a temp commitment for model-a at epoch 5, as a rotation-created escrow is, and read its row back.
//  2. Build the row an import carrying that row's role, epoch and route prefix stores, and the row an import without them stores.
//  3. Assert `countActive` counts the rotation-created row and the imported row once each, and the bare row not at all.
//  4. Call `fillToLabel` for temps at epoch 5 with a target of 1 over the imported row, with a chain client that fails the test on a create.
//  5. Assert it creates nothing, so the rotation plan funds no new set after a handover.
func TestAnImportedEscrowCarryingItsRotationIdentityIsCountedLikeARotationCreatedOne(t *testing.T) {
	ctx := context.Background()
	testStore := newFakeStore()
	m := newRotationManager(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, false)
	m.routePrefix = "/devshard/v7"
	if err := m.persistEscrow(ctx, "77", store.Commitment{TxHash: "TX-ROTATED", Model: "model-a", Role: roleTemp, Epoch: 5, PrivateKeyEnv: "KEY_A"}); err != nil {
		t.Fatalf("persistEscrow(): %v", err)
	}
	created, err := testStore.ListDevshards(ctx)
	if err != nil || len(created) != 1 {
		t.Fatalf("ListDevshards() = %+v, %v, want exactly 1 record", created, err)
	}
	imported := created[0]
	imported.EscrowID = "88"
	bare := imported
	bare.EscrowID = "99"
	bare.RotationRole, bare.RotationEpoch, bare.RoutePrefix = store.RoleUnset, 0, ""

	if got := countActive(created, "model-a", roleTemp, 5); got != 1 {
		t.Errorf("countActive(rotation-created) = %d, want 1", got)
	}
	if got := countActive([]store.DevshardRecord{imported}, "model-a", roleTemp, 5); got != 1 {
		t.Errorf("countActive(imported with identity) = %d, want 1", got)
	}
	if got := countActive([]store.DevshardRecord{bare}, "model-a", roleTemp, 5); got != 0 {
		t.Errorf("countActive(imported without identity) = %d, want 0", got)
	}
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "KEY_A"}
	snapshot := servedSnapshot(5, 100, "model-a")
	fill := fillFromStore(t, m, ctx, roleTemp, snapshot.EpochIndex, []ModelConfig{model}, func(ModelConfig) int { return 1 }, snapshot, []store.DevshardRecord{imported})[0]
	if fill.err != nil || fill.created != 0 {
		t.Errorf("fillToLabel(imported) = created %d, err %v, want 0, nil", fill.created, fill.err)
	}
}
