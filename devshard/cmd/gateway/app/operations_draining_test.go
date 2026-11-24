package app

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/registry"
)

type noHandover struct{}

func (noHandover) HandOver(string) {}

func (noHandover) Resume(string) bool { return false }

type recordedHandover struct {
	mu         sync.Mutex
	calls      []string
	handedOver map[string]bool
}

func (r *recordedHandover) HandOver(escrowID string) {
	r.record("hand over " + escrowID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handedOver == nil {
		r.handedOver = map[string]bool{}
	}
	r.handedOver[escrowID] = true
}

func (r *recordedHandover) Resume(escrowID string) bool {
	r.record("resume " + escrowID)
	r.mu.Lock()
	defer r.mu.Unlock()
	was := r.handedOver[escrowID]
	delete(r.handedOver, escrowID)
	return was
}

func (r *recordedHandover) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordedHandover) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

const drainReleaseWait = 2 * time.Second

// Test flow:
//  1. Register escrow 42 active and hold its session as a race owing a vote would.
//  2. Hand it over, then Activate it while the hold is still taken.
//  3. Assert Activate answers registry.ErrDraining, the row reads inactive, nothing routes the escrow and the engine was told to hand it over again.
//  4. Release the hold, wait for the session to close, and Activate again.
//  5. Assert the second Activate succeeds, the row reads active and the escrow routes.
func TestActivateRefusedWhileDrainingLeavesTheRowInactiveAndAnActivateAfterTheReleaseServes(t *testing.T) {
	ctx := context.Background()
	t.Setenv("DEVSHARD_IMPORT_KEY", "0x01")
	records := openedStore(t)
	escrows := servingRegistry(t)
	races := &recordedHandover{}
	operator := &operations{races: races, store: records, escrows: escrows, storageDir: t.TempDir()}
	if err := operator.AddDevshard(ctx, api.AddDevshardRequest{EscrowID: "42", Model: "model-a", PrivateKeyEnv: "DEVSHARD_IMPORT_KEY", Activate: true}); err != nil {
		t.Fatalf("AddDevshard(active) = %v", err)
	}
	races.mu.Lock()
	races.calls = nil
	races.mu.Unlock()
	_, release, held := escrows.HoldSettlement("42")
	if !held {
		t.Fatal("HoldSettlement(42) = not held, want the live session held")
	}
	if err := operator.HandOver(ctx, "42"); err != nil {
		t.Fatalf("HandOver = %v", err)
	}

	if err := operator.Activate(ctx, "42"); !errors.Is(err, registry.ErrDraining) {
		t.Fatalf("Activate while draining = %v, want registry.ErrDraining", err)
	}
	if storedDevshard(t, records, "42").Active {
		t.Fatal("the row reads active after a refused Activate, want inactive")
	}
	if _, routable := escrows.Routable("42"); routable {
		t.Fatal("escrow 42 routes after a refused Activate")
	}
	if got, want := races.recorded(), []string{"hand over 42", "resume 42", "hand over 42"}; !slices.Equal(got, want) {
		t.Fatalf("engine calls after the refused Activate = %v, want %v", got, want)
	}

	release()
	deadline := time.Now().Add(drainReleaseWait)
	for escrows.HoldsSession("42") {
		if time.Now().After(deadline) {
			t.Fatalf("escrow 42 still held %s after its hold was released", drainReleaseWait)
		}
		time.Sleep(time.Millisecond)
	}
	if err := operator.Activate(ctx, "42"); err != nil {
		t.Fatalf("Activate after the release = %v", err)
	}
	if !storedDevshard(t, records, "42").Active {
		t.Fatal("the row reads inactive after a successful Activate")
	}
	if _, routable := escrows.Routable("42"); !routable {
		t.Fatal("escrow 42 does not route after a successful Activate")
	}
}

// Test flow:
//  1. Register escrow 42 active with a recording engine and clear what registering recorded.
//  2. Deactivate it, Activate it, hand it over and Activate it again.
//  3. Assert the engine was told nothing for the plain deactivation, to resume for each activation and to hand over for the handover.
func TestOnlyAHandoverTellsTheEngineAndEveryActivationResumes(t *testing.T) {
	ctx := context.Background()
	t.Setenv("DEVSHARD_IMPORT_KEY", "0x01")
	records := openedStore(t)
	races := &recordedHandover{}
	operator := &operations{races: races, store: records, escrows: servingRegistry(t), storageDir: t.TempDir()}
	if err := operator.AddDevshard(ctx, api.AddDevshardRequest{EscrowID: "42", Model: "model-a", PrivateKeyEnv: "DEVSHARD_IMPORT_KEY", Activate: true}); err != nil {
		t.Fatalf("AddDevshard(active) = %v", err)
	}
	races.mu.Lock()
	races.calls = nil
	races.mu.Unlock()

	if err := operator.Deactivate(ctx, "42"); err != nil {
		t.Fatalf("Deactivate = %v", err)
	}
	if err := operator.Activate(ctx, "42"); err != nil {
		t.Fatalf("Activate = %v", err)
	}
	if err := operator.HandOver(ctx, "42"); err != nil {
		t.Fatalf("HandOver = %v", err)
	}
	if err := operator.Activate(ctx, "42"); err != nil {
		t.Fatalf("Activate after the handover = %v", err)
	}

	want := []string{"resume 42", "hand over 42", "resume 42"}
	if got := races.recorded(); !slices.Equal(got, want) {
		t.Fatalf("engine calls = %v, want %v", got, want)
	}
}

// Test flow:
//  1. Register escrow 42 active, hold its session as a race owing a vote would, and clear what registering recorded.
//  2. Deactivate it without a handover, then Activate it while the hold is still taken.
//  3. Assert Activate answers registry.ErrDraining, the row reads inactive and the engine was only told to resume, never to hand the escrow over.
func TestActivateRefusedWhileDrainingAfterAPlainDeactivationDoesNotHandTheEscrowOver(t *testing.T) {
	ctx := context.Background()
	t.Setenv("DEVSHARD_IMPORT_KEY", "0x01")
	records := openedStore(t)
	escrows := servingRegistry(t)
	races := &recordedHandover{}
	operator := &operations{races: races, store: records, escrows: escrows, storageDir: t.TempDir()}
	if err := operator.AddDevshard(ctx, api.AddDevshardRequest{EscrowID: "42", Model: "model-a", PrivateKeyEnv: "DEVSHARD_IMPORT_KEY", Activate: true}); err != nil {
		t.Fatalf("AddDevshard(active) = %v", err)
	}
	races.mu.Lock()
	races.calls = nil
	races.mu.Unlock()
	_, release, held := escrows.HoldSettlement("42")
	if !held {
		t.Fatal("HoldSettlement(42) = not held, want the live session held")
	}
	defer release()
	if err := operator.Deactivate(ctx, "42"); err != nil {
		t.Fatalf("Deactivate = %v", err)
	}

	if err := operator.Activate(ctx, "42"); !errors.Is(err, registry.ErrDraining) {
		t.Fatalf("Activate while draining = %v, want registry.ErrDraining", err)
	}
	if storedDevshard(t, records, "42").Active {
		t.Fatal("the row reads active after a refused Activate, want inactive")
	}
	if got, want := races.recorded(), []string{"resume 42"}; !slices.Equal(got, want) {
		t.Fatalf("engine calls after the refused Activate = %v, want %v", got, want)
	}
}
