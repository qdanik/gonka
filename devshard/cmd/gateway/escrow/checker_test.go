package escrow

import (
	"context"
	"errors"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
)

// INVARIANT 8: only a positive "chain confirms not found" resolves to deactivation.
func TestTriggerEscrowCheckNotFoundDeactivates(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true}
	txClient := &fakeTxClient{
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			return chain.EscrowInfo{}, false, nil
		},
	}
	m := &Manager{tx: txClient, store: testStore}

	if err := m.TriggerEscrowCheck(context.Background(), "1"); err != nil {
		t.Fatalf("TriggerEscrowCheck() = %v, want nil", err)
	}
	if got := testStore.devshards["1"]; got.Active {
		t.Fatal("devshard.Active = true, want false (chain confirmed the escrow does not exist)")
	}
}

// INVARIANT 8: a found escrow means the host's "missing" report was false -- no state change.
func TestTriggerEscrowCheckFoundKeepsActive(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true}
	log := &callLog{}
	testStore.calls = log
	txClient := &fakeTxClient{
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			return chain.EscrowInfo{EscrowID: escrowID, Balance: 500}, true, nil
		},
	}
	m := &Manager{tx: txClient, store: testStore}

	if err := m.TriggerEscrowCheck(context.Background(), "1"); err != nil {
		t.Fatalf("TriggerEscrowCheck() = %v, want nil", err)
	}
	if got := testStore.devshards["1"]; !got.Active {
		t.Fatal("devshard.Active = false, want true (escrow found, no reason to deactivate)")
	}
	for _, name := range log.snapshot() {
		if name == "SetDevshardActive(false)" {
			t.Fatal("SetDevshardActive(false) called, want no deactivation when the escrow is found")
		}
	}
}

// INVARIANT 8: ambiguity (a lookup failure) must never be read as confirmation -- keep active.
func TestTriggerEscrowCheckChainErrorKeepsActive(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true}
	log := &callLog{}
	testStore.calls = log
	chainErr := errors.New("endpoint unreachable")
	txClient := &fakeTxClient{
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			return chain.EscrowInfo{}, false, chainErr
		},
	}
	m := &Manager{tx: txClient, store: testStore}

	err := m.TriggerEscrowCheck(context.Background(), "1")
	if err == nil || !errors.Is(err, chainErr) {
		t.Fatalf("TriggerEscrowCheck() = %v, want wrapped %v", err, chainErr)
	}
	if got := testStore.devshards["1"]; !got.Active {
		t.Fatal("devshard.Active = false, want true (a lookup failure is ambiguity, never a reason to deactivate)")
	}
	for _, name := range log.snapshot() {
		if name == "SetDevshardActive(false)" {
			t.Fatal("SetDevshardActive(false) called, want no deactivation on chain error")
		}
	}
}

// INVARIANT 8 + dedup: N concurrent checks for the same escrow must resolve to exactly one
// chain lookup and one deactivation -- a concurrent check finding the flag already set is a no-op.
func TestTriggerEscrowCheckDedupesConcurrentCallers(t *testing.T) {
	const callerCount = 10
	testStore := newFakeStore()
	testStore.devshards["1"] = store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true}
	log := &callLog{}
	testStore.calls = log

	release := make(chan struct{})
	txClient := &fakeTxClient{
		calls: log,
		getEscrowFn: func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
			<-release
			return chain.EscrowInfo{}, false, nil
		},
	}
	m := &Manager{tx: txClient, store: testStore}

	done := make(chan struct{}, callerCount)
	for i := 0; i < callerCount; i++ {
		go func() {
			if err := m.TriggerEscrowCheck(context.Background(), "1"); err != nil {
				t.Errorf("TriggerEscrowCheck() = %v, want nil", err)
			}
			done <- struct{}{}
		}()
	}

	// The callerCount-1 deduped callers return without touching the chain; only the
	// winner blocks on release, so draining callerCount-1 signals first is deterministic.
	for i := 0; i < callerCount-1; i++ {
		<-done
	}
	close(release)
	<-done

	getEscrowCalls, deactivateCalls := 0, 0
	for _, name := range log.snapshot() {
		switch name {
		case "GetEscrow":
			getEscrowCalls++
		case "SetDevshardActive(false)":
			deactivateCalls++
		}
	}
	if getEscrowCalls != 1 {
		t.Fatalf("GetEscrow called %d times, want exactly 1", getEscrowCalls)
	}
	if deactivateCalls != 1 {
		t.Fatalf("SetDevshardActive(false) called %d times, want exactly 1", deactivateCalls)
	}
}
