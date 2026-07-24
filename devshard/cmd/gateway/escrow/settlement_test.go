package escrow

import (
	"context"
	"errors"
	"sync"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"
	"devshard/signing"
)

// callLog records call names in the order they happen, shared across fakes to
// pin cross-object ordering under concurrent access.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (c *callLog) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, name)
}

func (c *callLog) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// fakeSettlementSource stubs SettlementSource.
type fakeSettlementSource struct {
	busy        bool
	finalizeErr error
	buildErr    error
	buildInput  chain.SettlementInput
	calls       *callLog
}

func (f *fakeSettlementSource) IsBusy(escrowID string) bool {
	if f.calls != nil {
		f.calls.record("IsBusy")
	}
	return f.busy
}

func (f *fakeSettlementSource) Finalize(ctx context.Context, escrowID string) error {
	if f.calls != nil {
		f.calls.record("Finalize")
	}
	return f.finalizeErr
}

func (f *fakeSettlementSource) BuildSettlement(ctx context.Context, escrowID string) (chain.SettlementInput, error) {
	if f.calls != nil {
		f.calls.record("BuildSettlement")
	}
	if f.buildErr != nil {
		return chain.SettlementInput{}, f.buildErr
	}
	return f.buildInput, nil
}

func holderWithSettlementEnabled(enabled bool) *config.Holder {
	cfg := config.Defaults()
	cfg.Rotation.SettlementEnabled = enabled
	return config.NewHolder(&cfg)
}

func stringsEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// busy is a deferred-settle signal, not a failure: the escrow is deactivated (so it can drain)
// and marked pending, but no finalize/build/broadcast happens while it is busy.
func TestSettleBusyMarksPendingAndReturnsErrDevshardBusy(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "1", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	log := &callLog{}
	testStore.calls = log
	settlementSource := &fakeSettlementSource{busy: true, calls: log}
	txClient := &fakeTxClient{calls: log}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: settlementSource,
	}

	err := m.settle(context.Background(), record)
	if !errors.Is(err, errDevshardBusy) {
		t.Fatalf("settle() = %v, want errDevshardBusy", err)
	}

	want := []string{"SetDevshardActive(false)", "SetDevshardSettlementPending(true)", "IsBusy"}
	if got := log.snapshot(); !stringsEqual(got, want) {
		t.Fatalf("call log = %v, want %v (deactivate + mark pending, then busy short-circuits before any chain call)", got, want)
	}
	if got := testStore.devshards[record.EscrowID]; !got.SettlementPending || got.Active {
		t.Fatalf("devshard = %+v, want SettlementPending=true and Active=false (deactivated so it can drain)", got)
	}
}

// INVARIANT 2: deactivation happens before any chain call, and the pending
// marker clears only after a confirmed broadcast.
func TestSettleHappyPathOrderAndClearsPendingOnSuccess(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "2", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	log := &callLog{}
	testStore.calls = log
	settlementSource := &fakeSettlementSource{busy: false, calls: log, buildInput: chain.SettlementInput{EscrowID: 2}}
	txClient := &fakeTxClient{
		calls: log,
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: settlementSource,
	}

	if err := m.settle(context.Background(), record); err != nil {
		t.Fatalf("settle() = %v, want nil", err)
	}

	want := []string{
		"SetDevshardActive(false)", "SetDevshardSettlementPending(true)", "IsBusy",
		"Finalize", "BuildSettlement", "SettleEscrow", "SetDevshardSettlementPending(false)",
	}
	if got := log.snapshot(); !stringsEqual(got, want) {
		t.Fatalf("call log = %v, want %v", got, want)
	}
	got := testStore.devshards[record.EscrowID]
	if got.Active {
		t.Fatalf("devshard.Active = true, want false (deactivated)")
	}
	if got.SettlementPending {
		t.Fatalf("devshard.SettlementPending = true, want false (cleared on success)")
	}
}

// INVARIANT 2: any broadcast failure must leave SettlementPending set --
// clearing it is the one thing that only happens on confirmed success.
func TestSettleBroadcastFailureLeavesSettlementPendingSet(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "3", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	settlementSource := &fakeSettlementSource{busy: false}
	broadcastErr := errors.New("broadcast rejected")
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			return chain.SettleEscrowResult{}, broadcastErr
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: settlementSource,
	}

	err := m.settle(context.Background(), record)
	if err == nil || !errors.Is(err, broadcastErr) {
		t.Fatalf("settle() = %v, want wrapped %v", err, broadcastErr)
	}
	got := testStore.devshards[record.EscrowID]
	if !got.SettlementPending {
		t.Fatal("devshard.SettlementPending = false, want true (never cleared on failure)")
	}
	if got.Active {
		t.Fatal("devshard.Active = true, want false (deactivation already happened before the broadcast)")
	}
}

// retire with the toggle off never touches the chain -- deactivate and drop the row.
func TestRetireSettlementDisabledSkipsChainDeactivatesAndDeletes(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "4", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			t.Fatal("SettleEscrow must not be called when the settlement toggle is off")
			return chain.SettleEscrowResult{}, nil
		},
	}

	m := &Manager{
		tx:     txClient,
		store:  testStore,
		signer: &fakeSignerSource{signer: testSigner(t)},
		config: holderWithSettlementEnabled(false),
	}

	if err := m.retire(context.Background(), record); err != nil {
		t.Fatalf("retire() = %v, want nil", err)
	}
	if _, ok := testStore.devshards[record.EscrowID]; ok {
		t.Fatal("devshard still present after retire(), want deleted")
	}
}

// retire with the toggle on settles first, then drops the row only after settle succeeds.
func TestRetireSettlementEnabledHappyPathSettlesThenDeletes(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "5", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	settlementSource := &fakeSettlementSource{busy: false}
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: settlementSource,
		config:           holderWithSettlementEnabled(true),
	}

	if err := m.retire(context.Background(), record); err != nil {
		t.Fatalf("retire() = %v, want nil", err)
	}
	if _, ok := testStore.devshards[record.EscrowID]; ok {
		t.Fatal("devshard still present after retire(), want deleted once settled")
	}
}

// retire must not delete the row when settle fails (including the busy case):
// the escrow stays registered, inactive, and pending for a later deferred-settle completion.
func TestRetireSettlementEnabledBusyLeavesDevshardRegistered(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "6", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	settlementSource := &fakeSettlementSource{busy: true}
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			t.Fatal("SettleEscrow must not be called while busy")
			return chain.SettleEscrowResult{}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: settlementSource,
		config:           holderWithSettlementEnabled(true),
	}

	err := m.retire(context.Background(), record)
	if !errors.Is(err, errDevshardBusy) {
		t.Fatalf("retire() = %v, want errDevshardBusy", err)
	}
	got, ok := testStore.devshards[record.EscrowID]
	if !ok {
		t.Fatal("devshard deleted after a busy retire(), want it kept registered")
	}
	if !got.SettlementPending {
		t.Fatal("devshard.SettlementPending = false, want true (deferred-settle marker left set)")
	}
}

// INVARIANT 7: two concurrent settle calls for the same escrow must broadcast exactly once.
func TestSettleDedupesConcurrentCallsForSameEscrow(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "7", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	settlementSource := &fakeSettlementSource{busy: false}

	entered := make(chan struct{})
	release := make(chan struct{})
	log := &callLog{}
	txClient := &fakeTxClient{
		calls: log,
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			close(entered)
			<-release
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: settlementSource,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.settle(context.Background(), record); err != nil {
			t.Errorf("first settle() = %v, want nil", err)
		}
	}()

	<-entered
	if err := m.settle(context.Background(), record); err != nil {
		t.Fatalf("second concurrent settle() = %v, want nil (already in flight is a no-op)", err)
	}
	close(release)
	wg.Wait()

	broadcastCount := 0
	for _, name := range log.snapshot() {
		if name == "SettleEscrow" {
			broadcastCount++
		}
	}
	if broadcastCount != 1 {
		t.Fatalf("SettleEscrow called %d times, want exactly 1", broadcastCount)
	}
}
