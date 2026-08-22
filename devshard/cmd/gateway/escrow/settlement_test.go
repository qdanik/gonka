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

// fakeSettlementSource models the registry: Retire stops routing synchronously, and commit stands in
// for the request path taking a nonce -- refused once retired, counted as in-flight work until then.
type fakeSettlementSource struct {
	mu        sync.Mutex
	retired   bool
	committed int

	busy        bool
	finalizeErr error
	buildErr    error
	buildInput  chain.SettlementInput
	calls       *callLog
}

func (f *fakeSettlementSource) Retire(escrowID string) error {
	if f.calls != nil {
		f.calls.record("Retire")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retired = true
	return nil
}

func (f *fakeSettlementSource) commit() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.retired {
		return false
	}
	f.committed++
	return true
}

func (f *fakeSettlementSource) IsBusy(escrowID string) bool {
	if f.calls != nil {
		f.calls.record("IsBusy")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.busy || f.committed > 0
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

	_, err := m.settle(context.Background(), record)
	if !errors.Is(err, ErrDevshardBusy) {
		t.Fatalf("settle() = %v, want ErrDevshardBusy", err)
	}

	want := []string{"ParkForSettlement", "Retire", "IsBusy"}
	if got := log.snapshot(); !stringsEqual(got, want) {
		t.Fatalf("call log = %v, want %v (park + stop routing, then busy short-circuits before any chain call)", got, want)
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
		calls:        log,
		settleTxHash: "SETTLE-TX",
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

	if _, err := m.settle(context.Background(), record); err != nil {
		t.Fatalf("settle() = %v, want nil", err)
	}

	// The hash is recorded inside SettleEscrow, before the broadcast: a settle that commits while the
	// gateway is not looking must be recognisable on the next tick rather than rebuilt.
	want := []string{
		"ParkForSettlement", "Retire", "IsBusy",
		"Finalize", "BuildSettlement", "SettleEscrow", `SetDevshardSettleTxHash("SETTLE-TX")`,
		"SetDevshardSettlementPending(false)",
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

	_, err := m.settle(context.Background(), record)
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

// retire with the toggle off never touches the chain: the escrow is parked, not dropped, because its
// row carries the only key that can settle it later.
func TestRetireSettlementDisabledSkipsChainAndParksEscrow(t *testing.T) {
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
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		config:           holderWithSettlementEnabled(false),
	}

	if err := m.retire(context.Background(), record); err != nil {
		t.Fatalf("retire() = %v, want nil", err)
	}
	assertParked(t, testStore, record.EscrowID)
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
	if !errors.Is(err, ErrDevshardBusy) {
		t.Fatalf("retire() = %v, want ErrDevshardBusy", err)
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
	wg.Go(func() {
		if _, err := m.settle(context.Background(), record); err != nil {
			t.Errorf("first settle() = %v, want nil", err)
		}
	})

	<-entered
	if _, err := m.settle(context.Background(), record); !errors.Is(err, ErrSettlementInFlight) {
		t.Fatalf("second concurrent settle() = %v, want ErrSettlementInFlight", err)
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

// A settlement claims the escrow's funds, so by the time it is broadcast the escrow must already be
// out of routing: a nonce committed after that point can never be settled.
func TestSettleRefusesNonceCommitsOnceTheSettlementIsBroadcast(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "20", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record
	routing := &fakeSettlementSource{}
	committedDuringBroadcast := true
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			committedDuringBroadcast = routing.commit()
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: routing,
	}

	if _, err := m.settle(context.Background(), record); err != nil {
		t.Fatalf("settle() = %v, want nil", err)
	}
	if committedDuringBroadcast {
		t.Fatal("a nonce was committed on an escrow whose settlement was already being broadcast")
	}
}

// The deduped caller must leave the row alone: it carries the private-key env name that is the only
// way to settle the escrow, and it belongs to the caller that is really settling.
func TestDedupedSettleLeavesTheRowForTheCallerThatIsReallySettling(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "21", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true}
	testStore.devshards[record.EscrowID] = record

	entered := make(chan struct{})
	release := make(chan struct{})
	txClient := &fakeTxClient{
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
		settlementSource: &fakeSettlementSource{},
		config:           holderWithSettlementEnabled(true),
	}

	var settling sync.WaitGroup
	settling.Go(func() {
		if err := m.retire(context.Background(), record); err != nil {
			t.Errorf("settling retire() = %v, want nil", err)
		}
	})

	<-entered
	dedupedErr := m.retire(context.Background(), record)
	kept, present := testStore.snapshotDevshard(record.EscrowID)
	if !present {
		t.Fatal("the deduped caller deleted the row while the real settlement was still in flight")
	}
	if kept.PrivateKeyEnv != record.PrivateKeyEnv {
		t.Fatalf("kept row = %+v, want the private-key env preserved", kept)
	}
	if !errors.Is(dedupedErr, ErrSettlementInFlight) {
		t.Fatalf("deduped retire() = %v, want ErrSettlementInFlight", dedupedErr)
	}

	close(release)
	settling.Wait()
	if _, stillPresent := testStore.snapshotDevshard(record.EscrowID); stillPresent {
		t.Fatal("row survived the settlement that actually completed, want it dropped")
	}
}

func parkedRecord(escrowID string) store.DevshardRecord {
	return store.DevshardRecord{EscrowID: escrowID, PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: false, SettlementPending: true}
}

func settlingTxClient() *fakeTxClient {
	return &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}
}

// A parked escrow is only reachable through the pending sweep, so without it the funds are stranded.
func TestSettlePendingSettlesParkedEscrowAndDropsRow(t *testing.T) {
	testStore := newFakeStore()
	record := parkedRecord("9")
	testStore.devshards[record.EscrowID] = record

	m := &Manager{
		tx:               settlingTxClient(),
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		config:           holderWithSettlementEnabled(true),
	}

	if err := m.settlePending(context.Background(), []store.DevshardRecord{record}); err != nil {
		t.Fatalf("settlePending() = %v, want nil", err)
	}
	if _, ok := testStore.devshards[record.EscrowID]; ok {
		t.Fatal("row still present after a confirmed settle, want it dropped")
	}
}

// Re-importing a parked escrow writes the record with SettlementPending at its zero value; the sweep
// keys on that marker, so an upsert that cleared it would strand the escrow silently.
func TestSettlePendingStillSettlesAfterAnUnrelatedUpsert(t *testing.T) {
	testStore := newFakeStore()
	record := parkedRecord("13")
	testStore.devshards[record.EscrowID] = record

	m := &Manager{
		tx:               settlingTxClient(),
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		config:           holderWithSettlementEnabled(true),
	}

	reimported := record
	reimported.SettlementPending = false
	if err := testStore.UpsertDevshard(context.Background(), reimported); err != nil {
		t.Fatalf("UpsertDevshard(): %v", err)
	}
	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}

	if err := m.settlePending(context.Background(), devshards); err != nil {
		t.Fatalf("settlePending() = %v, want nil", err)
	}
	if _, ok := testStore.snapshotDevshard(record.EscrowID); ok {
		t.Fatal("the escrow was not settled after an unrelated upsert; nothing else will ever pick it up")
	}
}

func TestSettlePendingBusyEscrowStaysParkedForTheNextTick(t *testing.T) {
	testStore := newFakeStore()
	record := parkedRecord("10")
	testStore.devshards[record.EscrowID] = record

	m := &Manager{
		tx:               settlingTxClient(),
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{busy: true},
		config:           holderWithSettlementEnabled(true),
	}

	if err := m.settlePending(context.Background(), []store.DevshardRecord{record}); !errors.Is(err, ErrDevshardBusy) {
		t.Fatalf("settlePending() = %v, want ErrDevshardBusy", err)
	}
	assertParked(t, testStore, record.EscrowID)
}

func TestSettlePendingIsNoOpWhileSettlementDisabled(t *testing.T) {
	testStore := newFakeStore()
	record := parkedRecord("11")
	testStore.devshards[record.EscrowID] = record
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			t.Fatal("SettleEscrow must not be called while the settlement toggle is off")
			return chain.SettleEscrowResult{}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		config:           holderWithSettlementEnabled(false),
	}

	if err := m.settlePending(context.Background(), []store.DevshardRecord{record}); err != nil {
		t.Fatalf("settlePending() = %v, want nil", err)
	}
	assertParked(t, testStore, record.EscrowID)
}

// The sweep is keyed on the pending marker, not merely on being inactive: a deactivated escrow that
// was never marked has no settlement intent and must not be settled.
func TestSettlePendingIgnoresInactiveEscrowWithoutPendingMarker(t *testing.T) {
	testStore := newFakeStore()
	record := store.DevshardRecord{EscrowID: "12", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: false, SettlementPending: false}
	testStore.devshards[record.EscrowID] = record
	txClient := &fakeTxClient{
		settleEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			t.Fatal("SettleEscrow called for an escrow that was never marked settlement-pending")
			return chain.SettleEscrowResult{}, nil
		},
	}

	m := &Manager{
		tx:               txClient,
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
		config:           holderWithSettlementEnabled(true),
	}

	if err := m.settlePending(context.Background(), []store.DevshardRecord{record}); err != nil {
		t.Fatalf("settlePending() = %v, want nil", err)
	}
	if _, ok := testStore.devshards[record.EscrowID]; !ok {
		t.Fatal("unmarked escrow was settled and dropped")
	}
}

// A settle whose wait times out may commit moments later. Building a fresh transaction on the next
// tick produces a new hash the keeper rejects as already-settled, and the tick burns a fee for it
// every fifteen seconds forever. Asking the chain about the recorded hash first is what stops that.
func TestASettleAlreadyOnChainIsReconciledInsteadOfRebroadcast(t *testing.T) {
	log := &callLog{}
	record := store.DevshardRecord{EscrowID: "7", Model: "model-a", SettlementPending: true, SettleTxHash: "SETTLE-TX"}
	testStore := newFakeStore()
	testStore.devshards[record.EscrowID] = record
	testStore.calls = log
	txClient := &fakeTxClient{
		calls:         log,
		txCommittedFn: func(string) (bool, error) { return true, nil },
	}
	m := &Manager{
		tx: txClient, store: testStore, signer: &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
	}

	result, err := m.settle(context.Background(), record)
	if err != nil {
		t.Fatalf("settle() = %v, want the recorded settlement recognised", err)
	}
	if result.TxHash != "SETTLE-TX" {
		t.Fatalf("TxHash = %q, want the transaction already on chain", result.TxHash)
	}
	for _, call := range log.snapshot() {
		if call == "SettleEscrow" {
			t.Fatal("a second settlement was broadcast for an escrow the chain had already settled")
		}
	}
}

// The mirror case: a recorded transaction that never landed must not block a genuine retry, and the
// stale hash has to go so the next attempt is judged on its own.
func TestASettleThatNeverLandedIsRetried(t *testing.T) {
	record := store.DevshardRecord{EscrowID: "7", Model: "model-a", SettlementPending: true, SettleTxHash: "GONE"}
	testStore := newFakeStore()
	testStore.devshards[record.EscrowID] = record
	txClient := &fakeTxClient{
		settleTxHash:  "FRESH-TX",
		txCommittedFn: func(string) (bool, error) { return false, chain.ErrTxNotFound },
		settleEscrowFn: func(_ context.Context, _ *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error) {
			return chain.SettleEscrowResult{EscrowID: input.EscrowID}, nil
		},
	}
	m := &Manager{
		tx: txClient, store: testStore, signer: &fakeSignerSource{signer: testSigner(t)},
		settlementSource: &fakeSettlementSource{},
	}

	if _, err := m.settle(context.Background(), record); err != nil {
		t.Fatalf("settle() = %v, want a fresh settlement", err)
	}

	if got := testStore.devshards["7"].SettleTxHash; got != "FRESH-TX" {
		t.Fatalf("recorded hash = %q, want the new transaction's", got)
	}
}
