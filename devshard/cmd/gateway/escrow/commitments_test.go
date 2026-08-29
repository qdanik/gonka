package escrow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
	"devshard/signing"
)

// fakeTxClient stubs escrowTxClient. createEscrowFn should mirror the real
// chain.TxClient.CreateEscrow contract: call onPrepared(txHash) and only
// produce a result if onPrepared returns nil.
type fakeTxClient struct {
	createEscrowFn  func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error)
	getTxEscrowIDFn func(ctx context.Context, txHash string) (uint64, bool, error)
	settleEscrowFn  func(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error)
	getEscrowFn     func(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error)
	txCommittedFn   func(txHash string) (bool, error)
	settleTxHash    string
	createCalls     int
	calls           *callLog
}

func (f *fakeTxClient) CreateEscrow(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
	f.createCalls++
	return f.createEscrowFn(ctx, signer, amount, modelID, onPrepared)
}

func (f *fakeTxClient) SettleEscrow(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput, onPrepared func(string) error) (chain.SettleEscrowResult, error) {
	if f.calls != nil {
		f.calls.record("SettleEscrow")
	}
	if onPrepared != nil {
		if err := onPrepared(f.settleTxHash); err != nil {
			return chain.SettleEscrowResult{}, err
		}
	}
	if f.settleEscrowFn != nil {
		return f.settleEscrowFn(ctx, signer, input)
	}
	return chain.SettleEscrowResult{}, errors.New("fakeTxClient: SettleEscrow not stubbed")
}

// TxCommitted answers for a settle already broadcast. The zero value reports "not on chain", which is
// what a fake with no recorded settle should say.
func (f *fakeTxClient) TxCommitted(_ context.Context, txHash string) (bool, error) {
	if f.calls != nil {
		f.calls.record("TxCommitted")
	}
	if f.txCommittedFn != nil {
		return f.txCommittedFn(txHash)
	}
	return false, chain.ErrTxNotFound
}

func (f *fakeTxClient) GetTxEscrowID(ctx context.Context, txHash string) (uint64, bool, error) {
	return f.getTxEscrowIDFn(ctx, txHash)
}

func (f *fakeTxClient) GetEscrow(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error) {
	if f.calls != nil {
		f.calls.record("GetEscrow")
	}
	if f.getEscrowFn != nil {
		return f.getEscrowFn(ctx, escrowID)
	}
	return chain.EscrowInfo{}, false, errors.New("fakeTxClient: GetEscrow not stubbed")
}

// fakeStore stubs escrowStore in memory. WithRetry calls fn once; retry timing is store/retry_test.go's.
// upsertDevshardErrByID overrides the blanket upsertDevshardErr, and savedCommitments keeps deleted ones.
type fakeStore struct {
	mu sync.Mutex

	saveCommitmentErr     error
	upsertDevshardErr     error
	upsertDevshardErrByID map[string]error
	rotationRoleErrByID   map[string]error
	deleteCommitmentErr   error
	setActiveErr          error
	setPendingErr         error
	deleteDevshardErr     error
	saveRotationStatusErr error
	listDevshardsErr      error

	commitments      map[string]store.Commitment
	devshards        map[string]store.DevshardRecord
	settleTxAt       map[string]time.Time
	rotationStatuses map[string]store.RotationStatus
	savedCommitments []store.Commitment
	calls            *callLog
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		commitments:      make(map[string]store.Commitment),
		devshards:        make(map[string]store.DevshardRecord),
		settleTxAt:       make(map[string]time.Time),
		rotationStatuses: make(map[string]store.RotationStatus),
	}
}

func (f *fakeStore) WithRetry(ctx context.Context, fn func() error) error { return fn() }

func (f *fakeStore) SaveCommitment(ctx context.Context, c store.Commitment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveCommitmentErr != nil {
		return f.saveCommitmentErr
	}
	f.commitments[c.TxHash] = c
	f.savedCommitments = append(f.savedCommitments, c)
	return nil
}

func (f *fakeStore) LoadCommitments(ctx context.Context) ([]store.Commitment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record("LoadCommitments")
	}
	commitments := make([]store.Commitment, 0, len(f.commitments))
	for _, c := range f.commitments {
		commitments = append(commitments, c)
	}
	return commitments, nil
}

func (f *fakeStore) DeleteCommitment(ctx context.Context, txHash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteCommitmentErr != nil {
		return f.deleteCommitmentErr
	}
	delete(f.commitments, txHash)
	return nil
}

// UpsertDevshard mirrors the store's contract: every field of an existing row is replaced except
// SettlementPending, which only SetDevshardSettlementPending moves.
func (f *fakeStore) UpsertDevshard(ctx context.Context, record store.DevshardRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.upsertDevshardErrByID[record.EscrowID]; ok {
		return err
	}
	if f.upsertDevshardErr != nil {
		return f.upsertDevshardErr
	}
	if existing, ok := f.devshards[record.EscrowID]; ok {
		record.SettlementPending = existing.SettlementPending
	}
	f.devshards[record.EscrowID] = record
	return nil
}

func (f *fakeStore) snapshotDevshard(escrowID string) (store.DevshardRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.devshards[escrowID]
	return record, ok
}

func (f *fakeStore) ListDevshards(ctx context.Context) ([]store.DevshardRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record("ListDevshards")
	}
	if f.listDevshardsErr != nil {
		return nil, f.listDevshardsErr
	}
	records := make([]store.DevshardRecord, 0, len(f.devshards))
	for _, record := range f.devshards {
		records = append(records, record)
	}
	return records, nil
}

func (f *fakeStore) SetDevshardActive(ctx context.Context, escrowID string, active bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record(fmt.Sprintf("SetDevshardActive(%v)", active))
	}
	if f.setActiveErr != nil {
		return f.setActiveErr
	}
	record, ok := f.devshards[escrowID]
	if !ok {
		return store.ErrDevshardNotFound
	}
	record.Active = active
	f.devshards[escrowID] = record
	return nil
}

func (f *fakeStore) SetDevshardSettlementPending(ctx context.Context, escrowID string, pending bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record(fmt.Sprintf("SetDevshardSettlementPending(%v)", pending))
	}
	if f.setPendingErr != nil {
		return f.setPendingErr
	}
	record, ok := f.devshards[escrowID]
	if !ok {
		return store.ErrDevshardNotFound
	}
	record.SettlementPending = pending
	f.devshards[escrowID] = record
	return nil
}

// ParkForSettlement writes both fields at once, the way the store does: a fake that wrote them
// separately would let a test pass against the shape the real one cannot produce.
func (f *fakeStore) ParkForSettlement(_ context.Context, escrowID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record("ParkForSettlement")
	}
	if f.setActiveErr != nil {
		return f.setActiveErr
	}
	if f.setPendingErr != nil {
		return f.setPendingErr
	}
	record, held := f.devshards[escrowID]
	if !held {
		return store.ErrDevshardNotFound
	}
	record.Active = false
	record.SettlementPending = true
	f.devshards[escrowID] = record
	return nil
}

func (f *fakeStore) DevshardSettleTxHash(_ context.Context, escrowID string) (string, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.devshards[escrowID].SettleTxHash, f.settleTxAt[escrowID], nil
}

func (f *fakeStore) SetDevshardRotationRole(_ context.Context, escrowID, role string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record(fmt.Sprintf("SetDevshardRotationRole(%q)", role))
	}
	if err := f.rotationRoleErrByID[escrowID]; err != nil {
		return err
	}
	record, held := f.devshards[escrowID]
	if !held {
		return store.ErrDevshardNotFound
	}
	record.RotationRole = role
	f.devshards[escrowID] = record
	return nil
}

func (f *fakeStore) SetDevshardSettleTxHash(_ context.Context, escrowID, txHash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record(fmt.Sprintf("SetDevshardSettleTxHash(%q)", txHash))
	}
	record, held := f.devshards[escrowID]
	if !held {
		return store.ErrDevshardNotFound
	}
	record.SettleTxHash = txHash
	f.devshards[escrowID] = record
	if txHash == "" {
		delete(f.settleTxAt, escrowID)
	} else {
		f.settleTxAt[escrowID] = time.Now().UTC()
	}
	return nil
}

func (f *fakeStore) DeleteDevshard(ctx context.Context, escrowID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != nil {
		f.calls.record("DeleteDevshard")
	}
	if f.deleteDevshardErr != nil {
		return f.deleteDevshardErr
	}
	if _, ok := f.devshards[escrowID]; !ok {
		return store.ErrDevshardNotFound
	}
	delete(f.devshards, escrowID)
	return nil
}

func (f *fakeStore) SaveRotationStatus(ctx context.Context, s store.RotationStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveRotationStatusErr != nil {
		return f.saveRotationStatusErr
	}
	f.rotationStatuses[s.Model+"|"+s.Role] = s
	return nil
}

type fakeSignerSource struct {
	signer       *signing.Secp256k1Signer
	err          error
	requestedEnv string
}

func (f *fakeSignerSource) SignerFor(privateKeyEnv string) (*signing.Secp256k1Signer, error) {
	f.requestedEnv = privateKeyEnv
	if f.err != nil {
		return nil, f.err
	}
	return f.signer, nil
}

func testSigner(t *testing.T) *signing.Secp256k1Signer {
	t.Helper()
	signer, err := signing.GenerateKey()
	if err != nil {
		t.Fatalf("signing.GenerateKey(): %v", err)
	}
	return signer
}

// INVARIANT 1: a failed intent write must abort before any chain broadcast,
// leaving zero commitments and zero devshards -- never a broadcast with a lost intent.
func TestCreateEscrowAbortsWhenIntentWriteFails(t *testing.T) {
	testStore := newFakeStore()
	testStore.saveCommitmentErr = errors.New("store unavailable")
	testStore.upsertDevshardErr = errors.New("store unavailable")

	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			if err := onPrepared("TX-DEAD"); err != nil {
				return chain.CreateEscrowResult{}, err
			}
			t.Fatal("CreateEscrow must not broadcast when onPrepared fails")
			return chain.CreateEscrowResult{}, nil
		},
	}
	m := &Manager{
		tx:      txClient,
		store:   testStore,
		signer:  &fakeSignerSource{signer: testSigner(t)},
		breaker: newCreateBreaker(),
		now:     func() time.Time { return time.Unix(0, 0) },
	}
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}

	if _, err := m.createEscrow(context.Background(), model, "temp", 5, 100); err == nil {
		t.Fatal("createEscrow() = nil, want error when the intent write fails")
	}

	devshards, _ := testStore.ListDevshards(context.Background())
	if len(devshards) != 0 {
		t.Fatalf("ListDevshards() = %+v, want zero (broadcast must never happen without a durable intent)", devshards)
	}
	commitments, _ := testStore.LoadCommitments(context.Background())
	if len(commitments) != 0 {
		t.Fatalf("LoadCommitments() = %+v, want zero (the failed intent write itself must not persist)", commitments)
	}
}

// A signer that cannot be resolved must abort before the chain client is even called.
func TestCreateEscrowSignerResolutionFailureNeverCallsChain(t *testing.T) {
	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			t.Fatal("CreateEscrow must not be called when signer resolution fails")
			return chain.CreateEscrowResult{}, nil
		},
	}
	m := &Manager{
		tx:      txClient,
		store:   newFakeStore(),
		signer:  &fakeSignerSource{err: errors.New("key not configured")},
		breaker: newCreateBreaker(),
		now:     time.Now,
	}
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MISSING_KEY"}

	if _, err := m.createEscrow(context.Background(), model, "temp", 5, 100); err == nil {
		t.Fatal("createEscrow() = nil, want error when signer resolution fails")
	}
	if txClient.createCalls != 0 {
		t.Fatalf("CreateEscrow called %d times, want 0", txClient.createCalls)
	}
}

// Happy path: the intent commitment is written (with every field) before the
// chain call returns, and the devshard is persisted once it does.
func TestCreateEscrowHappyPathPersistsDevshardAndClearsCommitment(t *testing.T) {
	testStore := newFakeStore()
	fixedNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			if amount != 1000 || modelID != "model-a" {
				t.Fatalf("CreateEscrow(amount=%d, modelID=%q), want (1000, model-a)", amount, modelID)
			}
			if err := onPrepared("TX-HAPPY"); err != nil {
				return chain.CreateEscrowResult{}, err
			}
			return chain.CreateEscrowResult{EscrowID: 42, TxHash: "TX-HAPPY"}, nil
		},
	}
	signerSource := &fakeSignerSource{signer: testSigner(t)}
	m := &Manager{
		tx:      txClient,
		store:   testStore,
		signer:  signerSource,
		breaker: newCreateBreaker(),
		now:     func() time.Time { return fixedNow },
	}
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}

	if _, err := m.createEscrow(context.Background(), model, "temp", 7, 500); err != nil {
		t.Fatalf("createEscrow(): %v", err)
	}

	if signerSource.requestedEnv != "MODEL_A_KEY" {
		t.Fatalf("SignerFor called with %q, want MODEL_A_KEY", signerSource.requestedEnv)
	}
	if len(testStore.savedCommitments) != 1 {
		t.Fatalf("savedCommitments = %+v, want exactly 1 recorded intent", testStore.savedCommitments)
	}
	savedIntent := testStore.savedCommitments[0]
	if savedIntent.TxHash != "TX-HAPPY" || savedIntent.Model != "model-a" || savedIntent.Role != "temp" ||
		savedIntent.Epoch != 7 || savedIntent.PrivateKeyEnv != "MODEL_A_KEY" ||
		savedIntent.BlockHeight != 500 || !savedIntent.CreatedAt.Equal(fixedNow) {
		t.Fatalf("intent commitment before broadcast = %+v, want TxHash=TX-HAPPY Model=model-a Role=temp Epoch=7 "+
			"PrivateKeyEnv=MODEL_A_KEY BlockHeight=500 CreatedAt=%v", savedIntent, fixedNow)
	}

	commitments, err := testStore.LoadCommitments(context.Background())
	if err != nil {
		t.Fatalf("LoadCommitments(): %v", err)
	}
	if len(commitments) != 0 {
		t.Fatalf("LoadCommitments() = %+v, want empty (cleared after persist)", commitments)
	}

	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}
	want := store.DevshardRecord{EscrowID: "42", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true, RotationRole: "temp", RotationEpoch: 7}
	if len(devshards) != 1 || devshards[0] != want {
		t.Fatalf("ListDevshards() = %+v, want [%+v]", devshards, want)
	}
}

// INVARIANT 1/6: when the post-broadcast registry write fails, the commitment
// survives so a later reconcile can recover the escrow by tx hash.
func TestCreateEscrowPersistFailureRecoversViaReconcile(t *testing.T) {
	testStore := newFakeStore()
	testStore.upsertDevshardErr = errors.New("registry unavailable")
	fixedNow := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)

	txClient := &fakeTxClient{
		createEscrowFn: func(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(string) error) (chain.CreateEscrowResult, error) {
			if err := onPrepared("TX-CRASH"); err != nil {
				return chain.CreateEscrowResult{}, err
			}
			return chain.CreateEscrowResult{EscrowID: 888, TxHash: "TX-CRASH"}, nil
		},
	}
	m := &Manager{
		tx:      txClient,
		store:   testStore,
		signer:  &fakeSignerSource{signer: testSigner(t)},
		breaker: newCreateBreaker(),
		now:     func() time.Time { return fixedNow },
	}
	model := ModelConfig{ModelID: "model-a", Amount: 1000, PrivateKeyEnv: "MODEL_A_KEY"}

	if _, err := m.createEscrow(context.Background(), model, "temp", 11, 200); err == nil {
		t.Fatal("createEscrow() = nil, want error when the post-create registry write fails")
	}
	if devshards, _ := testStore.ListDevshards(context.Background()); len(devshards) != 0 {
		t.Fatalf("ListDevshards() = %+v, want empty (registration never completed)", devshards)
	}
	commitments, err := testStore.LoadCommitments(context.Background())
	if err != nil {
		t.Fatalf("LoadCommitments(): %v", err)
	}
	if len(commitments) != 1 || commitments[0].TxHash != "TX-CRASH" {
		t.Fatalf("LoadCommitments() = %+v, want exactly the TX-CRASH commitment kept for recovery", commitments)
	}

	// The store recovers, and reconcile resolves the same tx hash to its escrow id.
	testStore.upsertDevshardErr = nil
	txClient.getTxEscrowIDFn = func(ctx context.Context, txHash string) (uint64, bool, error) {
		if txHash != "TX-CRASH" {
			t.Fatalf("GetTxEscrowID(%q), want TX-CRASH", txHash)
		}
		return 888, true, nil
	}

	if err := m.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile(): %v", err)
	}

	commitments, _ = testStore.LoadCommitments(context.Background())
	if len(commitments) != 0 {
		t.Fatalf("LoadCommitments() after reconcile = %+v, want empty", commitments)
	}
	devshards, _ := testStore.ListDevshards(context.Background())
	want := store.DevshardRecord{EscrowID: "888", PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", Active: true, RotationRole: "temp", RotationEpoch: 11}
	if len(devshards) != 1 || devshards[0] != want {
		t.Fatalf("ListDevshards() after reconcile = %+v, want [%+v]", devshards, want)
	}
}

// reconcile's decision table (INVARIANTS 5/6): every GetTxEscrowID outcome
// must map to exactly the documented clear/keep/recover action.
func TestReconcileDecisionTable(t *testing.T) {
	fixedNow := time.Date(2026, 3, 3, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name               string
		commitment         store.Commitment
		getTxEscrowIDFn    func(ctx context.Context, txHash string) (uint64, bool, error)
		wantErr            bool
		wantErrContains    string
		wantCommitmentKept bool
		wantDevshard       *store.DevshardRecord
	}{
		{
			name:            "found resolves escrow id: persists, clears commitment, resets breaker",
			commitment:      store.Commitment{TxHash: "TXA", Model: "model-a", Role: "temp", Epoch: 3, PrivateKeyEnv: "KEY_A", BlockHeight: 10, CreatedAt: fixedNow},
			getTxEscrowIDFn: func(ctx context.Context, txHash string) (uint64, bool, error) { return 55, true, nil },
			wantDevshard:    &store.DevshardRecord{EscrowID: "55", PrivateKeyEnv: "KEY_A", Model: "model-a", Active: true, RotationRole: "temp", RotationEpoch: 3},
		},
		{
			name:            "committed but failed (no escrow event): clears, terminal",
			commitment:      store.Commitment{TxHash: "TXB", Model: "model-a", Role: "temp", CreatedAt: fixedNow},
			getTxEscrowIDFn: func(ctx context.Context, txHash string) (uint64, bool, error) { return 0, false, nil },
		},
		{
			name:               "not found within grace: kept for the next tick",
			commitment:         store.Commitment{TxHash: "TXC", Model: "model-a", Role: "temp", CreatedAt: fixedNow},
			getTxEscrowIDFn:    func(ctx context.Context, txHash string) (uint64, bool, error) { return 0, false, chain.ErrTxNotFound },
			wantCommitmentKept: true,
		},
		{
			name:            "not found past grace: can never land, clears",
			commitment:      store.Commitment{TxHash: "TXD", Model: "model-a", Role: "temp", CreatedAt: fixedNow.Add(-commitmentReconcileGrace - time.Second)},
			getTxEscrowIDFn: func(ctx context.Context, txHash string) (uint64, bool, error) { return 0, false, chain.ErrTxNotFound },
		},
		{
			name:       "endpoint unreachable: kept for the next tick, error surfaced",
			commitment: store.Commitment{TxHash: "TXE", Model: "model-a", Role: "temp", CreatedAt: fixedNow},
			getTxEscrowIDFn: func(ctx context.Context, txHash string) (uint64, bool, error) {
				return 0, false, errors.New("chain unreachable")
			},
			wantErr:            true,
			wantErrContains:    "chain unreachable",
			wantCommitmentKept: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testStore := newFakeStore()
			if err := testStore.SaveCommitment(context.Background(), tt.commitment); err != nil {
				t.Fatalf("seeding commitment: %v", err)
			}
			breaker := newCreateBreaker()
			if tt.wantDevshard != nil {
				// pre-record a failure so a successful persist's breaker reset is observable.
				breaker.recordFailure(tt.commitment.Model, tt.commitment.Role)
			}
			m := &Manager{
				tx:      &fakeTxClient{getTxEscrowIDFn: tt.getTxEscrowIDFn},
				store:   testStore,
				breaker: breaker,
				now:     func() time.Time { return fixedNow },
			}

			err := m.reconcile(context.Background())
			if tt.wantErr && err == nil {
				t.Fatal("reconcile() = nil, want a surfaced error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("reconcile() = %v, want nil", err)
			}
			if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("reconcile() error = %q, want it to mention %q", err.Error(), tt.wantErrContains)
			}

			commitments, loadErr := testStore.LoadCommitments(context.Background())
			if loadErr != nil {
				t.Fatalf("LoadCommitments(): %v", loadErr)
			}
			if tt.wantCommitmentKept {
				if len(commitments) != 1 || commitments[0].TxHash != tt.commitment.TxHash {
					t.Fatalf("LoadCommitments() = %+v, want the commitment kept for retry", commitments)
				}
			} else if len(commitments) != 0 {
				t.Fatalf("LoadCommitments() = %+v, want cleared", commitments)
			}

			if tt.wantDevshard != nil {
				devshards, err := testStore.ListDevshards(context.Background())
				if err != nil {
					t.Fatalf("ListDevshards(): %v", err)
				}
				if len(devshards) != 1 || devshards[0] != *tt.wantDevshard {
					t.Fatalf("ListDevshards() = %+v, want [%+v]", devshards, *tt.wantDevshard)
				}
				if breaker.gated(tt.commitment.Model, tt.commitment.Role) {
					t.Fatal("breaker still gated after a successful persist, want reset")
				}
			}
		})
	}
}

// txMayStillLand gates the reconcile clear-vs-keep decision on the unordered-tx
// TTL grace window, with a zero CreatedAt defensively treated as still-pending.
func TestTxMayStillLand(t *testing.T) {
	fixedNow := time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC)
	m := &Manager{now: func() time.Time { return fixedNow }}

	tests := []struct {
		name      string
		createdAt time.Time
		want      bool
	}{
		{name: "zero CreatedAt is defensively still-pending", createdAt: time.Time{}, want: true},
		{name: "freshly created may still land", createdAt: fixedNow, want: true},
		{name: "exactly at the grace boundary may still land", createdAt: fixedNow.Add(-commitmentReconcileGrace), want: true},
		{name: "just past the grace boundary can no longer land", createdAt: fixedNow.Add(-commitmentReconcileGrace - time.Second), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := m.txMayStillLand(store.Commitment{CreatedAt: tt.createdAt}); got != tt.want {
				t.Fatalf("txMayStillLand(CreatedAt=%v) = %v, want %v", tt.createdAt, got, tt.want)
			}
		})
	}
}

// Recorded at creation, not re-derived on the next boot, which is a different version after a redeploy.
func TestPersistEscrowStampsTheVersionItWasMintedUnder(t *testing.T) {
	testStore := newFakeStore()
	m := &Manager{store: testStore, breaker: newCreateBreaker(), now: time.Now, routePrefix: "/devshard/v3"}

	if err := m.persistEscrow(context.Background(), "77",
		store.Commitment{TxHash: "TXV", Model: "model-a", Role: "temp", Epoch: 1, PrivateKeyEnv: "KEY_A"}); err != nil {
		t.Fatalf("persistEscrow(): %v", err)
	}

	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}
	if len(devshards) != 1 {
		t.Fatalf("ListDevshards() = %+v, want exactly 1 record", devshards)
	}
	if devshards[0].RoutePrefix != "/devshard/v3" {
		t.Fatalf("RoutePrefix = %q, want %q: an unpinned escrow follows the gateway onto a version its hosts refuse",
			devshards[0].RoutePrefix, "/devshard/v3")
	}
}

// INVARIANT 4: registering the same escrow id twice is a success no-op, not an error.
func TestPersistEscrowSameEscrowIDTwiceIsSuccessNoOp(t *testing.T) {
	testStore := newFakeStore()
	m := &Manager{store: testStore, breaker: newCreateBreaker(), now: time.Now}
	c := store.Commitment{TxHash: "TXG", Model: "model-a", Role: "temp", Epoch: 1, PrivateKeyEnv: "KEY_A"}

	if err := m.persistEscrow(context.Background(), "77", c); err != nil {
		t.Fatalf("persistEscrow() first call: %v", err)
	}
	if err := m.persistEscrow(context.Background(), "77", c); err != nil {
		t.Fatalf("persistEscrow() second call for the same escrow id = %v, want nil (idempotent upsert)", err)
	}

	devshards, err := testStore.ListDevshards(context.Background())
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}
	if len(devshards) != 1 {
		t.Fatalf("ListDevshards() = %+v, want exactly 1 record (duplicate registration must not create a second row)", devshards)
	}
}

// A commitment can outlive its own create: the gateway dies between registering the escrow and
// dropping the commitment. By the time reconcile finds it, that escrow may already be parked with a
// settle transaction recorded — and re-registering it would flip it active and wipe the hash the
// settlement reconciliation depends on.
func TestReconcileLeavesAnAlreadyRegisteredEscrowAlone(t *testing.T) {
	testStore := newFakeStore()
	parked := store.DevshardRecord{
		EscrowID: "42", Model: "model-a", Active: false,
		SettlementPending: true, SettleTxHash: "SETTLE-TX",
	}
	testStore.devshards["42"] = parked
	testStore.commitments["CREATE-TX"] = store.Commitment{TxHash: "CREATE-TX", Model: "model-a", Role: "regular"}
	txClient := &fakeTxClient{
		getTxEscrowIDFn: func(context.Context, string) (uint64, bool, error) { return 42, true, nil },
	}
	m := &Manager{tx: txClient, store: testStore, breaker: newCreateBreaker(), now: time.Now}

	if err := m.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile() = %v, want nil", err)
	}

	got := testStore.devshards["42"]
	if got.Active {
		t.Fatal("a parked escrow was resurrected as active")
	}
	if got.SettleTxHash != "SETTLE-TX" {
		t.Fatalf("settle hash = %q, want it untouched: its reconciliation depends on it", got.SettleTxHash)
	}
	if _, held := testStore.commitments["CREATE-TX"]; held {
		t.Fatal("the commitment was left behind, so this repeats every tick")
	}
}
