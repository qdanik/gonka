package escrow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"

	"go.uber.org/goleak"
)

type fakeSnapshotSource struct {
	snapshot chain.PhaseSnapshot
}

func (f *fakeSnapshotSource) Snapshot() chain.PhaseSnapshot { return f.snapshot }

// blockingSnapshotSource blocks Snapshot() until release is closed, signaling started first;
// it lets a test deterministically catch a tick mid-flight.
type blockingSnapshotSource struct {
	started  chan struct{}
	release  chan struct{}
	snapshot chain.PhaseSnapshot
}

func (f *blockingSnapshotSource) Snapshot() chain.PhaseSnapshot {
	close(f.started)
	<-f.release
	return f.snapshot
}

func hasCall(calls []string, name string) bool {
	for _, c := range calls {
		if c == name {
			return true
		}
	}
	return false
}

func testManagerDeps(t *testing.T, testStore *fakeStore, txClient *fakeTxClient, snapshots snapshotSource, cfg *config.Config) Deps {
	t.Helper()
	return Deps{
		Tx:         txClient,
		Store:      testStore,
		Snapshots:  snapshots,
		Settlement: &fakeSettlementSource{},
		Signer:     &fakeSignerSource{signer: testSigner(t)},
		Config:     config.NewHolder(cfg),
		Now:        time.Now,
	}
}

func TestTickReconcileRunsEvenWhenRotationDisabled(t *testing.T) {
	testStore := newFakeStore()
	log := &callLog{}
	testStore.calls = log
	cfg := config.Defaults()
	cfg.Rotation.Enabled = false
	m := NewManager(testManagerDeps(t, testStore, &fakeTxClient{}, &fakeSnapshotSource{}, &cfg))

	if err := m.tick(context.Background()); err != nil {
		t.Fatalf("tick(): %v", err)
	}

	calls := log.snapshot()
	if !hasCall(calls, "LoadCommitments") {
		t.Fatalf("calls = %v, want LoadCommitments (reconcile must run even when rotation is disabled)", calls)
	}
	if hasCall(calls, "SaveRotationStatus") {
		t.Fatalf("calls = %v, want no rotation work while rotation is disabled", calls)
	}
}

func TestTickColdStartReturnsAfterReconcile(t *testing.T) {
	tests := []struct {
		name     string
		snapshot chain.PhaseSnapshot
	}{
		{name: "zero epoch index", snapshot: chain.PhaseSnapshot{EpochIndex: 0, BlockHeight: 500}},
		{name: "zero block height", snapshot: chain.PhaseSnapshot{EpochIndex: 5, BlockHeight: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testStore := newFakeStore()
			log := &callLog{}
			testStore.calls = log
			cfg := config.Defaults()
			cfg.Rotation.Enabled = true
			m := NewManager(testManagerDeps(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, &fakeSnapshotSource{snapshot: tt.snapshot}, &cfg))

			if err := m.tick(context.Background()); err != nil {
				t.Fatalf("tick(): %v", err)
			}
			if hasCall(log.snapshot(), "SaveRotationStatus") {
				t.Fatal("rotation ran on cold start, want it skipped until the chain snapshot is populated")
			}
		})
	}
}

func TestTickInvalidModelsJSONSurfacesErrorAlongsideReconcile(t *testing.T) {
	testStore := newFakeStore()
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	cfg.Rotation.ModelsJSON = "not valid json"
	m := NewManager(testManagerDeps(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, &fakeSnapshotSource{}, &cfg))

	if err := m.tick(context.Background()); err == nil {
		t.Fatal("tick() = nil, want the bad rotation config surfaced")
	}
}

func TestTickListDevshardsFailureSurfacesErrorAlongsideReconcile(t *testing.T) {
	testStore := newFakeStore()
	testStore.listDevshardsErr = errors.New("store unavailable")
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	snapshot := chain.PhaseSnapshot{EpochIndex: 5, BlockHeight: 100}
	m := NewManager(testManagerDeps(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, &fakeSnapshotSource{snapshot: snapshot}, &cfg))

	if err := m.tick(context.Background()); err == nil {
		t.Fatal("tick() = nil, want the ListDevshards failure surfaced")
	}
}

func TestTickPrePoCWindowRunsPrepareBridgeEvenWhenPoCInactive(t *testing.T) {
	testStore := newFakeStore()
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(700)}
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	cfg.Rotation.ModelsJSON = `[{"model_id":"model-a","temp_count":1,"target_count":1,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
	snapshot := chain.PhaseSnapshot{
		EpochIndex: 9, BlockHeight: 500, EpochSwitchBlockHeight: 500 + cfg.Rotation.PrePoCBlocks/2,
		RequestsBlocked:    false, // pre-PoC must win even though PoC is not active
		FullWeightsByModel: map[string]map[string]float64{"model-a": {"p": 1}},
	}
	m := NewManager(testManagerDeps(t, testStore, txClient, &fakeSnapshotSource{snapshot: snapshot}, &cfg))

	if err := m.tick(context.Background()); err != nil {
		t.Fatalf("tick(): %v", err)
	}

	tempCreated, regularCreated := false, false
	for _, record := range testStore.devshards {
		if record.Model != "model-a" {
			continue
		}
		switch record.RotationRole {
		case roleTemp:
			tempCreated = true
		case roleRegular:
			regularCreated = true
		}
	}
	if !tempCreated {
		t.Fatal("no temp escrow created, want prepareBridge to run inside the pre-PoC window")
	}
	if regularCreated {
		t.Fatal("a regular escrow was created, want only prepareBridge (temp) to run in the pre-PoC window")
	}
}

func TestTickPoCOverRunsFinishBridge(t *testing.T) {
	testStore := newFakeStore()
	temp := store.DevshardRecord{EscrowID: "temp-1", Model: "model-a", Active: true, RotationRole: roleTemp, RotationEpoch: 9, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[temp.EscrowID] = temp
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(800)}
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	cfg.Rotation.ModelsJSON = `[{"model_id":"model-a","target_count":1,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
	snapshot := chain.PhaseSnapshot{
		EpochIndex: 9, BlockHeight: 800, EpochSwitchBlockHeight: 100, // outside the pre-PoC window: 100-800 < 0
		RequestsBlocked:    false, // PoC over
		FullWeightsByModel: map[string]map[string]float64{"model-a": {"p": 1}},
	}
	m := NewManager(testManagerDeps(t, testStore, txClient, &fakeSnapshotSource{snapshot: snapshot}, &cfg))

	if err := m.tick(context.Background()); err != nil {
		t.Fatalf("tick(): %v", err)
	}

	assertParked(t, testStore, "temp-1")
	regularCreated := false
	for _, record := range testStore.devshards {
		if record.Model == "model-a" && record.RotationRole == roleRegular {
			regularCreated = true
		}
	}
	if !regularCreated {
		t.Fatal("no regular escrow created, want finishBridge to run")
	}
}

// Neither branch fires here (PoC active, outside the pre-PoC window), which also proves
// checkDepletion is independent of the bridge outcome.
func TestTickCheckDepletionRunsRegardlessOfBridgeBranch(t *testing.T) {
	testStore := newFakeStore()
	depleted := store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true, RotationRole: roleRegular, PrivateKeyEnv: "MODEL_A_KEY"}
	testStore.devshards[depleted.EscrowID] = depleted
	txClient := &fakeTxClient{createEscrowFn: succeedingCreateEscrowFn(999)}
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	cfg.Rotation.ModelsJSON = `[{"model_id":"model-a","target_count":1,"amount":1000,"private_key_env":"MODEL_A_KEY"}]`
	snapshot := chain.PhaseSnapshot{
		EpochIndex: 9, BlockHeight: 800, EpochSwitchBlockHeight: 100,
		RequestsBlocked:    true, // PoC active and outside the window: neither bridge branch runs
		FullWeightsByModel: map[string]map[string]float64{"model-a": {"p": 1}},
	}
	m := NewManager(testManagerDeps(t, testStore, txClient, &fakeSnapshotSource{snapshot: snapshot}, &cfg))
	m.OnBalanceExhausted("1")

	if err := m.tick(context.Background()); err != nil {
		t.Fatalf("tick(): %v", err)
	}

	assertParked(t, testStore, "1")
}

// If Start were not idempotent, the first goroutine's stop/done channels would be overwritten
// and orphaned by the second call, leaking a goroutine that goleak.VerifyNone would catch.
func TestStartStopIdempotent(t *testing.T) {
	defer goleak.VerifyNone(t)

	cfg := config.Defaults()
	m := NewManager(testManagerDeps(t, newFakeStore(), &fakeTxClient{}, &fakeSnapshotSource{}, &cfg))

	ctx := context.Background()
	m.Start(ctx)
	m.Start(ctx) // no-op: must not spawn a second goroutine
	m.Stop()
	m.Stop() // no-op: must not block or panic on an already-stopped Manager
}

func TestStopBlocksUntilInFlightTickExits(t *testing.T) {
	defer goleak.VerifyNone(t)

	started := make(chan struct{})
	release := make(chan struct{})
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true // reach the snapshot fetch, where this test blocks the tick
	m := NewManager(testManagerDeps(t, newFakeStore(), &fakeTxClient{}, &blockingSnapshotSource{started: started, release: release}, &cfg))

	m.Start(context.Background())
	<-started // the immediate first tick is now blocked inside Snapshot()

	stopReturned := make(chan struct{})
	go func() {
		m.Stop()
		close(stopReturned)
	}()

	select {
	case <-stopReturned:
		t.Fatal("Stop() returned before the in-flight tick finished, want it to block")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return after the in-flight tick finished")
	}
}

// Mirrors chain.PhaseObserver's own lifecycle test: canceling the ctx passed to Start, without
// ever calling Stop, must still terminate the tick goroutine (accessing m.done directly, same
// package as chain/observer_test.go does with doneCh).
func TestContextCancelAloneStopsTickGoroutine(t *testing.T) {
	defer goleak.VerifyNone(t)

	cfg := config.Defaults()
	m := NewManager(testManagerDeps(t, newFakeStore(), &fakeTxClient{}, &fakeSnapshotSource{}, &cfg))

	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	done := m.done
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("tick goroutine did not exit within 2s of context cancel")
	}
}

func TestStartStopConcurrentCallsAreRaceFree(t *testing.T) {
	defer goleak.VerifyNone(t)

	cfg := config.Defaults()
	m := NewManager(testManagerDeps(t, newFakeStore(), &fakeTxClient{}, &fakeSnapshotSource{}, &cfg))

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(2)
		go func() { defer wg.Done(); m.Start(context.Background()) }()
		go func() { defer wg.Done(); m.Stop() }()
	}
	wg.Wait()
	m.Stop() // whichever Start last won the race must still be cleanly stoppable
}

// The sweep must not depend on the rotation toggle: a parked escrow still has to settle.
func TestTickSettlesParkedEscrowWhileRotationDisabled(t *testing.T) {
	testStore := newFakeStore()
	record := parkedRecord("42")
	testStore.devshards[record.EscrowID] = record
	cfg := config.Defaults()
	cfg.Rotation.Enabled = false
	cfg.Rotation.SettlementEnabled = true

	m := NewManager(testManagerDeps(t, testStore, settlingTxClient(), &fakeSnapshotSource{}, &cfg))

	if err := m.tick(context.Background()); err != nil {
		t.Fatalf("tick(): %v", err)
	}
	if _, ok := testStore.devshards[record.EscrowID]; ok {
		t.Fatal("parked escrow survived the tick; nothing else will ever settle it")
	}
}

// Stop must not return while a tick is still writing: the caller closes the store right after, so an
// early return means a settlement write can land on a closed database. A second concurrent Stop has
// to wait with the first rather than racing past it.
func TestManagerStopIsABarrierForConcurrentCallers(t *testing.T) {
	defer goleak.VerifyNone(t)
	testStore := newFakeStore()
	blocking := &blockingSnapshotSource{started: make(chan struct{}), release: make(chan struct{})}
	cfg := config.Defaults()
	cfg.Rotation.Enabled = true
	m := NewManager(testManagerDeps(t, testStore, &fakeTxClient{createEscrowFn: failOnCreate(t)}, blocking, &cfg))

	m.Start(context.Background())
	<-blocking.started // a tick is inside Snapshot and cannot finish until released

	returned := make(chan struct{}, 2)
	var stoppers sync.WaitGroup
	for range 2 {
		stoppers.Add(1)
		go func() {
			defer stoppers.Done()
			m.Stop()
			returned <- struct{}{}
		}()
	}

	select {
	case <-returned:
		t.Fatal("Stop returned while a tick was still running")
	case <-time.After(100 * time.Millisecond):
	}

	close(blocking.release)
	stoppers.Wait()
	if len(returned) != 2 {
		t.Fatalf("%d of 2 Stop calls returned", len(returned))
	}
}
