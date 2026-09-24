package registry

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

func settleableSession(nonce uint64) *fakeSession {
	session := newFakeSession("hostA", "hostB")
	session.nonce.Store(nonce)
	session.escrowState = types.EscrowState{
		EscrowID:                    "5",
		StateRootAndProtocolVersion: "v2",
		Balance:                     900,
		Fees:                        11,
		HostStats: map[uint32]*types.HostStats{
			2: {Missed: 1, Invalid: 0, Cost: 30, RequiredValidations: 3, CompletedValidations: 2},
			0: {Missed: 0, Invalid: 4, Cost: 10},
			1: nil,
		},
	}
	session.signatures = map[uint64]map[uint32][]byte{
		nonce: {2: []byte("sig-2"), 0: []byte("sig-0")},
	}
	return session
}

func TestBuildSettlementRehydratesANonResidentEscrowReadOnly(t *testing.T) {
	t.Parallel()
	serving := newSessions(map[string]*fakeSession{"5": settleableSession(9)})
	readOnly := newSessions(map[string]*fakeSession{"5": settleableSession(9)})
	registry := New(Deps{ServingSessions: serving.open, ReadOnlySessions: readOnly.open, Now: fixedClock()})

	input, err := registry.BuildSettlement(context.Background(), "5")
	if err != nil {
		t.Fatalf("BuildSettlement = %v, want nil", err)
	}
	if input.EscrowID != 5 || input.Nonce != 9 {
		t.Errorf("settlement input = escrow %d nonce %d, want escrow 5 nonce 9", input.EscrowID, input.Nonce)
	}
	if got := readOnly.calls.Load(); got != 1 {
		t.Errorf("read-only factory calls = %d, want 1", got)
	}
	if got := serving.calls.Load(); got != 0 {
		t.Errorf("serving factory calls = %d, want 0: building a payload needs neither chain nor hosts", got)
	}
	if got := readOnly.byEscrow["5"].closeCalls.Load(); got != 1 {
		t.Errorf("Close calls on the rehydrated session = %d, want 1", got)
	}
}

func TestBuildSettlementUsesTheResidentSessionWithoutRehydrating(t *testing.T) {
	t.Parallel()
	resident := settleableSession(4)
	serving := newSessions(map[string]*fakeSession{"5": resident})
	readOnly := newSessions(map[string]*fakeSession{"5": settleableSession(4)})
	registry := New(Deps{ServingSessions: serving.open, ReadOnlySessions: readOnly.open, Now: fixedClock()})
	mustAdd(t, registry, "5", "qwen")

	input, err := registry.BuildSettlement(context.Background(), "5")
	if err != nil {
		t.Fatalf("BuildSettlement = %v, want nil", err)
	}
	if input.Nonce != 4 {
		t.Errorf("settlement nonce = %d, want 4 (the resident session's)", input.Nonce)
	}
	if got := readOnly.calls.Load(); got != 0 {
		t.Errorf("read-only factory calls = %d, want 0", got)
	}
	if got := resident.closeCalls.Load(); got != 0 {
		t.Errorf("Close calls on the resident session = %d, want 0", got)
	}
}

func TestBuildSettlementOrdersHostStatsAndSignaturesBySlot(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ReadOnlySessions: newSessions(map[string]*fakeSession{"5": settleableSession(9)}).open,
		Now:              fixedClock(),
	})

	input, err := registry.BuildSettlement(context.Background(), "5")
	if err != nil {
		t.Fatalf("BuildSettlement = %v, want nil", err)
	}
	wantStats := []chain.SettlementHostStat{
		{SlotID: 0, Invalid: 4, Cost: 10},
		{SlotID: 2, Missed: 1, Cost: 30, RequiredValidations: 3, CompletedValidations: 2},
	}
	if !reflect.DeepEqual(input.HostStats, wantStats) {
		t.Errorf("host stats = %+v, want %+v (slot order, nil entries dropped)", input.HostStats, wantStats)
	}
	wantSigs := []chain.SettlementSlotSig{
		{SlotID: 0, Signature: []byte("sig-0")},
		{SlotID: 2, Signature: []byte("sig-2")},
	}
	if !reflect.DeepEqual(input.SlotSigs, wantSigs) {
		t.Errorf("slot signatures = %+v, want %+v", input.SlotSigs, wantSigs)
	}
	if got, want := input.Fees, uint64(11); got != want {
		t.Errorf("fees = %d, want %d", got, want)
	}
	if got, want := input.Version, "v2"; got != want {
		t.Errorf("version = %q, want %q", got, want)
	}
	if len(input.StateRoot) == 0 || len(input.RestHash) == 0 {
		t.Errorf("state root (%d bytes) and rest hash (%d bytes) must both be populated", len(input.StateRoot), len(input.RestHash))
	}
}

func TestBuildSettlementRejectsANonNumericEscrowID(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ReadOnlySessions: newSessions(map[string]*fakeSession{"escrow-five": settleableSession(9)}).open,
		Now:              fixedClock(),
	})

	_, err := registry.BuildSettlement(context.Background(), "escrow-five")

	if err == nil {
		t.Fatal("BuildSettlement = nil, want an error: the chain message carries a numeric escrow id")
	}
}

// Finalizing collects signatures from the hosts, so a read-only rehydration cannot do it.
func TestFinalizeRehydratesANonResidentEscrowWithAServingSession(t *testing.T) {
	t.Parallel()
	serving := newSessions(map[string]*fakeSession{"5": settleableSession(9)})
	readOnly := newSessions(map[string]*fakeSession{"5": settleableSession(9)})
	registry := New(Deps{ServingSessions: serving.open, ReadOnlySessions: readOnly.open, Now: fixedClock()})

	if err := registry.Finalize(context.Background(), "5"); err != nil {
		t.Fatalf("Finalize = %v, want nil", err)
	}

	if got := serving.calls.Load(); got != 1 {
		t.Errorf("serving factory calls = %d, want 1", got)
	}
	if got := readOnly.calls.Load(); got != 0 {
		t.Errorf("read-only factory calls = %d, want 0: it has no host clients to finalize against", got)
	}
	rehydrated := serving.byEscrow["5"]
	if got := rehydrated.finalizeCalls.Load(); got != 1 {
		t.Errorf("Finalize calls on the rehydrated session = %d, want 1", got)
	}
	if got := rehydrated.closeCalls.Load(); got != 1 {
		t.Errorf("Close calls on the rehydrated session = %d, want 1", got)
	}
}

func TestFinalizeUsesTheResidentSessionWithoutRehydrating(t *testing.T) {
	t.Parallel()
	resident := settleableSession(9)
	serving := newSessions(map[string]*fakeSession{"5": resident})
	readOnly := newSessions(map[string]*fakeSession{"5": settleableSession(9)})
	registry := New(Deps{ServingSessions: serving.open, ReadOnlySessions: readOnly.open, Now: fixedClock()})
	mustAdd(t, registry, "5", "qwen")

	if err := registry.Finalize(context.Background(), "5"); err != nil {
		t.Fatalf("Finalize = %v, want nil", err)
	}

	if got := serving.calls.Load(); got != 1 {
		t.Errorf("serving factory calls = %d, want 1 (the Add only)", got)
	}
	if got := readOnly.calls.Load(); got != 0 {
		t.Errorf("read-only factory calls = %d, want 0", got)
	}
	if got := resident.finalizeCalls.Load(); got != 1 {
		t.Errorf("Finalize calls = %d, want 1", got)
	}
	if got := resident.closeCalls.Load(); got != 0 {
		t.Errorf("Close calls on the resident session = %d, want 0", got)
	}
}

func TestFinalizeLetsAnEscrowAlreadyInSettlementCollectMissingSignatures(t *testing.T) {
	t.Parallel()
	resident := settleableSession(9)
	resident.setPhase(types.PhaseSettlement)
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"5": resident}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "5", "qwen")

	if err := registry.Finalize(context.Background(), "5"); err != nil {
		t.Fatalf("Finalize = %v, want nil", err)
	}

	if got := resident.finalizeCalls.Load(); got != 1 {
		t.Errorf("Finalize calls on an escrow already in settlement = %d, want 1: only the session knows whether its quorum is held", got)
	}
}

func TestADrainedEscrowClosesOnlyAfterTheFinalizeRunningOnIt(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, releaseRequest, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}
	var closedDuringFinalize int64
	session.onFinalize = func() {
		releaseRequest()
		registry.closing.Wait()
		closedDuringFinalize = session.closeCalls.Load()
	}

	if err := registry.Finalize(context.Background(), "1"); err != nil {
		t.Fatalf("Finalize(1) = %v, want nil", err)
	}
	registry.closing.Wait()

	if closedDuringFinalize != 0 {
		t.Fatalf("Close calls while Finalize ran = %d, want 0: the drain closed the store under the settlement", closedDuringFinalize)
	}
	if got := session.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls after Finalize = %d, want 1", got)
	}
}

func TestASettlementHoldIsNotARequest(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	_, release, held := registry.HoldSettlement("1")
	if !held {
		t.Fatal("HoldSettlement(1) = false, want true")
	}
	defer release()

	if registry.IsBusy("1") {
		t.Error("IsBusy(1) under a settlement hold = true: a finalize would refuse itself as busy")
	}
	if candidate, _ := registry.Routable("1"); candidate.ActiveUsers != 0 {
		t.Errorf("ActiveUsers under a settlement hold = %d, want 0: routing must not price a read as traffic", candidate.ActiveUsers)
	}
}

func TestRetiringAnEscrowUnderASettlementHoldClosesItOnlyAfterTheHold(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, held := registry.HoldSettlement("1")
	if !held {
		t.Fatal("HoldSettlement(1) = false, want true")
	}

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}
	closedUnderTheHold := session.closeCalls.Load()
	release()
	registry.closing.Wait()

	if closedUnderTheHold != 0 {
		t.Fatalf("Close calls under the settlement hold = %d, want 0", closedUnderTheHold)
	}
	if got := session.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls after the hold = %d, want 1", got)
	}
}

func TestASettlementHoldIsRefusedOnceTheCloseHasStarted(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, releaseRequest, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}
	flushing, finishFlush := make(chan struct{}), make(chan struct{})
	session.onFlush = func() {
		close(flushing)
		<-finishFlush
	}

	releaseRequest()
	<-flushing
	_, releaseHold, held := registry.HoldSettlement("1")
	close(finishFlush)
	registry.closing.Wait()

	if held {
		releaseHold()
		registry.closing.Wait()
		t.Fatal("HoldSettlement(1) during the close = true, want false: a hold on a closing session outlives its store")
	}
	if got := session.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls = %d, want exactly 1", got)
	}
}

func TestFinalizeReportsTheSessionFailure(t *testing.T) {
	t.Parallel()
	resident := settleableSession(9)
	resident.finalizeErr = errors.New("quorum not reached")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"5": resident}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "5", "qwen")

	err := registry.Finalize(context.Background(), "5")

	if !errors.Is(err, resident.finalizeErr) {
		t.Errorf("Finalize = %v, want it to wrap %v", err, resident.finalizeErr)
	}
}

func TestSettlementPathsReportAMissingFactory(t *testing.T) {
	t.Parallel()
	registry := New(Deps{Now: fixedClock()})

	if err := registry.Finalize(context.Background(), "5"); err == nil {
		t.Error("Finalize without a serving factory = nil, want an error")
	}
	if _, err := registry.BuildSettlement(context.Background(), "5"); err == nil {
		t.Error("BuildSettlement without a read-only factory = nil, want an error")
	}
}
