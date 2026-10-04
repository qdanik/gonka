package escrow

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"
)

// Test flow:
//  1. Table-driven over the chain epoch, the label, the effective and latest epochs, the height and the switch height, with a 600-block margin.
//  2. Assert deadlineAt reports whether the deadline is known, passed or inside the margin, and the settle-by height.
func TestTheDeadlineReadsTheChainEpochAgainstTheEffectiveEpoch(t *testing.T) {
	lastEpoch := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2100, EpochSwitchBlockHeight: 3100}
	insideMargin := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}
	testCases := []struct {
		name     string
		record   store.DevshardRecord
		snapshot chain.PhaseSnapshot
		want     settleDeadline
	}{
		{name: "the chain epoch is the effective one", record: store.DevshardRecord{ChainEpoch: 8}, snapshot: insideMargin, want: settleDeadline{known: true}},
		{name: "last epoch, outside the margin", record: store.DevshardRecord{ChainEpoch: 7}, snapshot: lastEpoch, want: settleDeadline{known: true, settleBy: 3100}},
		{name: "last epoch, inside the margin", record: store.DevshardRecord{ChainEpoch: 7}, snapshot: insideMargin, want: settleDeadline{known: true, inMargin: true, settleBy: 3100}},
		{name: "two epochs behind has passed", record: store.DevshardRecord{ChainEpoch: 6}, snapshot: lastEpoch, want: settleDeadline{known: true, passed: true, inMargin: true}},
		{name: "an unresolved row reads its label less one", record: store.DevshardRecord{RotationEpoch: 8}, snapshot: insideMargin, want: settleDeadline{known: true, inMargin: true, settleBy: 3100}},
		{name: "a resolved chain epoch wins over the label", record: store.DevshardRecord{ChainEpoch: 7, RotationEpoch: 10}, snapshot: insideMargin, want: settleDeadline{known: true, inMargin: true, settleBy: 3100}},
		{name: "an unknown effective epoch reads the latest", record: store.DevshardRecord{ChainEpoch: 7}, snapshot: chain.PhaseSnapshot{EpochIndex: 8, BlockHeight: 2050, EpochSwitchBlockHeight: 2100}, want: settleDeadline{known: true, inMargin: true, settleBy: 2100}},
		{name: "no switch height is inside the margin", record: store.DevshardRecord{ChainEpoch: 7}, snapshot: chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2100}, want: settleDeadline{known: true, inMargin: true}},
		{name: "a row with no epoch at all", record: store.DevshardRecord{}, snapshot: insideMargin, want: settleDeadline{}},
		{name: "a cold start", record: store.DevshardRecord{ChainEpoch: 7}, snapshot: chain.PhaseSnapshot{}, want: settleDeadline{}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := deadlineAt(testCase.record, testCase.snapshot, 600); got != testCase.want {
				t.Fatalf("deadlineAt(%s) = %+v, want %+v", testCase.name, got, testCase.want)
			}
		})
	}
}

// Test flow:
//  1. Table-driven over the blocks around the switch S into epoch 8, with the snapshot each block yields: at S it already names the next switch.
//  2. Assert a row of chain epoch 7 enters no margin at S or S+1, and a row of chain epoch 6 is inside its margin at S-1 and passed at S.
func TestTheDeadlineAroundTheSwitchBlockReadsTheNextSwitch(t *testing.T) {
	beforeSwitch := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 7, BlockHeight: 2099, EpochSwitchBlockHeight: 2100}
	atSwitch := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2100, EpochSwitchBlockHeight: 3100}
	afterSwitch := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2101, EpochSwitchBlockHeight: 3100}
	testCases := []struct {
		name       string
		chainEpoch uint64
		snapshot   chain.PhaseSnapshot
		want       settleDeadline
	}{
		{name: "epoch 7 at S-1", chainEpoch: 7, snapshot: beforeSwitch, want: settleDeadline{known: true}},
		{name: "epoch 7 at S", chainEpoch: 7, snapshot: atSwitch, want: settleDeadline{known: true, settleBy: 3100}},
		{name: "epoch 7 at S+1", chainEpoch: 7, snapshot: afterSwitch, want: settleDeadline{known: true, settleBy: 3100}},
		{name: "epoch 6 at S-1", chainEpoch: 6, snapshot: beforeSwitch, want: settleDeadline{known: true, inMargin: true, settleBy: 2100}},
		{name: "epoch 6 at S", chainEpoch: 6, snapshot: atSwitch, want: settleDeadline{known: true, passed: true, inMargin: true}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := deadlineAt(store.DevshardRecord{ChainEpoch: testCase.chainEpoch}, testCase.snapshot, 600); got != testCase.want {
				t.Fatalf("deadlineAt(%s) = %+v, want %+v", testCase.name, got, testCase.want)
			}
		})
	}
}

// Test flow:
//  1. Sort a deadline with no settle-by height, a later and an earlier settle-by height, and a passed deadline by their order.
//  2. Assert the passed deadline comes first, then the earlier height, the later one, and the one without a height last.
func TestDeadlinesOrderPassedFirstThenBySettleByHeight(t *testing.T) {
	passed := settleDeadline{known: true, passed: true, inMargin: true}
	earlier := settleDeadline{known: true, settleBy: 2100}
	later := settleDeadline{known: true, settleBy: 3100}
	unknown := settleDeadline{known: true}
	deadlines := []settleDeadline{unknown, later, earlier, passed}

	slices.SortFunc(deadlines, func(left, right settleDeadline) int { return cmp.Compare(left.order(), right.order()) })

	if want := []settleDeadline{passed, earlier, later, unknown}; !slices.Equal(deadlines, want) {
		t.Fatalf("deadlines sorted by order() = %+v, want %+v", deadlines, want)
	}
}

func deadlineManager(t *testing.T, testStore *fakeStore, modelsJSON string) (*Manager, *recordingLifecycleNarrator) {
	t.Helper()
	cfg := config.Defaults()
	cfg.Rotation.SettlementEnabled = true
	cfg.Rotation.ModelsJSON = modelsJSON
	deps := testManagerDeps(t, testStore, &fakeTxClient{}, &fakeSnapshotSource{}, &cfg)
	deps.ChainFacts = &fakeTxClient{}
	narrator := &recordingLifecycleNarrator{}
	deps.Narrator = narrator
	return mustManager(t, deps), narrator
}

// Test flow:
//  1. Store, all of chain epoch 7 inside the margin of the switch into epoch 9: a serving row, a serving row of a model with settlement off, an operator-deactivated row, a row gone from chain; and a serving row of chain epoch 8.
//  2. Run the deadline pass twice.
//  3. Assert only the first row was parked and narrated as reached; the settlement-off and deactivated rows were narrated once each and left as they were; the gone row and the chain-epoch-8 row were untouched.
func TestTheDeadlineParksWhatMaySettleAndNarratesTheRest(t *testing.T) {
	testStore := newFakeStore()
	rows := []store.DevshardRecord{
		{EscrowID: "1", Model: "model-a", Active: true, ChainEpoch: 7},
		{EscrowID: "2", Model: "model-b", Active: true, ChainEpoch: 7},
		{EscrowID: "3", Model: "model-a", ChainEpoch: 7},
		{EscrowID: "4", Model: "model-a", ChainEpoch: 7, GoneFromChain: true},
		{EscrowID: "5", Model: "model-a", Active: true, ChainEpoch: 8},
	}
	for _, row := range rows {
		testStore.devshards[row.EscrowID] = row
	}
	manager, narrator := deadlineManager(t, testStore, `[{"model_id":"model-b","amount":1000,"settlement_enabled":false}]`)
	snapshot := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}

	updated, err := manager.parkAtDeadline(context.Background(), snapshot, rows)
	if err != nil {
		t.Fatalf("parkAtDeadline() = %v, want nil", err)
	}
	if _, err := manager.parkAtDeadline(context.Background(), snapshot, updated); err != nil {
		t.Fatalf("parkAtDeadline() again = %v, want nil", err)
	}

	assertParked(t, testStore, "1")
	if !slices.ContainsFunc(updated, func(record store.DevshardRecord) bool {
		return record.EscrowID == "1" && !record.Active && record.SettlementPending
	}) {
		t.Fatalf("parkAtDeadline() rows = %+v, want row 1 parked in the returned slice", updated)
	}
	if !testStore.devshards["2"].Active || !testStore.devshards["5"].Active || testStore.devshards["3"].SettlementPending || testStore.devshards["4"].SettlementPending {
		t.Fatalf("rows 2-5 = %+v %+v %+v %+v, want untouched", testStore.devshards["2"], testStore.devshards["3"], testStore.devshards["4"], testStore.devshards["5"])
	}
	want := []string{
		"deadline reached 1 epoch 7 settle by 3100",
		"deadline unsettled 2 settle by 3100: settlement_disabled",
		"deadline unsettled 3 settle by 3100: operator_deactivated",
	}
	recorded := slices.DeleteFunc(narrator.recorded(), func(line string) bool { return line == "parked 1" })
	if !slices.Equal(recorded, want) {
		t.Fatalf("narration = %v, want %v", recorded, want)
	}
}

// Test flow:
//  1. Store a parked row of chain epoch 6 at effective epoch 8, two epochs behind.
//  2. Run the deadline pass.
//  3. Assert it was narrated as deadline_passed and left parked for settlePending to drop.
func TestAPassedDeadlineIsNarrated(t *testing.T) {
	testStore := newFakeStore()
	row := store.DevshardRecord{EscrowID: "1", Model: "model-a", SettlementPending: true, ChainEpoch: 6}
	testStore.devshards["1"] = row
	manager, narrator := deadlineManager(t, testStore, "")

	if _, err := manager.parkAtDeadline(context.Background(), chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2100, EpochSwitchBlockHeight: 3100}, []store.DevshardRecord{row}); err != nil {
		t.Fatalf("parkAtDeadline() = %v, want nil", err)
	}

	if recorded := narrator.recorded(); !slices.Equal(recorded, []string{"deadline unsettled 1 settle by 0: deadline_passed"}) {
		t.Fatalf("narration = %v, want one deadline_passed line", recorded)
	}
}

// Test flow:
//  1. Store a row an operator deactivated after the tick read it as serving, inside its margin.
//  2. Run the deadline pass over the stale serving copy.
//  3. Assert the stored row stays inactive and unparked, and nothing was narrated as reached.
func TestTheDeadlineDoesNotParkARowAnOperatorDeactivatedMidTick(t *testing.T) {
	testStore := newFakeStore()
	testStore.devshards["1"] = store.DevshardRecord{EscrowID: "1", Model: "model-a", ChainEpoch: 7}
	stale := store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true, ChainEpoch: 7}
	manager, narrator := deadlineManager(t, testStore, "")
	snapshot := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}

	updated, err := manager.parkAtDeadline(context.Background(), snapshot, []store.DevshardRecord{stale})
	if err != nil {
		t.Fatalf("parkAtDeadline() = %v, want nil", err)
	}

	if stored := testStore.devshards["1"]; stored.SettlementPending || stored.Active {
		t.Fatalf("stored row = %+v, want still deactivated and unparked", stored)
	}
	if updated[0].SettlementPending || len(narrator.recorded()) != 0 {
		t.Fatalf("parkAtDeadline() row = %+v narration = %v, want no park and no narration", updated[0], narrator.recorded())
	}
}

// Test flow:
//  1. Store a serving row inside its margin; build one manager without a chain-facts lookup, and run another over a snapshot with no epoch.
//  2. Run the deadline pass on each.
//  3. Assert neither touched the row.
func TestTheDeadlineNeedsChainFactsAndAnEpoch(t *testing.T) {
	row := store.DevshardRecord{EscrowID: "1", Model: "model-a", Active: true, ChainEpoch: 7}
	inside := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}

	withoutFacts := newFakeStore()
	withoutFacts.devshards["1"] = row
	cfg := config.Defaults()
	manager := mustManager(t, testManagerDeps(t, withoutFacts, &fakeTxClient{}, &fakeSnapshotSource{}, &cfg))
	if _, err := manager.parkAtDeadline(context.Background(), inside, []store.DevshardRecord{row}); err != nil || !withoutFacts.devshards["1"].Active {
		t.Fatalf("parkAtDeadline() without chain facts = %v, row %+v, want nil and still serving", err, withoutFacts.devshards["1"])
	}

	coldStart := newFakeStore()
	coldStart.devshards["1"] = row
	manager, _ = deadlineManager(t, coldStart, "")
	if _, err := manager.parkAtDeadline(context.Background(), chain.PhaseSnapshot{}, []store.DevshardRecord{row}); err != nil || !coldStart.devshards["1"].Active {
		t.Fatalf("parkAtDeadline() on a cold start = %v, row %+v, want nil and still serving", err, coldStart.devshards["1"])
	}
}

// orderedSettlementSource records which escrows reached Finalize, in order, and how often the busy check ran.
type orderedSettlementSource struct {
	*fakeSettlementSource
	mu        sync.Mutex
	finalized []string
	busyAsked int
}

func (source *orderedSettlementSource) Finalize(ctx context.Context, escrowID string) error {
	source.mu.Lock()
	source.finalized = append(source.finalized, escrowID)
	source.mu.Unlock()
	return source.fakeSettlementSource.Finalize(ctx, escrowID)
}

func (source *orderedSettlementSource) IsBusy(escrowID string) bool {
	source.mu.Lock()
	source.busyAsked++
	source.mu.Unlock()
	return source.fakeSettlementSource.IsBusy(escrowID)
}

// Test flow:
//  1. Store five parked rows of chain epoch 8 outside any margin and two of chain epoch 7 inside the margin of the switch into epoch 9, every escrow still busy.
//  2. Settle the pending rows once.
//  3. Assert the two inside the margin were finalized first, in escrow-id order, despite being busy, and four of the other five were tried and deferred as busy: the budget counted only them.
func TestPendingSettlesRunInDeadlineOrderWithTheMarginForcedAndUnbudgeted(t *testing.T) {
	testStore := newFakeStore()
	devshards := make([]store.DevshardRecord, 0, 7)
	for _, escrowID := range []string{"10", "11", "12", "13", "14"} {
		devshards = append(devshards, store.DevshardRecord{EscrowID: escrowID, PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", SettlementPending: true, ChainEpoch: 8})
	}
	for _, escrowID := range []string{"31", "30"} {
		devshards = append(devshards, store.DevshardRecord{EscrowID: escrowID, PrivateKeyEnv: "MODEL_A_KEY", Model: "model-a", SettlementPending: true, ChainEpoch: 7})
	}
	for _, record := range devshards {
		testStore.devshards[record.EscrowID] = record
	}
	source := &orderedSettlementSource{fakeSettlementSource: &fakeSettlementSource{busy: true}}
	manager := &Manager{
		tx:               settlingTxClient(),
		store:            testStore,
		signer:           &fakeSignerSource{signer: testSigner(t)},
		settlementSource: source,
		config:           holderWithSettlementEnabled(true),
		chainFacts:       &fakeTxClient{},
		now:              time.Now,
	}
	snapshot := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}

	err := manager.settlePending(context.Background(), snapshot, devshards)

	if !errors.Is(err, ErrDevshardBusy) {
		t.Fatalf("settlePending() = %v, want the deferred busy settles reported", err)
	}
	if !slices.Equal(source.finalized, []string{"30", "31"}) {
		t.Fatalf("finalized %v, want [30 31]: the rows inside the margin, forced, in id order", source.finalized)
	}
	if source.busyAsked != pendingSettleBudget {
		t.Fatalf("busy checks = %d, want %d: only rows outside the margin count against the budget", source.busyAsked, pendingSettleBudget)
	}
}

// Test flow:
//  1. Store a settlement-off row and an operator-deactivated row inside their margin, and run the deadline pass twice.
//  2. Assert each was counted once under its model and reason.
func TestEveryUnsettledDeadlineIsCountedOnce(t *testing.T) {
	testStore := newFakeStore()
	rows := []store.DevshardRecord{
		{EscrowID: "2", Model: "model-b", Active: true, ChainEpoch: 7},
		{EscrowID: "3", Model: "model-a", ChainEpoch: 7},
	}
	for _, row := range rows {
		testStore.devshards[row.EscrowID] = row
	}
	manager, _ := deadlineManager(t, testStore, `[{"model_id":"model-b","amount":1000,"settlement_enabled":false}]`)
	snapshot := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}

	for range 2 {
		if _, err := manager.parkAtDeadline(context.Background(), snapshot, rows); err != nil {
			t.Fatalf("parkAtDeadline() = %v, want nil", err)
		}
	}

	want := map[DeadlineUnsettled]uint64{
		{Model: "model-b", Reason: "settlement_disabled"}:  1,
		{Model: "model-a", Reason: "operator_deactivated"}: 1,
	}
	if counts := manager.DeadlineUnsettledCounts(); !maps.Equal(counts, want) {
		t.Fatalf("DeadlineUnsettledCounts() = %v, want %v", counts, want)
	}
}

// Test flow:
//  1. Store an inactive, unparked row of chain epoch 7 inside its margin, on a manager whose signer cannot resolve the row's key.
//  2. Run the deadline pass.
//  3. Assert it was narrated as key_missing, not as an operator's deactivation.
func TestADeactivatedRowWhoseKeyIsMissingIsNarratedKeyMissing(t *testing.T) {
	testStore := newFakeStore()
	row := store.DevshardRecord{EscrowID: "3", Model: "model-a", ChainEpoch: 7, PrivateKeyEnv: "GONE_KEY"}
	testStore.devshards["3"] = row
	manager, narrator := deadlineManager(t, testStore, "")
	manager.signer = &fakeSignerSource{err: errors.New("private key GONE_KEY is not set")}
	snapshot := chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8, BlockHeight: 2600, EpochSwitchBlockHeight: 3100}

	if _, err := manager.parkAtDeadline(context.Background(), snapshot, []store.DevshardRecord{row}); err != nil {
		t.Fatalf("parkAtDeadline() = %v, want nil", err)
	}

	if recorded := narrator.recorded(); !slices.Equal(recorded, []string{"deadline unsettled 3 settle by 3100: key_missing"}) {
		t.Fatalf("narration = %v, want key_missing", recorded)
	}
}
