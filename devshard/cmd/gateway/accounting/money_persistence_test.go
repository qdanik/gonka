package accounting

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"devshard/cmd/gateway/filters"
	"devshard/types"
)

func finishedInference(inputTokens, outputTokens uint64) map[uint64]*types.InferenceRecord {
	return map[uint64]*types.InferenceRecord{
		5: {
			Status: types.StatusFinished, ExecutorSlot: 1, InputLength: 400, MaxTokens: 512,
			InputTokens: inputTokens, OutputTokens: outputTokens, ReservedCost: 900, ActualCost: 300,
		},
	}
}

func moneyOf(t *testing.T, book *Book) SlotMoney {
	t.Helper()
	var money SlotMoney
	for _, record := range book.Query(QueryFilter{}) {
		money.add(SlotMoney{
			Reserved:       record.ReservedCost,
			Actual:         record.ActualCost,
			Refunded:       record.RefundedCost,
			EstimatedInput: record.EstimatedInput,
			EstimatedError: record.EstimatedError,
			MaxTokens:      record.MaxTokens,
			Input:          record.InputTokens,
			CountedNonces:  record.CountedNonces,
		})
	}
	return money
}

// The escrow an operator deactivated is never read from the chain again, so its money has to be the
// gateway's own: a restart that leaves the nonce counts standing must leave the tokens standing too.
func TestTheTokensOfARetiredEscrowSurviveARestart(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 8); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.ObserveInferences(testEscrow, finishedInference(100, 42)); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}
	book.RetireEscrow(testEscrow)

	before := moneyOf(t, book)
	after := moneyOf(t, saveAndReload(t, book, openTestStore(t)))

	if before.Input != 100 || before.CountedNonces != 1 {
		t.Fatalf("before the restart input=%d counted=%d, want 100 and 1", before.Input, before.CountedNonces)
	}
	if after != before {
		t.Fatalf("after the restart %+v, want %+v", after, before)
	}
}

// A live escrow is read from the chain every sweep, and that reading is the whole truth: the saved
// fold stands in only until it arrives, and adding the two would count every token twice.
func TestAChainReadingReplacesTheSavedTokensRatherThanAddingToThem(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveInferences(testEscrow, finishedInference(100, 42)); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}
	restored := saveAndReload(t, book, openTestStore(t))

	if err := restored.ObserveInferences(testEscrow, finishedInference(100, 55)); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}

	money := moneyOf(t, restored)
	if money.Input != 100 || money.MaxTokens != 512 || money.CountedNonces != 1 {
		t.Fatalf("input=%d max_tokens=%d counted=%d, want 100, 512 and 1", money.Input, money.MaxTokens, money.CountedNonces)
	}
}

// Every slot keeps its own share, over a restart as well: the two hosts that answered are told apart
// from each other and from the one whose nonce timed out.
func TestTheSavedTokensComeBackOnTheSlotThatEarnedThem(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveInferences(testEscrow, map[uint64]*types.InferenceRecord{
		5: {Status: types.StatusFinished, ExecutorSlot: 1, InputLength: 400, InputTokens: 100, OutputTokens: 42},
		6: {Status: types.StatusFinished, ExecutorSlot: 2, InputLength: 800, InputTokens: 200, OutputTokens: 84},
		7: {Status: types.StatusTimedOut, ExecutorSlot: 3, InputLength: 1200, ReservedCost: 700},
	}); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}
	book.RetireEscrow(testEscrow)
	expected := map[string]SlotMoney{
		participantFor(0): {},
		participantFor(1): {Input: 100, EstimatedInput: uint64(filters.EstimatedPromptTokens(400)), CountedNonces: 1},
		participantFor(2): {Input: 200, EstimatedInput: uint64(filters.EstimatedPromptTokens(800)), CountedNonces: 1},
		participantFor(3): {Reserved: 700, Refunded: 700, EstimatedError: uint64(filters.EstimatedPromptTokens(1200))},
	}

	for _, book := range []*Book{book, saveAndReload(t, book, openTestStore(t))} {
		for participant, money := range slotTotals(book.Query(QueryFilter{})) {
			if money != expected[participant] {
				t.Fatalf("%s holds %+v, want %+v", participant, money, expected[participant])
			}
		}
	}
}

func slotTotals(records []ParticipantRecord) map[string]SlotMoney {
	totals := make(map[string]SlotMoney)
	for _, record := range records {
		for _, slot := range record.Slots {
			totals[record.Participant] = SlotMoney{
				Reserved: slot.ReservedCost, Actual: slot.ActualCost, Refunded: slot.RefundedCost,
				EstimatedInput: slot.EstimatedInput, EstimatedError: slot.EstimatedError,
				MaxTokens: slot.MaxTokens, Input: slot.InputTokens,
				CountedNonces: slot.CountedNonces,
			}
		}
	}
	return totals
}

// The money table is an addition to a store this gateway already writes, so the deploy that brings it
// must find the ledger it left behind rather than drop an epoch of counters to make room.
func TestAStoreWrittenBeforeTheMoneyTableIsKeptRatherThanDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounting.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore(): %v", err)
	}
	book := newTestBook(t, 4)
	if err := book.RecordGhost(testEscrow, 5, "participant_window_full_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save(): %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	dropMoneyTable(t, path)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore() over the older store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	snapshot, err := reopened.Load(context.Background())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if len(snapshot.Escrows) != 1 {
		t.Fatalf("the upgrade left %d escrows, want the one the older store held", len(snapshot.Escrows))
	}
	if len(snapshot.Escrows[0].Counters) == 0 {
		t.Fatal("the upgrade dropped the counters of the escrow it kept")
	}
}

func dropMoneyTable(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening the store as the older build left it: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`DROP TABLE accounting_money`); err != nil {
		t.Fatalf("dropping the money table: %v", err)
	}
}
