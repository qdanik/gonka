package accounting

import (
	"testing"

	"devshard/types"
)

func TestSlotRecordCarriesWhatTheChainChargedTheSlot(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: "5",
		Model:    "Qwen/Test",
		Slots:    []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	if err := book.ObserveHostStats("5", 0, types.HostStats{Cost: 4_200, Missed: 1}); err != nil {
		t.Fatalf("ObserveHostStats: %v", err)
	}

	records := book.Query(QueryFilter{})

	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	if got := records[0].ChainCost; got != 4_200 {
		t.Errorf("chain cost = %d, want 4200 -- the number the chain already told us", got)
	}
	if got := records[0].ChainMissed; got != 1 {
		t.Errorf("chain missed = %d, want 1 (the neighbouring counters must keep working)", got)
	}
}

func TestNonceCostsAreCarriedFromTheEscrowRecord(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{EscrowID: "5", Model: "Qwen/Test", Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}}}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}

	if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{7: {
		ReservedCost: 10_000,
		ActualCost:   6_400,
		Status:       types.StatusFinished,
		InputTokens:  512,
		OutputTokens: 128,
	}}); err != nil {
		t.Fatalf("ObserveInferences: %v", err)
	}

	totals := queryTotals(t, book)
	if got := totals.ReservedCost; got != 10_000 {
		t.Errorf("reserved = %d, want 10000", got)
	}
	if got := totals.ActualCost; got != 6_400 {
		t.Errorf("actual = %d, want 6400", got)
	}
	if got := totals.RefundedCost; got != 3_600 {
		t.Errorf("refunded = %d, want 3600 (reserved minus actual, never stored)", got)
	}
	if got := totals.InputTokens; got != 512 {
		t.Errorf("input tokens = %d, want 512", got)
	}
	if got := totals.OutputTokens; got != 128 {
		t.Errorf("output tokens = %d, want 128", got)
	}
}

func TestAnUnfinishedNonceRefundsNothing(t *testing.T) {
	t.Parallel()
	for _, status := range []types.InferenceStatus{types.StatusPending, types.StatusStarted} {
		t.Run(statusName(status), func(t *testing.T) {
			t.Parallel()
			book := NewBook(nil)
			if err := book.OpenEscrow(EscrowMetadata{EscrowID: "5", Model: "Qwen/Test", Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}}}); err != nil {
				t.Fatalf("OpenEscrow: %v", err)
			}

			if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{
				7: {ReservedCost: 10_000, Status: status},
			}); err != nil {
				t.Fatalf("ObserveInferences: %v", err)
			}

			totals := queryTotals(t, book)
			if got := totals.ReservedCost; got != 10_000 {
				t.Errorf("reserved = %d, want 10000", got)
			}
			if got := totals.RefundedCost; got != 0 {
				t.Errorf("refunded = %d, want 0 while the nonce is still open", got)
			}
		})
	}
}

func statusName(status types.InferenceStatus) string {
	if status == types.StatusPending {
		return "pending"
	}
	return "started"
}

func TestMoneyIsSummedAcrossTheNoncesOfOneSlot(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: "5", Model: "Qwen/Test",
		Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}, {SlotID: 1, ValidatorAddress: "gonka1bbb"}},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}

	spent := &types.InferenceRecord{
		ReservedCost: 1_000, ActualCost: 400, InputTokens: 10, OutputTokens: 4,
		Status: types.StatusFinished,
	}
	if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{7: spent, 9: spent}); err != nil {
		t.Fatalf("ObserveInferences: %v", err)
	}

	var totals nonceTotals
	for _, record := range book.Query(QueryFilter{}) {
		if record.Participant == "gonka1bbb" {
			totals = record.nonceTotals
		}
	}
	for _, field := range []struct {
		name string
		got  uint64
		want uint64
	}{
		{"reserved", totals.ReservedCost, 2_000},
		{"actual", totals.ActualCost, 800},
		{"refunded", totals.RefundedCost, 1_200},
		{"input tokens", totals.InputTokens, 20},
		{"output tokens", totals.OutputTokens, 8},
	} {
		if field.got != field.want {
			t.Errorf("%s = %d, want %d across two nonces of one slot", field.name, field.got, field.want)
		}
	}
}

func TestMoneyIsSummedAcrossTheSlotsOfOneParticipant(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: "5", Model: "Qwen/Test",
		Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}, {SlotID: 1, ValidatorAddress: "gonka1aaa"}},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}

	spent := &types.InferenceRecord{
		ReservedCost: 1_000, ActualCost: 400, InputTokens: 10, OutputTokens: 4, Status: types.StatusFinished,
	}
	if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{7: spent, 8: spent}); err != nil {
		t.Fatalf("ObserveInferences: %v", err)
	}

	for slotID, cost := range map[uint32]uint64{0: 30, 1: 70} {
		if err := book.ObserveHostStats("5", slotID, types.HostStats{Cost: cost}); err != nil {
			t.Fatalf("ObserveHostStats %d: %v", slotID, err)
		}
	}

	totals := queryTotals(t, book)
	for _, field := range []struct {
		name string
		got  uint64
		want uint64
	}{
		{"reserved", totals.ReservedCost, 2_000},
		{"actual", totals.ActualCost, 800},
		{"refunded", totals.RefundedCost, 1_200},
		{"input tokens", totals.InputTokens, 20},
		{"output tokens", totals.OutputTokens, 8},
		{"chain cost", totals.ChainCost, 100},
	} {
		if field.got != field.want {
			t.Errorf("%s = %d, want %d across the two slots one validator owns", field.name, field.got, field.want)
		}
	}
}

func TestInvalidationRefundsTheWholeReserve(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{EscrowID: "5", Model: "Qwen/Test", Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}}}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}

	if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{
		7: {ReservedCost: 1_000, ActualCost: 400, Status: types.StatusInvalidated},
	}); err != nil {
		t.Fatalf("ObserveInferences: %v", err)
	}

	if got := queryTotals(t, book).RefundedCost; got != 1_000 {
		t.Errorf("refunded = %d, want the whole reserve: invalidation returns the cost on top of the surplus", got)
	}
}

func TestObservingTheSameNonceTwiceDoesNotDoubleTheMoney(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{EscrowID: "5", Model: "Qwen/Test", Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}}}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	record := &types.InferenceRecord{ReservedCost: 10_000, ActualCost: 6_400, OutputTokens: 128, Status: types.StatusFinished}

	for range 3 {
		if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{7: record}); err != nil {
			t.Fatalf("ObserveInferences: %v", err)
		}
	}

	totals := queryTotals(t, book)
	if got := totals.ReservedCost; got != 10_000 {
		t.Errorf("reserved = %d, want 10000 after three sweeps of one nonce", got)
	}
	if got := totals.OutputTokens; got != 128 {
		t.Errorf("output tokens = %d, want 128 after three sweeps", got)
	}
}

func queryTotals(t *testing.T, book *Book) nonceTotals {
	t.Helper()
	records := book.Query(QueryFilter{})
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	return records[0].nonceTotals
}

func TestWhatTheHostWasGivenIsCarriedFromTheEscrowRecord(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{EscrowID: "5", Model: "Qwen/Test", Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}}}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}

	if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{7: {
		InputLength:  2_048,
		MaxTokens:    256,
		InputTokens:  512,
		OutputTokens: 128,
		Status:       types.StatusFinished,
	}}); err != nil {
		t.Fatalf("ObserveInferences: %v", err)
	}

	totals := queryTotals(t, book)
	if got := totals.InputLengthBytes; got != 2_048 {
		t.Errorf("input length = %d, want the 2048 bytes the gateway handed over", got)
	}
	if got := totals.MaxTokens; got != 256 {
		t.Errorf("max tokens = %d, want the 256 the gateway reserved on the output side", got)
	}
	if got := totals.InputTokens; got != 512 {
		t.Errorf("input tokens = %d, want the chain's own count beside the bytes", got)
	}
}

// A burn commits a chain record like any other nonce, so folding it in would report the reservations
// this gateway chose never to send as reservations the host wasted.
func TestABurnedNonceLeavesOutWhatItWasGivenAndNothingElse(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{EscrowID: "5", Model: "Qwen/Test", Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "gonka1aaa"}}}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	if err := book.RecordGhost("5", 7, "participant_window_full_no_send"); err != nil {
		t.Fatalf("RecordGhost: %v", err)
	}

	if err := book.ObserveInferences("5", map[uint64]*types.InferenceRecord{
		7: {InputLength: 60, MaxTokens: 64, ReservedCost: 124, Status: types.StatusTimedOut},
		8: {
			InputLength: 2_048, MaxTokens: 256, ReservedCost: 2_304, ActualCost: 1_000,
			InputTokens: 512, OutputTokens: 128, Status: types.StatusFinished,
		},
	}); err != nil {
		t.Fatalf("ObserveInferences: %v", err)
	}

	totals := queryTotals(t, book)
	for _, field := range []struct {
		name string
		got  uint64
		want uint64
		why  string
	}{
		{"input length", totals.InputLengthBytes, 2_048, "the burn's 60 bytes are the gateway's own prompt"},
		{"max tokens", totals.MaxTokens, 256, "the burn's 64 tokens were reserved by the gateway, not by a host"},
		{"reserved", totals.ReservedCost, 2_428, "money still counts the burn: a burn costs the escrow"},
		{"refunded", totals.RefundedCost, 1_428, "the burn's whole reserve came back, the served nonce's surplus with it"},
		{"output tokens", totals.OutputTokens, 128, "the burn produced nothing to add"},
	} {
		if field.got != field.want {
			t.Errorf("%s = %d, want %d -- %s", field.name, field.got, field.want, field.why)
		}
	}
}
