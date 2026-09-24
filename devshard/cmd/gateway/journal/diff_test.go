package journal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/internal/logcapture"
	"devshard/types"
)

// composedDiff carries a valid and an invalid verdict, an applied timeout, and a transaction the ledger ignores.
func composedDiff() *types.Diff {
	return &types.Diff{Nonce: 9, Txs: []*types.DevshardTx{
		{Tx: &types.DevshardTx_Validation{Validation: &types.MsgValidation{InferenceId: 4, ValidatorSlot: 1, Valid: true}}},
		{Tx: &types.DevshardTx_Validation{Validation: &types.MsgValidation{InferenceId: 5, ValidatorSlot: 0, Valid: false}}},
		{Tx: &types.DevshardTx_TimeoutInference{TimeoutInference: &types.MsgTimeoutInference{InferenceId: 6}}},
		{Tx: &types.DevshardTx_StartInference{}},
	}}
}

// Test flow:
//  1. Build a `composedDiff` (two validations, an applied timeout, and one ignored transaction).
//  2. Convert it to facts via `diffFacts`.
//  3. Assert the returned facts match the expected sequence, in the diff's own order.
func TestADiffIsReadIntoFactsInOrder(t *testing.T) {
	require.Equal(t, []DiffFact{
		{Kind: DiffFactValidation, Nonce: 4, ValidatorSlot: 1},
		{Kind: DiffFactValidation, Nonce: 5, ValidatorSlot: 0},
		{Kind: DiffFactInvalidVerdict, Nonce: 5, ValidatorSlot: 0},
		{Kind: DiffFactAppliedTimeout, Nonce: 6},
	}, diffFacts(composedDiff()))
}

// Test flow:
//  1. Build a journal with a `ledgerSpy` and a `composedDiff`.
//  2. Record the diff via `DiffComposed`, then immediately clear the diff's `Txs`.
//  3. Flush the journal.
//  4. Assert the ledger spy received the facts as copies, unaffected by clearing `Txs`.
func TestAComposedDiffReachesTheLedgerAsFacts(t *testing.T) {
	ledger := &ledgerSpy{}
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: ledger})
	diff := composedDiff()

	events.DiffComposed("escrow-1", diff)
	diff.Txs = nil
	events.Flush()

	require.Equal(t, []string{
		"diff escrow-1 1 4 1",
		"diff escrow-1 1 5 0",
		"diff escrow-1 2 5 0",
		"diff escrow-1 3 6 0",
	}, ledger.arrived())
}

// Test flow:
//  1. Build a journal with a `ledgerSpy`.
//  2. Record a diff via `DiffComposed` that carries no ledger fact (only a `StartInference` transaction).
//  3. Assert `events.accepted` stays zero under the session lock.
func TestADiffWithNoLedgerFactQueuesNothing(t *testing.T) {
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: &ledgerSpy{}})

	events.DiffComposed("escrow-1", &types.Diff{Nonce: 9, Txs: []*types.DevshardTx{{Tx: &types.DevshardTx_StartInference{}}}})

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Zero(t, events.accepted, "a diff with no ledger fact must not take a queue slot under the session lock")
}

// Test flow:
//  1. Build a journal with a `ledgerSpy`.
//  2. Record a warmup probe attempt via `ProbeRecorded`.
//  3. Flush the journal.
//  4. Assert the ledger spy received the probe fact.
func TestAWarmupProbeReachesTheLedger(t *testing.T) {
	ledger := &ledgerSpy{}
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: ledger})

	events.ProbeRecorded("escrow-1", accounting.Attempt{Nonce: 7, Terminal: accounting.TerminalWarmupProbe})
	events.Flush()

	require.Equal(t, []string{"probe escrow-1 7 " + accounting.TerminalWarmupProbe}, ledger.arrived())
}
