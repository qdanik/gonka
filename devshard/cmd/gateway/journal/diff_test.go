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

// Only a copy of what the diff said crosses into the queue, in the order the diff said it.
func TestADiffIsReadIntoFactsInOrder(t *testing.T) {
	require.Equal(t, []DiffFact{
		{Kind: DiffFactValidation, Nonce: 4, ValidatorSlot: 1},
		{Kind: DiffFactValidation, Nonce: 5, ValidatorSlot: 0},
		{Kind: DiffFactInvalidVerdict, Nonce: 5, ValidatorSlot: 0},
		{Kind: DiffFactAppliedTimeout, Nonce: 6},
	}, diffFacts(composedDiff()))
}

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

// Almost every composed diff carries no verdict and no timeout; queueing it would cost the session lock for nothing.
func TestADiffWithNoLedgerFactQueuesNothing(t *testing.T) {
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: &ledgerSpy{}})

	events.DiffComposed("escrow-1", &types.Diff{Nonce: 9, Txs: []*types.DevshardTx{{Tx: &types.DevshardTx_StartInference{}}}})

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Zero(t, events.accepted, "a diff with no ledger fact must not take a queue slot under the session lock")
}

func TestAWarmupProbeReachesTheLedger(t *testing.T) {
	ledger := &ledgerSpy{}
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: ledger})

	events.ProbeRecorded("escrow-1", accounting.Attempt{Nonce: 7, Terminal: accounting.TerminalWarmupProbe})
	events.Flush()

	require.Equal(t, []string{"probe escrow-1 7 " + accounting.TerminalWarmupProbe}, ledger.arrived())
}
