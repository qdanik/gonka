package journal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/internal/logcapture"
)

// Each want is the warmup's line after L6 and L13; the comments name what the line carried before.
func TestWarmupTransitionsRenderWithoutNilErrorsAndWithAFailedVoteAtWarn(t *testing.T) {
	probeFailure := errors.New("host stopped answering")
	catchUpFailure := errors.New("catch-up timed out")
	ledgerRefusal := errors.New("ledger closed")

	testCases := []struct {
		name    string
		produce func(events *Journal)
		want    logcapture.Entry
	}{
		{
			name:    "no nonce and no error: before, it carried error nil",
			produce: func(events *Journal) { events.WarmupFoundNoNonce("escrow-1", nil) },
			want:    logcapture.Entry{Level: "warn", Msg: "escrow warmup found no nonce to spend", Fields: []any{"escrow", "escrow-1"}},
		},
		{
			name:    "no nonce with an error",
			produce: func(events *Journal) { events.WarmupFoundNoNonce("escrow-1", probeFailure) },
			want: logcapture.Entry{Level: "warn", Msg: "escrow warmup found no nonce to spend", Fields: []any{
				"escrow", "escrow-1", "error", probeFailure,
			}},
		},
		{
			name:    "warmed with a clean catch-up: before, it carried catch_up_error nil",
			produce: func(events *Journal) { events.EscrowWarmed("escrow-1", "model-a", 7, true, nil) },
			want: logcapture.Entry{Level: "info", Msg: "escrow warmed", Fields: []any{
				"escrow", "escrow-1", "model", "model-a", "nonce", uint64(7), "served", true,
			}},
		},
		{
			name:    "warmed with a failed catch-up",
			produce: func(events *Journal) { events.EscrowWarmed("escrow-1", "model-a", 7, false, catchUpFailure) },
			want: logcapture.Entry{Level: "info", Msg: "escrow warmed", Fields: []any{
				"escrow", "escrow-1", "model", "model-a", "nonce", uint64(7), "served", false, "catch_up_error", catchUpFailure,
			}},
		},
		{
			name:    "the ledger refuses to open the escrow",
			produce: func(events *Journal) { events.WarmupLedgerOpenFailed("escrow-1", ledgerRefusal) },
			want: logcapture.Entry{Level: "warn", Msg: "escrow warmup could not open the escrow in the ledger", Fields: []any{
				"escrow", "escrow-1", "error", ledgerRefusal,
			}},
		},
		{
			name:    "a completed vote",
			produce: func(events *Journal) { events.WarmupVoted("escrow-1", 7, "completed", "none") },
			want: logcapture.Entry{Level: "info", Msg: "escrow warmup voted on its unfinished nonce", Fields: []any{
				"escrow", "escrow-1", "nonce", uint64(7), "action", "completed", "reason", "none",
			}},
		},
		{
			name:    "a failed vote: before, it was info",
			produce: func(events *Journal) { events.WarmupVoted("escrow-1", 7, "failed", "timeout_collection_error") },
			want: logcapture.Entry{Level: "warn", Msg: "escrow warmup voted on its unfinished nonce", Fields: []any{
				"escrow", "escrow-1", "nonce", uint64(7), "action", "failed", "reason", "timeout_collection_error",
			}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			lines := &logcapture.Recorder{}
			events := newJournal(t, Settings{Lines: lines})

			testCase.produce(events)
			events.Flush()

			require.Equal(t, []logcapture.Entry{testCase.want}, lines.All())
		})
	}
}
