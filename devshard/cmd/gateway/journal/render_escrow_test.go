package journal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/internal/logcapture"
)

// Each want is the line its producer used to write itself, key for key and type for type.
func TestEscrowTransitionsRenderTheLinesTheirProducersWrote(t *testing.T) {
	closeFailure := errors.New("storage refused to close")
	unverifiable := errors.New("slot 2 signature does not verify")
	missing := errors.New("escrow not found")

	testCases := []struct {
		name    string
		produce func(events *Journal)
		want    logcapture.Entry
	}{
		{
			name:    "an escrow is published",
			produce: func(events *Journal) { events.EscrowServing("7", "qwen") },
			want:    logcapture.Entry{Level: "info", Msg: "escrow serving", Fields: []any{"escrow", "7", "model", "qwen"}},
		},
		{
			name:    "an idle escrow is retired",
			produce: func(events *Journal) { events.EscrowRetired("7") },
			want:    logcapture.Entry{Level: "info", Msg: "escrow retired", Fields: []any{"escrow", "7"}},
		},
		{
			name:    "a busy escrow is retired",
			produce: func(events *Journal) { events.EscrowRetiredDraining("7", 3) },
			want: logcapture.Entry{Level: "info", Msg: "escrow retired, draining", Fields: []any{
				"escrow", "7", "in_flight", int64(3),
			}},
		},
		{
			name:    "a drained escrow closes",
			produce: func(events *Journal) { events.DrainingEscrowClosed("7", nil) },
			want:    logcapture.Entry{Level: "info", Msg: "draining escrow closed", Fields: []any{"escrow", "7"}},
		},
		{
			name:    "a drained escrow fails to close",
			produce: func(events *Journal) { events.DrainingEscrowClosed("7", closeFailure) },
			want: logcapture.Entry{Level: "error", Msg: "draining escrow failed to close, its storage stays held", Fields: []any{
				"escrow", "7", "error", closeFailure,
			}},
		},
		{
			name:    "a settlement would not verify",
			produce: func(events *Journal) { events.SettlementUnverifiable("5", 9, unverifiable) },
			want: logcapture.Entry{Level: "warn", Msg: "settlement signatures did not verify", Fields: []any{
				"escrow", "5", "nonce", uint64(9), "error", unverifiable,
			}},
		},
		{
			name:    "boot finds a devshard it cannot serve",
			produce: func(events *Journal) { events.EscrowUnservable("gone", missing) },
			want: logcapture.Entry{Level: "warn", Msg: "devshard cannot be served, marking inactive", Fields: []any{
				"escrow", "gone", "error", missing,
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
