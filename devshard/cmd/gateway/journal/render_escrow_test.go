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

// Each want is the line the escrow manager or the transaction client wrote, with the escrow id as text where it was a number.
func TestEscrowLifecycleTransitionsRenderTheLinesTheirProducersWrote(t *testing.T) {
	tickFailure := errors.New("store unavailable")

	testCases := []struct {
		name    string
		produce func(events *Journal)
		want    logcapture.Entry
	}{
		{
			name:    "an escrow is created",
			produce: func(events *Journal) { events.EscrowCreated("42", "model-a", "temp", 7, "TX-HAPPY") },
			want: logcapture.Entry{Level: "info", Msg: "escrow created", Fields: []any{
				"escrow", "42", "model", "model-a", "role", "temp", "epoch", uint64(7), "tx", "TX-HAPPY",
			}},
		},
		{
			name:    "an escrow is recovered from its commitment",
			produce: func(events *Journal) { events.EscrowRecovered("55", "model-a", "temp", 3, "TXA") },
			want: logcapture.Entry{Level: "info", Msg: "escrow recovered from commitment", Fields: []any{
				"escrow", "55", "model", "model-a", "role", "temp", "epoch", uint64(3), "tx", "TXA",
			}},
		},
		{
			name: "a commitment is cleared",
			produce: func(events *Journal) {
				events.CommitmentCleared("TXB", "model-a", "temp", 3, "transaction created no escrow")
			},
			want: logcapture.Entry{Level: "warn", Msg: "commitment cleared", Fields: []any{
				"tx", "TXB", "model", "model-a", "role", "temp", "epoch", uint64(3), "reason", "transaction created no escrow",
			}},
		},
		{
			name:    "an escrow is gone from the chain",
			produce: func(events *Journal) { events.EscrowGoneFromChain("1") },
			want:    logcapture.Entry{Level: "warn", Msg: "escrow gone from chain, taken out of service", Fields: []any{"escrow", "1"}},
		},
		{
			name:    "an escrow is marked for replacement",
			produce: func(events *Journal) { events.EscrowMarkedForReplacement("1", "nonce_cap") },
			want: logcapture.Entry{Level: "warn", Msg: "escrow marked for replacement", Fields: []any{
				"escrow", "1", "reason", "nonce_cap",
			}},
		},
		{
			name:    "a depleted escrow has no replacement",
			produce: func(events *Journal) { events.EscrowDepletedWithoutReplacement("1", "model-a") },
			want: logcapture.Entry{Level: "warn", Msg: "escrow depleted with no replacement configured", Fields: []any{
				"escrow", "1", "model", "model-a",
			}},
		},
		{
			name:    "a rotation is skipped",
			produce: func(events *Journal) { events.RotationSkipped("qwen", "regular", 4) },
			want: logcapture.Entry{Level: "warn", Msg: "rotation skipped, the network serves no such model", Fields: []any{
				"model", "qwen", "role", "regular", "epoch", uint64(4),
			}},
		},
		{
			name:    "regulars are promoted to temp",
			produce: func(events *Journal) { events.RegularsPromotedToTemp("model-a", 9, 2) },
			want: logcapture.Entry{Level: "warn", Msg: "regular escrows promoted to temp", Fields: []any{
				"model", "model-a", "epoch", uint64(9), "promoted", 2,
			}},
		},
		{
			name:    "a bridge is prepared",
			produce: func(events *Journal) { events.BridgePrepared("model-a", 9, 1, 2) },
			want: logcapture.Entry{Level: "info", Msg: "bridge prepared", Fields: []any{
				"model", "model-a", "epoch", uint64(9), "created", 1, "retired", 2,
			}},
		},
		{
			name:    "a bridge is finished",
			produce: func(events *Journal) { events.BridgeFinished("model-a", 9, 2, 1) },
			want: logcapture.Entry{Level: "info", Msg: "bridge finished", Fields: []any{
				"model", "model-a", "epoch", uint64(9), "created", 2, "retired", 1,
			}},
		},
		{
			name:    "an escrow is parked",
			produce: func(events *Journal) { events.EscrowParked("5") },
			want:    logcapture.Entry{Level: "info", Msg: "escrow parked for settlement", Fields: []any{"escrow", "5"}},
		},
		{
			name:    "an escrow is settled",
			produce: func(events *Journal) { events.EscrowSettled("5", "model-a", "SETTLE-TX", "gonka1settler") },
			want: logcapture.Entry{Level: "info", Msg: "escrow settled", Fields: []any{
				"escrow", "5", "model", "model-a", "tx", "SETTLE-TX", "settler", "gonka1settler",
			}},
		},
		{
			name:    "a settle already on chain is reconciled",
			produce: func(events *Journal) { events.SettlementReconciled("7", "SETTLE-TX") },
			want: logcapture.Entry{Level: "info", Msg: "settle already on chain, reconciled", Fields: []any{
				"escrow", "7", "tx", "SETTLE-TX",
			}},
		},
		{
			name:    "a settled record is dropped",
			produce: func(events *Journal) { events.SettledRecordDropped("5") },
			want:    logcapture.Entry{Level: "info", Msg: "settled escrow record dropped", Fields: []any{"escrow", "5"}},
		},
		{
			name:    "a tick fails",
			produce: func(events *Journal) { events.EscrowTickFailed(tickFailure) },
			want:    logcapture.Entry{Level: "error", Msg: "escrow tick failed", Fields: []any{"error", tickFailure}},
		},
		{
			name:    "a sweep found votes owed",
			produce: func(events *Journal) { events.TimeoutsSwept(4, 3, 1) },
			want: logcapture.Entry{Level: "info", Msg: "execution timeouts swept", Fields: []any{
				"swept_due", 4, "swept_applied", 3, "swept_failed", 1,
			}},
		},
		{
			name:    "a settle transaction is broadcast",
			produce: func(events *Journal) { events.SettleBroadcast("123", "ABCDEF", "gonka1settler") },
			want: logcapture.Entry{Level: "info", Msg: "settle tx broadcast", Fields: []any{
				"escrow", "123", "tx", "ABCDEF", "settler", "gonka1settler",
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
