package journal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/internal/logcapture"
)

// Each want pins one chain transition's level, message and every key with its type.
func TestChainTransitionsRenderTheLinesTheirProducersWrote(t *testing.T) {
	testCases := []struct {
		name    string
		produce func(events *Journal)
		want    logcapture.Entry
	}{
		{
			name:    "a new epoch or phase",
			produce: func(events *Journal) { events.ChainEpoch(7, chain.EpochPhaseInference, 100, 150) },
			want: logcapture.Entry{Level: "info", Msg: "chain epoch", Fields: []any{
				"epoch", uint64(7), "phase", chain.EpochPhaseInference, "height", int64(100), "switch_height", int64(150),
			}},
		},
		{
			name:    "requests become blocked",
			produce: func(events *Journal) { events.ChainRequestsBlocked(chain.BlockReasonPoC, 7, 100) },
			want: logcapture.Entry{Level: "warn", Msg: "chain blocked requests", Fields: []any{
				"reason", chain.BlockReasonPoC, "epoch", uint64(7), "height", int64(100),
			}},
		},
		{
			name:    "a block clears",
			produce: func(events *Journal) { events.ChainRequestsUnblocked(7, 120) },
			want: logcapture.Entry{Level: "info", Msg: "chain unblocked requests", Fields: []any{
				"epoch", uint64(7), "height", int64(120),
			}},
		},
		{
			name:    "the snapshot goes stale",
			produce: func(events *Journal) { events.ChainSnapshotStale("fetch epoch info: 503", 0, 0) },
			want: logcapture.Entry{Level: "warn", Msg: "chain snapshot stale", Fields: []any{
				"error", "fetch epoch info: 503", "epoch", uint64(0), "height", int64(0),
			}},
		},
		{
			name:    "the snapshot recovers",
			produce: func(events *Journal) { events.ChainSnapshotRecovered(7, 100) },
			want: logcapture.Entry{Level: "info", Msg: "chain snapshot recovered", Fields: []any{
				"epoch", uint64(7), "height", int64(100),
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

// A first poll that failed publishes a snapshot with epoch 0, which announces no epoch: epoch 0 reads as a restarted chain.
func TestAnEpochlessSnapshotAnnouncesNoEpoch(t *testing.T) {
	lines := &logcapture.Recorder{}
	events := newJournal(t, Settings{Lines: lines})

	events.ChainEpoch(0, "", 0, 0)
	events.Flush()

	require.Empty(t, lines.All(), "a snapshot with epoch 0 writes no chain epoch line")
}
