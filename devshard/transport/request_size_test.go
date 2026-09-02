package transport

import (
	"testing"

	"devshard/types"

	"github.com/stretchr/testify/require"
)

func TestPromptWireBytesCountsTheBase64ItTravelsAs(t *testing.T) {
	const raw = 3 << 20

	wire := PromptWireBytes(raw)

	require.Equal(t, raw/3*4+envelopeBytes, wire,
		"a prompt is a byte slice on this wire, so it costs four bytes for every three")
}

// A prompt inside the client's own cap can still be past the host's once encoded, which is the whole
// defect: 8 MiB of JSON leaves here as more than 10 MiB.
func TestAPromptUnderTheCapCanStillBeTooLargeEncoded(t *testing.T) {
	require.True(t, InferenceRequestFits(6<<20, nil))
	require.False(t, InferenceRequestFits(8<<20, nil))
}

func TestCatchUpIsCountedAgainstTheSameBody(t *testing.T) {
	diffs := make([]types.Diff, 96)
	for index := range diffs {
		diffs[index] = types.Diff{UserSig: make([]byte, 100<<10), PostStateRoot: make([]byte, 32)}
	}

	require.True(t, InferenceRequestFits(1<<20, nil))
	require.False(t, InferenceRequestFits(1<<20, diffs),
		"a backlog the prompt leaves no room for is what turns a servable request into a 413")
}

func TestTheWireWalkStopsAtTheLimit(t *testing.T) {
	diffs := make([]types.Diff, 1_000)
	for index := range diffs {
		diffs[index] = types.Diff{UserSig: make([]byte, 1<<20)}
	}

	require.True(t, DiffsExceedWireBytes(diffs, 1<<20),
		"the walk answers from the first diffs rather than reading a history of any length")
	require.False(t, DiffsExceedWireBytes(nil, 1))
}

func TestChunkingStopsWhereTheBodyWouldNotFit(t *testing.T) {
	diffs := make([]types.Diff, 20)
	for index := range diffs {
		diffs[index] = types.Diff{UserSig: make([]byte, 1<<20)}
	}

	require.Equal(t, 7, DiffsWithinWireBytes(diffs, 10<<20),
		"seven diffs of a megabyte are what fits ten once each is base64")
	require.Equal(t, 20, DiffsWithinWireBytes(diffs, 100<<20))
}

// One diff past the limit still has to move, or the walk that carries it never advances again.
func TestASingleOversizedDiffIsStillTaken(t *testing.T) {
	require.Equal(t, 1, DiffsWithinWireBytes([]types.Diff{{UserSig: make([]byte, 20<<20)}}, 1<<20))
	require.Zero(t, DiffsWithinWireBytes(nil, 1<<20))
}
