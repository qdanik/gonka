package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/gatewayprotocol/streamevidence"
	"devshard/host"
	"devshard/internal/testutil"
	"devshard/types"
)

const errorStreamFixtures = "../../gatewayprotocol/streamevidence/testdata"

// Test flow:
//  1. For each error stream fixture in `gatewayprotocol/streamevidence/testdata`, ask `streamevidence.ProvesErrorMiss` whether the payload of its evidence lines proves the error.
//  2. Write the fixture through a fresh `raceWriter` once as one read per SSE event and once as two reads split inside its error event, then flush the final event.
//  3. End the send and give the attempt a Finish in its response mempool.
//  4. Assert `errorMissClaimable` claims the miss exactly when the shared rule proves it.
func TestErrorMissClaimableFollowsTheSharedEvidenceRule(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(errorStreamFixtures, "*.sse"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	for _, path := range paths {
		stream, err := os.ReadFile(path)
		require.NoError(t, err)
		payload, err := streamevidence.Payload(streamevidence.DataLines(stream))
		require.NoError(t, err)
		wantClaim := streamevidence.ProvesErrorMiss(payload)

		for _, delivery := range []struct {
			name  string
			reads [][]byte
		}{
			{name: "one_read_per_event", reads: readsOnePerEvent(stream)},
			{name: "error_event_split_across_reads", reads: readsSplitInsideTheErrorEvent(t, stream)},
		} {
			t.Run(filepath.Base(path)+"/"+delivery.name, func(t *testing.T) {
				ctx := context.Background()
				var sink bytes.Buffer
				const nonce = 7
				rw, inf := newErrorRaceWriter(t, newRaceGroup(ctx, ctx, "escrow-x", &sink), nonce)
				for _, read := range delivery.reads {
					_, err := rw.Write(read)
					require.NoError(t, err)
				}
				rw.flushClassify()
				close(inf.done)
				inf.resp = &host.HostResponse{Mempool: []*types.DevshardTx{
					{Tx: &types.DevshardTx_FinishInference{FinishInference: &types.MsgFinishInference{ServedHash: testutil.TestServedHash, InferenceId: nonce}}},
				}}

				require.Equal(t, wantClaim, errorMissClaimable(inf, nil))
			})
		}
	}
}

// Test flow:
//  1. Build a finished attempt that saw a terminal error and holds a Finish in its response mempool but retained no evidence lines.
//  2. Assert `errorMissRunnable` still reports it runnable and `errorMissClaimable` does not claim it, since its payload is empty.
func TestErrorMissClaimableRefusesAFinishWithoutEvidence(t *testing.T) {
	const nonce = 7
	inf := &inflight{
		nonce:         nonce,
		errorSource:   "error.InternalServerError",
		errorTerminal: true,
		resp: &host.HostResponse{Mempool: []*types.DevshardTx{
			{Tx: &types.DevshardTx_FinishInference{FinishInference: &types.MsgFinishInference{ServedHash: testutil.TestServedHash, InferenceId: nonce}}},
		}},
	}

	require.True(t, errorMissRunnable(inf, nil))
	require.False(t, errorMissClaimable(inf, nil))
}

func readsSplitInsideTheErrorEvent(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	split := bytes.Index(stream, []byte(`"error"`))
	require.Positive(t, split, "every error stream fixture names an error")
	return [][]byte{stream[:split], stream[split:]}
}

func readsOnePerEvent(stream []byte) [][]byte {
	var reads [][]byte
	for _, event := range bytes.SplitAfter(stream, []byte("\n\n")) {
		if len(event) > 0 {
			reads = append(reads, event)
		}
	}
	return reads
}
