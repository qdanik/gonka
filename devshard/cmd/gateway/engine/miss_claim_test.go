package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/gatewayprotocol/streamevidence"
)

const errorStreamFixtures = "../../../gatewayprotocol/streamevidence/testdata"

// Test flow:
//  1. For each error stream fixture in `gatewayprotocol/streamevidence/testdata`, ask `streamevidence.ProvesErrorMiss` whether the payload of its evidence lines proves the error.
//  2. Run an attempt whose host streams the fixture through the real `sseClassifier`, once as one read per SSE event and once as two reads split inside its error event.
//  3. Plan the race's timeout steps from the attempt's outcome.
//  4. Assert the one step settles by a miss claim exactly when the shared rule proves it.
func TestAnErrorStreamIsClaimedExactlyWhenTheSharedEvidenceRuleProvesIt(t *testing.T) {
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
			reads []string
		}{
			{name: "one_read_per_event", reads: readsOnePerEvent(stream)},
			{name: "error_event_split_across_reads", reads: readsSplitInsideTheErrorEvent(t, stream)},
		} {
			t.Run(filepath.Base(path)+"/"+delivery.name, func(t *testing.T) {
				dispatch := &fakeDispatcher{receipt: true, chunks: delivery.reads, response: fakeResponse{confirmed: true}}
				fixture := newAttemptFixture(dispatch, &fakeClassifier{})
				fixture.spec.Classifier = newTestClassifier(false, nil)

				runAttempt(context.Background(), fixture.spec)
				outcome := *doneEvent(t, fixture.drain()).Outcome
				steps := RaceOutcome{Attempts: []AttemptOutcome{outcome}}.TimeoutPlan()

				require.Len(t, steps, 1)
				require.Equal(t, wantClaim, steps[0].Kind == SettleByMissClaim)
			})
		}
	}
}

func readsSplitInsideTheErrorEvent(t *testing.T, stream []byte) []string {
	t.Helper()
	split := bytes.Index(stream, []byte(`"error"`))
	require.Positive(t, split, "every error stream fixture names an error")
	return []string{string(stream[:split]), string(stream[split:])}
}

func readsOnePerEvent(stream []byte) []string {
	var reads []string
	for _, event := range bytes.SplitAfter(stream, []byte("\n\n")) {
		if len(event) > 0 {
			reads = append(reads, string(event))
		}
	}
	return reads
}
