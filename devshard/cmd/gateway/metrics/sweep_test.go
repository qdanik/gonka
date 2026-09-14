package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSweepCountsWhatEachTickAppliedAndFailed(t *testing.T) {
	recorder := newTestRaceRecorder(New())

	recorder.RecordSweep(5, 3, 2)
	recorder.RecordSweep(1, 1, 0)

	require.Equal(t, 2, testutil.CollectAndCount(recorder.sweeps), "only applied and failed are published")
	require.Equal(t, float64(4), testutil.ToFloat64(recorder.sweeps.WithLabelValues(sweepOutcomeApplied)))
	require.Equal(t, float64(2), testutil.ToFloat64(recorder.sweeps.WithLabelValues(sweepOutcomeFailed)))
}

func TestSweepWithNothingDueCountsNothing(t *testing.T) {
	recorder := newTestRaceRecorder(New())

	recorder.RecordSweep(0, 0, 0)

	require.Zero(t, testutil.CollectAndCount(recorder.sweeps))
}
