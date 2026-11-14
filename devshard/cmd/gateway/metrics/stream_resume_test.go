package metrics

import (
	"strings"
	"testing"
)

type fakeStreams struct{ orphans, bytes int64 }

func (f fakeStreams) StreamStats() (orphans, bytes int64) { return f.orphans, f.bytes }

// Test flow:
//  1. Register a `StreamResumeRecorder` and a `StreamCollector` over fixed stream stats.
//  2. Count one orphan outcome and one resume.
//  3. Scrape and assert the two counters carry their model and result labels and the gauges carry the stats.
func TestStreamResumeMetricsCarryTheirLabelsAndTheOrphanGauges(t *testing.T) {
	gatewayMetrics := New()
	recorder := NewStreamResumeRecorder(gatewayMetrics)
	gatewayMetrics.Register(NewStreamCollector(fakeStreams{orphans: 3, bytes: 4096}))

	recorder.OrphanOutcome("qwen", "stored")
	recorder.Resume("qwen", "live")

	exposition := scrape(t, gatewayMetrics)
	for _, want := range []string{
		`devshard_gateway_stream_orphan_outcomes_total{model="qwen",result="stored"} 1`,
		`devshard_gateway_stream_resume_total{model="qwen",result="live"} 1`,
		`devshard_gateway_stream_orphans 3`,
		`devshard_gateway_stream_orphan_bytes 4096`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition missing %q", want)
		}
	}
}

// Test flow:
//  1. Call both counters on a nil recorder.
//  2. Assert neither panics.
func TestANilStreamResumeRecorderCountsNothing(t *testing.T) {
	var recorder *StreamResumeRecorder

	recorder.OrphanOutcome("qwen", "stored")
	recorder.Resume("qwen", "live")
}
