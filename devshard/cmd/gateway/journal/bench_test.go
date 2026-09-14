package journal

import (
	"testing"
	"time"
)

// The sink keeps what a benchmark built from being optimised away.
var sinkFields []any

// discardLines keeps nothing, so a benchmark measures the journal rather than a log handler.
type discardLines struct{}

func (discardLines) Info(string, ...any)  {}
func (discardLines) Warn(string, ...any)  {}
func (discardLines) Error(string, ...any) {}

// The finish line every completed attempt writes, built the way the consumer builds it.
func BenchmarkAttemptFinishFields(b *testing.B) {
	step := widestFinishedStep()

	b.ReportAllocs()
	for b.Loop() {
		sinkFields = appendAttemptDeliveryFields(attemptFinishHead(&step), &step.Outcome)
	}
}

// RecordStep runs on the coordinator for every step it traces; the reported allocations include the consumer's rendering.
func BenchmarkRecordStep(b *testing.B) {
	events := New(Settings{Lines: discardLines{}})
	b.Cleanup(func() { _ = events.Close() })
	step := widestFinishedStep()

	b.ReportAllocs()
	for b.Loop() {
		events.RecordStep(step)
	}
}

// Every finished request builds its record once, whatever it did.
func BenchmarkRequestFinishedFields(b *testing.B) {
	line := RequestLine{
		RequestID: "request-1", Model: "qwen", ClientStream: true, Outcome: servedOutcome(),
		Verdict: "served", Elapsed: 3 * time.Second,
	}

	b.ReportAllocs()
	for b.Loop() {
		sinkFields = requestFinishedFields(&line)
	}
}
