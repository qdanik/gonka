package journal

import (
	"testing"
	"time"

	"devshard/types"
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

// DiffComposed runs under the session lock for every diff an escrow composes that carries a ledger fact.
func BenchmarkDiffComposed(b *testing.B) {
	events := New(Settings{Lines: discardLines{}})
	b.Cleanup(func() { _ = events.Close() })
	diff := composedDiff()

	b.ReportAllocs()
	for b.Loop() {
		events.DiffComposed("escrow-1", diff)
	}
}

// Almost every composed diff carries no ledger fact; that one must cost the session lock no allocation.
func BenchmarkDiffComposedWithoutFacts(b *testing.B) {
	events := New(Settings{Lines: discardLines{}})
	b.Cleanup(func() { _ = events.Close() })
	diff := &types.Diff{Nonce: 9, Txs: []*types.DevshardTx{{Tx: &types.DevshardTx_StartInference{}}}}

	b.ReportAllocs()
	for b.Loop() {
		events.DiffComposed("escrow-1", diff)
	}
}
