package engine

import (
	"testing"
	"time"
)

func benchAttempts(count int) []EscalationAttempt {
	start := time.Unix(1786114580, 0)
	attempts := make([]EscalationAttempt, 0, count)
	for i := range count {
		attempts = append(attempts, EscalationAttempt{
			SendTime:        start,
			ReceiptTime:     start.Add(200 * time.Millisecond),
			FirstContentP75: time.Duration(i+1) * time.Second,
		})
	}
	return attempts
}

// nextDeadline runs on every event the coordinator takes, chunks included.
func BenchmarkNextDeadline(b *testing.B) {
	policy := EscalationPolicy{
		ReceiptTimeout: 10 * time.Second, FirstTokenFloor: time.Second,
		FirstTokenCeiling: 60 * time.Second, InterChunkStall: time.Minute,
		LoserGrace: 5 * time.Second, MaxAttemptsPerRequest: 4,
	}
	plan := deadlinePlan{
		Policy:   policy,
		Request:  EscalationRequest{InputTokens: 4096},
		Attempts: benchAttempts(4),
	}
	now := time.Unix(1786114582, 0)

	b.ReportAllocs()
	for b.Loop() {
		nextDeadline(now, plan)
	}
}

func BenchmarkFirstTokenBudget(b *testing.B) {
	policy := EscalationPolicy{FirstTokenFloor: time.Second, FirstTokenCeiling: 60 * time.Second}

	b.ReportAllocs()
	for b.Loop() {
		policy.firstTokenBudget(4096, 5*time.Second)
	}
}

// recordChunkGap runs once per streamed chunk, beside the classifier.
func BenchmarkRecordChunkGap(b *testing.B) {
	content := []byte(`data: {"choices":[{"index":0,"delta":{"content":"hello there"}}]}` + "\n\n")
	state := &attemptState{}
	at := time.Unix(1786114580, 0)

	b.ReportAllocs()
	for b.Loop() {
		at = at.Add(40 * time.Millisecond)
		state.recordChunkGap(at, content)
	}
}

func BenchmarkRecordChunkGapAtDone(b *testing.B) {
	done := []byte("data: [DONE]\n\n")
	state := &attemptState{}
	at := time.Unix(1786114580, 0)

	b.ReportAllocs()
	for b.Loop() {
		at = at.Add(40 * time.Millisecond)
		state.recordChunkGap(at, done)
	}
}

// A host that bursts reasoning sends chunks far larger than one delta; the terminator check walks them.
func BenchmarkRecordChunkGapLargeChunk(b *testing.B) {
	payload := make([]byte, 0, 8<<10)
	payload = append(payload, `data: {"choices":[{"index":0,"delta":{"reasoning":"`...)
	for len(payload) < 8<<10 {
		payload = append(payload, "reasoning text that a host streamed in one go "...)
	}
	payload = append(payload, `"}}]}`+"\n\n"...)
	state := &attemptState{}
	at := time.Unix(1786114580, 0)

	b.ReportAllocs()
	for b.Loop() {
		at = at.Add(40 * time.Millisecond)
		state.recordChunkGap(at, payload)
	}
}
