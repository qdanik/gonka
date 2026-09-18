package journal

import (
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
)

var renderEpoch = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

func loggedValue(fields []any, name string) (any, bool) {
	for index := 0; index+1 < len(fields); index += 2 {
		if fields[index] == name {
			return fields[index+1], true
		}
	}
	return nil, false
}

// A loser is what the winner has to be compared against, so its line has to carry the same facts.
func TestAttemptDeliveryFields_ReportWhatTheHostReturned(t *testing.T) {
	t.Parallel()
	dispatchedAt := time.Unix(1786114580, 0)

	fields := appendAttemptDeliveryFields(nil, &engine.AttemptOutcome{
		SendTime:              dispatchedAt,
		ReceiptTime:           dispatchedAt.Add(120 * time.Millisecond),
		FirstToken:            dispatchedAt.Add(2 * time.Second),
		Completed:             dispatchedAt.Add(9 * time.Second),
		ContentChunks:         41,
		StreamChunks:          43,
		UsageCompletionTokens: 64,
	})

	for _, expected := range []struct {
		name  string
		value any
	}{
		{"content_chunks", int64(41)},
		{"stream_chunks", int64(43)},
		{"usage_tokens", int64(64)},
		{"receipt_ms", int64(120)},
		{"first_token_ms", int64(2000)},
		{"attempt_ms", int64(9000)},
	} {
		got, held := loggedValue(fields, expected.name)
		if !held {
			t.Errorf("%s missing", expected.name)
			continue
		}
		if got != expected.value {
			t.Errorf("%s = %v, want %v", expected.name, got, expected.value)
		}
	}
}

// A host that never sent a receipt has no receipt duration; reporting one would date it to the epoch.
func TestAttemptDeliveryFields_SkipAStageThatNeverHappened(t *testing.T) {
	t.Parallel()
	dispatchedAt := time.Unix(1786114580, 0)

	fields := appendAttemptDeliveryFields(nil, &engine.AttemptOutcome{
		SendTime:  dispatchedAt,
		Completed: dispatchedAt.Add(3 * time.Second),
	})

	for _, absent := range []string{"receipt_ms", "first_token_ms"} {
		if _, held := loggedValue(fields, absent); held {
			t.Errorf("%s reported for a stage that never happened", absent)
		}
	}
	if got, _ := loggedValue(fields, "attempt_ms"); got != int64(3000) {
		t.Errorf("attempt_ms = %v, want 3000", got)
	}
}

// An attempt that never left the gateway has no dispatch to measure from.
func TestAttemptDeliveryFields_SkipEveryDurationWithoutADispatch(t *testing.T) {
	t.Parallel()

	fields := appendAttemptDeliveryFields(nil, &engine.AttemptOutcome{Completed: time.Unix(1786114580, 0), StreamChunks: 2})

	for _, absent := range []string{"receipt_ms", "first_token_ms", "attempt_ms"} {
		if _, held := loggedValue(fields, absent); held {
			t.Errorf("%s reported without a dispatch to measure from", absent)
		}
	}
	if got, _ := loggedValue(fields, "stream_chunks"); got != int64(2) {
		t.Errorf("stream_chunks = %v, want 2", got)
	}
}

// widestFinishedStep carries every field a finish line can report, so the line it builds is the widest one.
func widestFinishedStep() engine.RaceStep {
	return engine.RaceStep{
		Kind: engine.RaceStepAttemptFinished, RequestID: "request-1", EscrowID: "escrow-1", Nonce: 77,
		Participant: "host-3", Terminal: engine.TerminalLost, NonceFinished: true, HasOutcome: true, PhaseAborted: true,
		Outcome: engine.AttemptOutcome{
			SendTime:     renderEpoch,
			ReceiptTime:  renderEpoch.Add(200 * time.Millisecond),
			FirstToken:   renderEpoch.Add(900 * time.Millisecond),
			FirstContent: renderEpoch.Add(950 * time.Millisecond),
			Completed:    renderEpoch.Add(9 * time.Second),

			ContentChunks: 412, StreamChunks: 415, OutputBytes: 61_440,
			UsageCompletionTokens: 512,
			MaxChunkGap:           1200 * time.Millisecond, MaxChunkGapAt: 310,
			MeanChunkGap:   21 * time.Millisecond,
			UpstreamStatus: 502, UpstreamBody: "upstream is down",
			LastChunkHead: `data: {"choices":[{"delta":{}}]}`,

			ReceiptDeadlineMissed: true, FirstTokenDeadlineMissed: true,
		},
	}
}

// The reservation is hand-counted, so the widest line the code can build must be measured against it.
func TestAFinishLineFitsWhatItReserves(t *testing.T) {
	t.Parallel()
	step := widestFinishedStep()

	fields := appendAttemptMarks(appendAttemptDeliveryFields(attemptFinishHead(&step), &step.Outcome), &step)

	if len(fields) != attemptFinishFields {
		t.Fatalf("the widest finish line is %d fields, but %d are reserved", len(fields), attemptFinishFields)
	}
}

// A slow host is found in the log by the line of the attempt it was late on.
func TestAFinishLineNamesOnlyTheDeadlinesItsHostMissed(t *testing.T) {
	t.Parallel()
	step := engine.RaceStep{HasOutcome: true, Outcome: engine.AttemptOutcome{FirstTokenDeadlineMissed: true}}

	fields := appendAttemptMarks(nil, &step)

	if got, _ := loggedValue(fields, "missed_first_token_deadline"); got != true {
		t.Errorf("missed_first_token_deadline = %v, want true", got)
	}
	if _, held := loggedValue(fields, "missed_receipt_deadline"); held {
		t.Error("missed_receipt_deadline reported for a deadline the host met")
	}
}
