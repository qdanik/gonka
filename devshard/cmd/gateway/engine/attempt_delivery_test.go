package engine

import (
	"testing"
	"time"
)

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

	fields := attemptDeliveryFields(AttemptOutcome{
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

	fields := attemptDeliveryFields(AttemptOutcome{
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

	fields := attemptDeliveryFields(AttemptOutcome{Completed: time.Unix(1786114580, 0), StreamChunks: 2})

	for _, absent := range []string{"receipt_ms", "first_token_ms", "attempt_ms"} {
		if _, held := loggedValue(fields, absent); held {
			t.Errorf("%s reported without a dispatch to measure from", absent)
		}
	}
	if got, _ := loggedValue(fields, "stream_chunks"); got != int64(2) {
		t.Errorf("stream_chunks = %v, want 2", got)
	}
}
