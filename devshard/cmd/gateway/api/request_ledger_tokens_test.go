package api

import (
	"testing"

	"devshard/cmd/gateway/engine"
)

// The row carries two numbers about one prompt: the char/4 estimate the gateway sized its reserve and its
// congestion windows on, and the count the host reported. Serving only the estimate beside a measured output
// put two different kinds of number under names that read as a pair.
func TestTheRequestRowCarriesTheEstimateAndTheMeasuredPromptApart(t *testing.T) {
	t.Parallel()
	winner := engine.AttemptOutcome{
		Participant:           "participant-1",
		Nonce:                 7,
		Terminal:              engine.TerminalWon,
		UsagePromptTokens:     27_438,
		UsageCompletionTokens: 4_096,
	}
	outcome := engine.RaceOutcome{
		RequestID:   "req-1",
		Model:       "qwen",
		InputTokens: 50_791,
		Succeeded:   true,
		WinnerNonce: 7,
		Attempts:    []engine.AttemptOutcome{winner},
	}

	record := requestRecord(outcome)

	if record.InputTokens != 50_791 {
		t.Fatalf("input_tokens = %d, want the gateway's own estimate", record.InputTokens)
	}
	if record.PromptTokens != 27_438 {
		t.Fatalf("prompt_tokens = %d, want the count the host reported", record.PromptTokens)
	}
	if record.WinnerOutputTokens != 4_096 {
		t.Fatalf("winner_output_tokens = %d, want the count the host reported", record.WinnerOutputTokens)
	}
}

func TestAHostThatReportedNoUsageStillHasItsOutputCounted(t *testing.T) {
	t.Parallel()
	outcome := engine.RaceOutcome{
		RequestID:   "req-1",
		Model:       "qwen",
		Succeeded:   true,
		WinnerNonce: 7,
		Attempts: []engine.AttemptOutcome{{
			Participant:   "participant-1",
			Nonce:         7,
			Terminal:      engine.TerminalWon,
			LogprobTokens: 4_096,
		}},
	}

	record := requestRecord(outcome)

	if record.WinnerOutputTokens != 4_096 {
		t.Fatalf("winner_output_tokens = %d, want the tokens the gateway counted off the stream", record.WinnerOutputTokens)
	}
	if record.TotalOutputTokens != 4_096 {
		t.Fatalf("total_output_tokens = %d, want the tokens the gateway counted off the stream", record.TotalOutputTokens)
	}
}

// A host that reports no usage leaves the measured half at zero rather than borrowing the estimate, or the
// estimate's own error becomes invisible in exactly the rows that would have shown it.
func TestAnUnreportedPromptIsNotFilledInFromTheEstimate(t *testing.T) {
	t.Parallel()
	outcome := engine.RaceOutcome{
		RequestID:   "req-1",
		Model:       "qwen",
		InputTokens: 50_791,
		Succeeded:   true,
		WinnerNonce: 7,
		Attempts:    []engine.AttemptOutcome{{Participant: "participant-1", Nonce: 7, Terminal: engine.TerminalWon}},
	}

	record := requestRecord(outcome)

	if record.PromptTokens != 0 {
		t.Fatalf("prompt_tokens = %d, want 0 when the host reported none", record.PromptTokens)
	}
	if record.InputTokens != 50_791 {
		t.Fatalf("input_tokens = %d, want the estimate to stand on its own", record.InputTokens)
	}
}
