package engine

import (
	"testing"
)

// A host reports both halves of its usage, and the gateway kept only one. The other is what tells an operator
// whether the char/4 estimate the windows and the escrow reserve are sized on is anywhere near the truth.
func TestBothHalvesOfAHostsUsageAreRead(t *testing.T) {
	t.Parallel()
	classifier := newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), testParticipant, testModel, nil)

	classifier.Classify([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n"))
	facts := classifier.Classify([]byte(`data: {"choices":[],"usage":{"prompt_tokens":27438,"completion_tokens":4096}}` + "\n\n"))

	if facts.UsagePromptTokens != 27_438 {
		t.Fatalf("UsagePromptTokens = %d, want the prompt the host counted", facts.UsagePromptTokens)
	}
	if facts.UsageCompletionTokens != 4_096 {
		t.Fatalf("UsageCompletionTokens = %d, want the completion the host counted", facts.UsageCompletionTokens)
	}
}

// Both halves latch the same way over an attempt, so a recorded row can never pair one usage object's prompt
// with another's completion.
func TestBothHalvesOfTheUsageLatchTogether(t *testing.T) {
	t.Parallel()
	attempt := &attemptState{}

	attempt.record(chunkFacts{UsagePromptTokens: 10, UsageCompletionTokens: 20})
	attempt.record(chunkFacts{})
	attempt.record(chunkFacts{UsagePromptTokens: 27_438, UsageCompletionTokens: 4_096})

	if attempt.usagePromptTokens != 27_438 || attempt.usageCompletionTokens != 4_096 {
		t.Fatalf("usage = %d/%d, want both halves from the same object",
			attempt.usagePromptTokens, attempt.usageCompletionTokens)
	}
}

// A host whose runtime reports running usage puts one on every event, counting up. One read carries many of
// them, so the last is the answer: the first says what had been produced when the answer had barely started.
func TestRunningUsageIsReadAtItsLastValueWithinOneRead(t *testing.T) {
	t.Parallel()
	classifier := newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), testParticipant, testModel, nil)

	facts := classifier.Classify([]byte(
		`data: {"choices":[{"delta":{"content":"a"}}],"usage":{"prompt_tokens":27438,"completion_tokens":1}}` + "\n\n" +
			`data: {"choices":[{"delta":{"content":"b"}}],"usage":{"prompt_tokens":27438,"completion_tokens":2}}` + "\n\n" +
			`data: {"choices":[],"usage":{"prompt_tokens":27438,"completion_tokens":4096}}` + "\n\n"))

	if facts.UsageCompletionTokens != 4_096 {
		t.Fatalf("UsageCompletionTokens = %d, want 4096 -- the count the stream ended on, not the one it opened with",
			facts.UsageCompletionTokens)
	}
	if facts.UsagePromptTokens != 27_438 {
		t.Fatalf("UsagePromptTokens = %d, want the prompt the host counted", facts.UsagePromptTokens)
	}
}
