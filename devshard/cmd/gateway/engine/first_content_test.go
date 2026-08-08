package engine

import (
	"testing"
	"time"
)

func classifiedState() (*attemptState, *sseClassifier) {
	return &attemptState{}, newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), "participant", "model", nil)
}

// A role-only chunk is a token, not content: it is what the two stamps exist to tell apart.
func TestFirstContentIsNotTheFirstChunk(t *testing.T) {
	t.Parallel()
	state, classifier := classifiedState()
	role := []byte(`data: {"choices":[{"delta":{"role":"assistant","content":""}}]}` + "\n\n")
	content := []byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n")
	at := time.Unix(1786114580, 0)

	state.firstToken = at
	state.record(classifier.Classify(role))
	if classifier.Classify(role).Content {
		t.Fatal("a role chunk with an empty content is not content")
	}

	facts := classifier.Classify(content)
	if !facts.Content {
		t.Fatal("a chunk carrying content is content")
	}
	state.firstContent = at.Add(120 * time.Millisecond)

	if state.firstContent.Equal(state.firstToken) {
		t.Error("the two stamps must part when a role chunk arrives first")
	}
}

// Reasoning and a tool call are content too: a host that opens with either has already started work.
func TestFirstContentCountsReasoningAndToolCalls(t *testing.T) {
	t.Parallel()
	for _, chunk := range []string{
		`data: {"choices":[{"delta":{"reasoning":"thinking"}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"reasoning_content":"thinking"}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}` + "\n\n",
	} {
		_, classifier := classifiedState()
		if !classifier.Classify([]byte(chunk)).Content {
			t.Errorf("not counted as content: %s", chunk)
		}
	}
}

// An empty tool_calls array is the shape a host sends before it has decided anything.
func TestFirstContentIgnoresAnEmptyOpening(t *testing.T) {
	t.Parallel()
	for _, chunk := range []string{
		`data: {"choices":[{"delta":{"tool_calls":[]}}]}` + "\n\n",
		`data: {"choices":[{"delta":{}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n",
	} {
		_, classifier := classifiedState()
		if classifier.Classify([]byte(chunk)).Content {
			t.Errorf("counted as content but carries none: %s", chunk)
		}
	}
}
