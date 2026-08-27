package filters

import (
	"encoding/json"
	"testing"

	"common/completionapi"
)

func normalizedDocument(t *testing.T, body, routedModel string) map[string]any {
	t.Helper()
	result, err := NormalizeRequest([]byte(body), Options{
		RoutedModel:      routedModel,
		DefaultMaxTokens: 3072,
		MaxTokensCap:     4096,
	})
	if err != nil {
		t.Fatalf("NormalizeRequest() = %v, want nil", err)
	}
	var document map[string]any
	if err := json.Unmarshal(result.Body, &document); err != nil {
		t.Fatalf("unmarshal normalized body: %v", err)
	}
	return document
}

func requireUintField(t *testing.T, document map[string]any, name string, want uint64) {
	t.Helper()
	raw, present := document[name]
	if !present {
		t.Fatalf("%s missing from %v", name, document)
	}
	number, ok := raw.(float64)
	if !ok || uint64(number) != want {
		t.Errorf("%s = %v, want %d", name, raw, want)
	}
}

func TestFloorInjectsMinTokensWhenTheClientOmitsIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096}`, "Qwen/Test")
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

func TestFloorLiftsAMinTokensBelowIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"min_tokens":4}`, "Qwen/Test")
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

func TestFloorKeepsAMinTokensAboveIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"min_tokens":100}`, "Qwen/Test")
	requireUintField(t, document, "min_tokens", 100)
}

// min_tokens has to fit inside the budget it is measured against, whatever the client asked for.
func TestFloorClampsMinTokensToTheResolvedBudget(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":300,"min_tokens":9000}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", 300)
	requireUintField(t, document, "min_tokens", 300)
}

func TestFloorLiftsASmallMaxTokensAndInjectsMinTokens(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

func TestFloorMirrorsMaxCompletionTokensWhenTheClientSentIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":8}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
	requireUintField(t, document, "max_completion_tokens", completionapi.MinTokensFloor)
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

// Every route, not only a profile that declares a floor of its own: the chain refuses a sub-floor
// reservation whatever model it was for.
func TestFloorAppliesToEveryRoute(t *testing.T) {
	for _, model := range []string{kimiModelID, minimaxModelID, "Qwen/Test"} {
		t.Run(model, func(t *testing.T) {
			document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`, model)
			requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
			requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
		})
	}
}

// The reservation the gateway signs is (input + max_tokens), so the body it sends has to carry the
// same max_tokens the accounting was told about.
func TestFloorLeavesTheBodyAgreeingWithTheDeclaredBudget(t *testing.T) {
	result, err := NormalizeRequest([]byte(`{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`),
		Options{RoutedModel: "Qwen/Test", DefaultMaxTokens: 3072, MaxTokensCap: 4096})
	if err != nil {
		t.Fatalf("NormalizeRequest() = %v, want nil", err)
	}
	var document map[string]any
	if err := json.Unmarshal(result.Body, &document); err != nil {
		t.Fatalf("unmarshal normalized body: %v", err)
	}
	if uint64(document["max_tokens"].(float64)) != result.MaxTokens {
		t.Errorf("body max_tokens = %v, declared %d", document["max_tokens"], result.MaxTokens)
	}
	if result.MaxTokens < completionapi.MinTokensFloor {
		t.Errorf("declared MaxTokens = %d, below the floor %d", result.MaxTokens, completionapi.MinTokensFloor)
	}
}

// stop_token_ids goes without being inspected: the floor puts min_tokens on every request, and vLLM
// masks stop-token logits when it is set, where an out-of-vocab id asserts device-side.
func TestStopTokenIdsAreStrippedWithoutBeingValidated(t *testing.T) {
	for _, body := range []string{
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"stop_token_ids":[1,2,3]}`,
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"stop_token_ids":["not-a-number"]}`,
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"stop_token_ids":[-5]}`,
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"stop_token_ids":"nonsense"}`,
	} {
		t.Run(body, func(t *testing.T) {
			document := normalizedDocument(t, body, "Qwen/Test")
			if _, present := document["stop_token_ids"]; present {
				t.Errorf("stop_token_ids survived: %v", document)
			}
		})
	}
}

func TestStopTokenIdsDoNotCostTheRequestItsMinTokens(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"min_tokens":100,"stop_token_ids":[1,2]}`, "Qwen/Test")
	if _, present := document["stop_token_ids"]; present {
		t.Errorf("stop_token_ids survived: %v", document)
	}
	requireUintField(t, document, "min_tokens", 100)
}

// The smaller of the two output-budget fields wins, but it cannot carry the request below the floor
// validation is measured against, whichever field the client made small.
func TestFloorHoldsWhenOneOutputBudgetFieldIsTiny(t *testing.T) {
	for _, body := range []string{
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"max_completion_tokens":16}`,
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":16,"max_completion_tokens":4096}`,
	} {
		t.Run(body, func(t *testing.T) {
			document := normalizedDocument(t, body, "Qwen/Test")
			requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
			requireUintField(t, document, "max_completion_tokens", completionapi.MinTokensFloor)
			requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
		})
	}
}

// Both fields leave agreeing, so no downstream layer has to decide which of them it believes.
func TestBothOutputBudgetFieldsLeaveAgreeing(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"max_completion_tokens":200}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", 200)
	requireUintField(t, document, "max_completion_tokens", 200)
}
