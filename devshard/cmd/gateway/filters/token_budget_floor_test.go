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

// Test flow:
//  1. Normalize a request with max_tokens set and min_tokens omitted.
//  2. Assert min_tokens is injected at completionapi.MinTokensFloor.
func TestFloorInjectsMinTokensWhenTheClientOmitsIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096}`, "Qwen/Test")
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

// Test flow:
//  1. Normalize a request with min_tokens set below the floor.
//  2. Assert min_tokens is lifted to completionapi.MinTokensFloor.
func TestFloorLiftsAMinTokensBelowIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"min_tokens":4}`, "Qwen/Test")
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

// Test flow:
//  1. Normalize a request with min_tokens set above the floor.
//  2. Assert min_tokens is left unchanged at its original value.
func TestFloorKeepsAMinTokensAboveIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"min_tokens":100}`, "Qwen/Test")
	requireUintField(t, document, "min_tokens", 100)
}

// Test flow:
//  1. Normalize a request whose max_tokens is above the floor but whose min_tokens is far larger than that resolved budget.
//  2. Assert max_tokens stays at the value the client asked for.
//  3. Assert min_tokens is clamped down to that same resolved budget, since it has to fit inside the budget it is measured against, whatever the client asked for.
func TestFloorClampsMinTokensToTheResolvedBudget(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":300,"min_tokens":9000}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", 300)
	requireUintField(t, document, "min_tokens", 300)
}

// Test flow:
//  1. Normalize a request whose max_tokens is below the floor.
//  2. Assert max_tokens is lifted to completionapi.MinTokensFloor.
//  3. Assert min_tokens is injected at the same floor.
func TestFloorLiftsASmallMaxTokensAndInjectsMinTokens(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

// Test flow:
//  1. Normalize a request that sends max_completion_tokens, below the floor, instead of max_tokens.
//  2. Assert max_tokens, max_completion_tokens, and min_tokens are all lifted to completionapi.MinTokensFloor.
func TestFloorMirrorsMaxCompletionTokensWhenTheClientSentIt(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":8}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
	requireUintField(t, document, "max_completion_tokens", completionapi.MinTokensFloor)
	requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
}

// Test flow:
//  1. Normalize a below-floor request once per routed model: `kimiModelID`, `minimaxModelID`, and a plain "Qwen/Test" model.
//  2. Assert max_tokens and min_tokens are lifted to completionapi.MinTokensFloor for every route, not only for a profile that declares a floor of its own.
func TestFloorAppliesToEveryRoute(t *testing.T) {
	for _, model := range []string{kimiModelID, minimaxModelID, "Qwen/Test"} {
		t.Run(model, func(t *testing.T) {
			document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`, model)
			requireUintField(t, document, "max_tokens", completionapi.MinTokensFloor)
			requireUintField(t, document, "min_tokens", completionapi.MinTokensFloor)
		})
	}
}

// Test flow:
//  1. Normalize a below-floor request and capture the returned result's declared MaxTokens.
//  2. Assert the normalized body's max_tokens field matches the declared MaxTokens exactly, since the gateway signs its reservation (input + max_tokens) against that same field.
//  3. Assert the declared MaxTokens is not below completionapi.MinTokensFloor.
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

// Test flow:
//  1. Normalize requests carrying stop_token_ids as valid ids, a non-numeric id, a negative id, and a non-array value.
//  2. Assert stop_token_ids is stripped from every case without being validated, since vLLM masks stop-token logits when it is set and an out-of-vocab id asserts device-side.
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

// Test flow:
//  1. Normalize a request carrying both stop_token_ids and an above-floor min_tokens.
//  2. Assert stop_token_ids is stripped and min_tokens is left at its original value.
func TestStopTokenIdsDoNotCostTheRequestItsMinTokens(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"min_tokens":100,"stop_token_ids":[1,2]}`, "Qwen/Test")
	if _, present := document["stop_token_ids"]; present {
		t.Errorf("stop_token_ids survived: %v", document)
	}
	requireUintField(t, document, "min_tokens", 100)
}

// Test flow:
//  1. Normalize requests where either max_tokens or max_completion_tokens is set far below the floor while the other field is large.
//  2. Assert max_tokens, max_completion_tokens, and min_tokens are all lifted to completionapi.MinTokensFloor regardless of which field the client made tiny — the smaller of the two output-budget fields wins but cannot carry the request below the floor.
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

// Test flow:
//  1. Normalize a request where max_tokens and max_completion_tokens agree above the floor.
//  2. Assert both fields leave the normalizer still agreeing on that same value, so no downstream layer has to decide which one it believes.
func TestBothOutputBudgetFieldsLeaveAgreeing(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"max_completion_tokens":200}`, "Qwen/Test")
	requireUintField(t, document, "max_tokens", 200)
	requireUintField(t, document, "max_completion_tokens", 200)
}
