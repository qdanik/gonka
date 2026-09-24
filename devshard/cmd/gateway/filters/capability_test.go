package filters

import (
	"encoding/json"
	"testing"
)

// kelvinSign is a multi-byte rune that lowercases to a single-byte "k".
const kelvinSign = "K"

// Test flow:
//  1. Build a message prefixed with kelvinSign, whose lowercased copy is two bytes shorter than the original, followed by a context-length refusal.
//  2. Call CapabilityLimits on the message.
//  3. Assert the parsed context limit is correct despite the byte-offset shift between the lowered copy and the original.
func TestCapabilityLimitsSurvivesALowercasingThatShortensTheMessage(t *testing.T) {
	t.Parallel()
	message := kelvinSign + " Maximum context length is 8192 tokens, however you requested 9000"

	contextLimit, _ := CapabilityLimits(message)

	if contextLimit != 8192 {
		t.Fatalf("context limit = %d, want 8192", contextLimit)
	}
}

// Test flow:
//  1. Call CapabilityLimits on a message whose digits are the last characters.
//  2. Assert the parsed context limit is correct.
func TestCapabilityLimitsReadsDigitsThatEndTheMessage(t *testing.T) {
	t.Parallel()
	contextLimit, _ := CapabilityLimits("This model's maximum context length is 8192")

	if contextLimit != 8192 {
		t.Fatalf("context limit = %d, want 8192", contextLimit)
	}
}

// Test flow:
//  1. Call CapabilityLimits on a message stating both the maximum context length and the requested total.
//  2. Assert both the context limit and the requested total are parsed correctly.
func TestCapabilityLimitsReadsTheRequestedTotal(t *testing.T) {
	t.Parallel()
	message := "This model's maximum context length is 8192 tokens. However, you requested for a total of at least 9001 tokens."

	contextLimit, contextRequested := CapabilityLimits(message)

	if contextLimit != 8192 || contextRequested != 9001 {
		t.Fatalf("limits = (%d, %d), want (8192, 9001)", contextLimit, contextRequested)
	}
}

// Test flow:
//  1. Call CapabilityLimits on a message that mentions the maximum context length without any digits.
//  2. Assert both returned values are zero.
func TestCapabilityLimitsIgnoresAPhraseWithoutDigits(t *testing.T) {
	t.Parallel()
	contextLimit, contextRequested := CapabilityLimits("maximum context length is unknown")

	if contextLimit != 0 || contextRequested != 0 {
		t.Fatalf("limits = (%d, %d), want (0, 0)", contextLimit, contextRequested)
	}
}

// Test flow:
//  1. Build an error body for each case's capability-refusal message via `errorBody`, varying the phrasing between a shortening-lowercase message, a message whose digits end it, and the tool-choice-unsupported message.
//  2. Assert IsCacheableResponse reports the 400 response as not cacheable.
//  3. Assert HasNonCacheableError reports the body as a non-cacheable error.
func TestContextRefusalStaysOutOfTheCacheHoweverItIsSpelled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		message string
	}{
		{name: "shortening_lowercase", message: kelvinSign + " Maximum context length is 8192 tokens"},
		{name: "digits_end_the_message", message: "This model's maximum context length is 8192"},
		{name: "tool_choice", message: ToolChoiceUnsupportedMessage},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			body := errorBody(t, testCase.message)
			if IsCacheableResponse(400, body) {
				t.Fatalf("cached a capability refusal the race would have retried: %q", testCase.message)
			}
			if !HasNonCacheableError(body) {
				t.Fatalf("capability refusal not reported as non-cacheable: %q", testCase.message)
			}
		})
	}
}

func errorBody(t *testing.T, message string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"error": map[string]any{"message": message, "type": "BadRequestError", "code": 400},
	})
	if err != nil {
		t.Fatalf("marshalling error body: %v", err)
	}
	return body
}
