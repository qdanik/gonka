package filters

import (
	"encoding/json"
	"testing"
)

// kelvinSign lowercases to a one-byte "k", so an index taken from the lowered copy and applied to
// the original lands two bytes early.
const kelvinSign = "K"

func TestCapabilityLimitsSurvivesALowercasingThatShortensTheMessage(t *testing.T) {
	t.Parallel()
	message := kelvinSign + " Maximum context length is 8192 tokens, however you requested 9000"

	contextLimit, _ := CapabilityLimits(message)

	if contextLimit != 8192 {
		t.Fatalf("context limit = %d, want 8192", contextLimit)
	}
}

func TestCapabilityLimitsReadsDigitsThatEndTheMessage(t *testing.T) {
	t.Parallel()
	contextLimit, _ := CapabilityLimits("This model's maximum context length is 8192")

	if contextLimit != 8192 {
		t.Fatalf("context limit = %d, want 8192", contextLimit)
	}
}

func TestCapabilityLimitsReadsTheRequestedTotal(t *testing.T) {
	t.Parallel()
	message := "This model's maximum context length is 8192 tokens. However, you requested for a total of at least 9001 tokens."

	contextLimit, contextRequested := CapabilityLimits(message)

	if contextLimit != 8192 || contextRequested != 9001 {
		t.Fatalf("limits = (%d, %d), want (8192, 9001)", contextLimit, contextRequested)
	}
}

func TestCapabilityLimitsIgnoresAPhraseWithoutDigits(t *testing.T) {
	t.Parallel()
	contextLimit, contextRequested := CapabilityLimits("maximum context length is unknown")

	if contextLimit != 0 || contextRequested != 0 {
		t.Fatalf("limits = (%d, %d), want (0, 0)", contextLimit, contextRequested)
	}
}

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
