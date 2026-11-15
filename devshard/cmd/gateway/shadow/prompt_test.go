package shadow

import (
	"strings"
	"testing"

	json "github.com/goccy/go-json"

	"devshard/user"
)

type wordCounter struct {
	tokensPerWord uint64
	unknown       bool
}

func (c wordCounter) Tokens(_ string, body []byte) (uint64, bool) {
	return uint64(strings.Count(string(body), probeWord)) * c.tokensPerWord, !c.unknown
}

type probeRequestBody struct {
	Messages  []struct{ Content string } `json:"messages"`
	MaxTokens uint64                     `json:"max_tokens"`
	Stream    bool                       `json:"stream"`
}

func decodeBody(t *testing.T, body []byte) probeRequestBody {
	t.Helper()
	var decoded probeRequestBody
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("probe body %q is not JSON: %v", body, err)
	}
	return decoded
}

// Test flow:
//  1. Build prompts over a counter that counts one token per word.
//  2. Take the body for a model.
//  3. Assert it decodes, asks for 64 output tokens as a stream, and its content counts 64 tokens.
func TestPromptCountsTheInputTokensItPromises(t *testing.T) {
	t.Parallel()
	body := decodeBody(t, newPrompts(wordCounter{tokensPerWord: 1}).body(probeModel))

	if body.MaxTokens != ProbeMaxTokens || !body.Stream || len(body.Messages) != 1 {
		t.Fatalf("body = %+v, want one message, %d max tokens, streaming", body, ProbeMaxTokens)
	}
	if got := strings.Count(body.Messages[0].Content, probeWord); got != ProbeInputTokens {
		t.Errorf("words = %d, want %d: one token each", got, ProbeInputTokens)
	}
}

// Test flow:
//  1. Build prompts over a counter that counts two tokens per word.
//  2. Take the body for a model.
//  3. Assert the content was fitted to half the words, so the counted tokens still come to 64.
func TestPromptIsFittedToTheTokenizersCount(t *testing.T) {
	t.Parallel()
	body := decodeBody(t, newPrompts(wordCounter{tokensPerWord: 2}).body(probeModel))

	if got := strings.Count(body.Messages[0].Content, probeWord) * 2; got != ProbeInputTokens {
		t.Errorf("counted tokens = %d, want %d", got, ProbeInputTokens)
	}
}

// Test flow:
//  1. Build prompts over a counter that does not know the model, and over no counter at all.
//  2. Take the body for a model from each.
//  3. Assert both fall back to a content of 64 bytes.
func TestPromptFallsBackToBytesWithoutATokenizer(t *testing.T) {
	t.Parallel()
	for name, counter := range map[string]TokenCounter{"unknown model": wordCounter{unknown: true}, "no counter": nil} {
		if got := len(decodeBody(t, newPrompts(counter).body(probeModel)).Messages[0].Content); got != fallbackBytes {
			t.Errorf("%s: content bytes = %d, want %d", name, got, fallbackBytes)
		}
	}
}

// Test flow:
//  1. Build prompts over a counter that knows the model and count its calls.
//  2. Take the body for the model twice.
//  3. Assert both bodies are equal and the tokenizer was asked once, so the fitted body is reused per model.
func TestFittedPromptIsReused(t *testing.T) {
	t.Parallel()
	counter := &countingCounter{}
	built := newPrompts(counter)

	first, second := built.body(probeModel), built.body(probeModel)

	if string(first) != string(second) || counter.calls != 1 {
		t.Errorf("tokenizer calls = %d, want 1: the fitted body is reused", counter.calls)
	}
}

type countingCounter struct{ calls int }

func (c *countingCounter) Tokens(_ string, body []byte) (uint64, bool) {
	c.calls++
	return uint64(strings.Count(string(body), probeWord)), true
}

// Test flow:
//  1. Bind a runner and ask it for the params of a probe.
//  2. Assert the params carry the model, 64 max tokens, the start time and an input length equal to the body's bytes, which is what a host verifies.
func TestParamsCarryTheBodyLengthAsInputLength(t *testing.T) {
	t.Parallel()
	params, ok := New(steppedClock()).Params(probeModel, 42).(user.InferenceParams)

	if !ok || params.Model != probeModel || params.MaxTokens != ProbeMaxTokens || params.StartedAt != 42 || params.InputLength != uint64(len(params.Prompt)) {
		t.Errorf("params = %+v, want the model, %d max tokens, started at 42 and InputLength = len(Prompt)", params, ProbeMaxTokens)
	}
}
