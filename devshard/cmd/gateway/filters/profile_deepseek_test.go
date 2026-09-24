package filters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test flow:
//  1. Assert deepseekProfile.Models lists only the DeepSeek model ID.
//  2. Assert every boolean hook (ForceZeroPenalties, RejectStructuredOutput, AllowSafetyIdentifier, KeepReasoningSplit, ThinkingTokenBudget) is false, since DeepSeek carries no hooks at all.
//  3. Assert deepseekProfile.Thinking is ThinkingNormalizeInPlace.
func TestDeepseekProfileHooks(t *testing.T) {
	require.Equal(t, []string{deepseekModelID}, deepseekProfile.Models)

	boolHooks := []struct {
		name string
		got  bool
	}{
		{"ForceZeroPenalties", deepseekProfile.ForceZeroPenalties},
		{"RejectStructuredOutput", deepseekProfile.RejectStructuredOutput},
		{"AllowSafetyIdentifier", deepseekProfile.AllowSafetyIdentifier},
		{"KeepReasoningSplit", deepseekProfile.KeepReasoningSplit},
		{"ThinkingTokenBudget", deepseekProfile.ThinkingTokenBudget},
	}
	for _, hook := range boolHooks {
		require.Falsef(t, hook.got, "deepseekProfile.%s", hook.name)
	}
	require.Equal(t, ThinkingNormalizeInPlace, deepseekProfile.Thinking)
}

// Test flow:
//  1. Normalize a request with no reasoning_effort field for the DeepSeek model.
//  2. Assert the normalized document has no reasoning_effort field, since an omitted field stays omitted and the encoder's own "high" fallback applies upstream.
func TestDeepseekFillsNoReasoningEffort(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096}`, deepseekModelID)
	require.NotContains(t, document, "reasoning_effort")
}

// Test flow:
//  1. Normalize a request that sets reasoning_effort to "low" for the DeepSeek model.
//  2. Assert the normalized document keeps reasoning_effort as "low".
func TestDeepseekKeepsAClientReasoningEffort(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"reasoning_effort":"low"}`, deepseekModelID)
	require.Equal(t, "low", document["reasoning_effort"])
}

// Test flow:
//  1. Normalize a request with no thinking_token_budget field for the DeepSeek model.
//  2. Assert the normalized document has no thinking_token_budget field, since the budget is not a DeepSeek hook and nothing invents one.
func TestDeepseekInventsNoThinkingBudget(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096}`, deepseekModelID)
	_, present := document["thinking_token_budget"]
	require.False(t, present)
}

// Test flow:
//  1. Normalize a request that sets reasoning_split to true for the DeepSeek model.
//  2. Assert the normalized document has no reasoning_split field, since it is MiniMax's field and other routes must not forward it.
func TestDeepseekStripsReasoningSplit(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"reasoning_split":true}`, deepseekModelID)
	_, present := document["reasoning_split"]
	require.False(t, present, "reasoning_split is MiniMax's field; other routes must not forward it")
}

// Test flow:
//  1. Call NormalizeRequest for the DeepSeek model with max_tokens set to 0.
//  2. Assert the call returns an error.
func TestDeepseekRejectsAZeroMaxTokens(t *testing.T) {
	_, err := NormalizeRequest([]byte(`{"model":"`+deepseekModelID+`","messages":[{"role":"user","content":"x"}],"max_tokens":0}`),
		Options{RoutedModel: deepseekModelID})
	require.Error(t, err)
}
