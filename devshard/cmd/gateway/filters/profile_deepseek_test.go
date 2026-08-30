package filters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeepseekProfileHooks(t *testing.T) {
	require.Equal(t, []string{deepseekModelID}, deepseekProfile.Models)

	// DeepSeek carries no hooks at all; one gained here silently would change the route without a decision.
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

// The level is the caller's to choose: an omitted field is forwarded as omitted, and the encoder's own
// fallback of "high" applies upstream.
func TestDeepseekFillsNoReasoningEffort(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096}`, deepseekModelID)
	require.NotContains(t, document, "reasoning_effort")
}

func TestDeepseekKeepsAClientReasoningEffort(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"reasoning_effort":"low"}`, deepseekModelID)
	require.Equal(t, "low", document["reasoning_effort"])
}

// The budget is not a DeepSeek hook, so nothing invents one; a client that asks for it still gets it.
func TestDeepseekInventsNoThinkingBudget(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096}`, deepseekModelID)
	_, present := document["thinking_token_budget"]
	require.False(t, present)
}

func TestDeepseekStripsReasoningSplit(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"reasoning_split":true}`, deepseekModelID)
	_, present := document["reasoning_split"]
	require.False(t, present, "reasoning_split is MiniMax's field; other routes must not forward it")
}

func TestDeepseekRejectsAZeroMaxTokens(t *testing.T) {
	_, err := NormalizeRequest([]byte(`{"model":"`+deepseekModelID+`","messages":[{"role":"user","content":"x"}],"max_tokens":0}`),
		Options{RoutedModel: deepseekModelID})
	require.Error(t, err)
}
