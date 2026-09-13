package filters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGLM53FlashProfileHooks(t *testing.T) {
	require.Equal(t, []string{glm53FlashModelID}, glm53FlashProfile.Models)

	boolHooks := []struct {
		name string
		got  bool
	}{
		{"ForceZeroPenalties", glm53FlashProfile.ForceZeroPenalties},
		{"RejectStructuredOutput", glm53FlashProfile.RejectStructuredOutput},
		{"AllowSafetyIdentifier", glm53FlashProfile.AllowSafetyIdentifier},
		{"KeepReasoningSplit", glm53FlashProfile.KeepReasoningSplit},
		{"ThinkingTokenBudget", glm53FlashProfile.ThinkingTokenBudget},
		{"LiftNonPositiveOutputTokens", glm53FlashProfile.LiftNonPositiveOutputTokens},
	}
	for _, hook := range boolHooks {
		require.Falsef(t, hook.got, "glm53FlashProfile.%s", hook.name)
	}
	require.Equal(t, ThinkingForceOn, glm53FlashProfile.Thinking)
}

func TestGLM53FlashReportsThinkingOnWhateverTheCallerSent(t *testing.T) {
	testCases := []struct {
		name               string
		body               string
		chatTemplateKwargs map[string]any
	}{
		{name: "chat_template_kwargs enable_thinking false", body: `{"messages":[{"role":"user","content":"hi"}],"chat_template_kwargs":{"enable_thinking":false}}`, chatTemplateKwargs: map[string]any{"enable_thinking": true}},
		{name: "chat_template_kwargs thinking false", body: `{"messages":[{"role":"user","content":"hi"}],"chat_template_kwargs":{"thinking":false}}`, chatTemplateKwargs: map[string]any{"thinking": false, "enable_thinking": true}},
		{name: "top-level enable_thinking false", body: `{"messages":[{"role":"user","content":"hi"}],"enable_thinking":false}`, chatTemplateKwargs: map[string]any{"enable_thinking": true}},
		{name: "reasoning_effort none", body: `{"messages":[{"role":"user","content":"hi"}],"reasoning_effort":"none"}`, chatTemplateKwargs: map[string]any{"enable_thinking": true}},
		{name: "reasoning enabled false", body: `{"messages":[{"role":"user","content":"hi"}],"reasoning":{"enabled":false}}`, chatTemplateKwargs: map[string]any{"enable_thinking": true}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			document := normalizedDocument(t, testCase.body, glm53FlashModelID)

			require.Equal(t, testCase.chatTemplateKwargs, document["chat_template_kwargs"],
				"enable_thinking must reach vLLM as true, with the caller's other kwargs as sent")
			require.NotContains(t, document, "enable_thinking")
		})
	}
}

func TestGLM53FlashKeepsTheCallersOtherTemplateKwargs(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"hi"}],"chat_template_kwargs":{"enable_thinking":false,"clear_thinking":true}}`, glm53FlashModelID)

	require.Equal(t, map[string]any{"enable_thinking": true, "clear_thinking": true}, document["chat_template_kwargs"])
}

func TestGLM53FlashLeavesTheCallersReasoningEffortAsSent(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"hi"}],"reasoning_effort":"none"}`, glm53FlashModelID)

	require.Equal(t, "none", document["reasoning_effort"])
}

func TestGLM52FP8KeepsTheCallersEnableThinkingFalse(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"hi"}],"chat_template_kwargs":{"enable_thinking":false}}`, "zai-org/GLM-5.2-FP8")

	require.Equal(t, map[string]any{"enable_thinking": false}, document["chat_template_kwargs"])
}

func TestGLM53FlashNormalizesATopLevelThinkingObjectLikeTheDefaultRoute(t *testing.T) {
	document := normalizedDocument(t, `{"messages":[{"role":"user","content":"hi"}],"thinking":{"type":"disabled"}}`, glm53FlashModelID)

	require.Equal(t, map[string]any{"type": "disabled"}, document["thinking"])
	require.Equal(t, map[string]any{"enable_thinking": true}, document["chat_template_kwargs"])
}
