package filters

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func normalizeForAffinity(t *testing.T, body string, enabled bool) AffinityMaterial {
	t.Helper()
	result, err := NormalizeRequest([]byte(body), Options{DefaultMaxTokens: 3072, MaxTokensCap: 4096, RoutedModel: "model-a", Affinity: enabled})
	require.NoError(t, err)
	return result.Affinity
}

// Test flow:
//  1. Normalize a body carrying a `prompt_cache_key`, at top level and inside `extra_body`, with the mode on.
//  2. Assert the key is read although the pipeline strips the field, and no leading messages are read beside it.
//  3. Normalize the same body with the mode off and assert nothing is read.
func TestThePromptCacheKeyIsReadBeforeTheStrip(t *testing.T) {
	top := `{"model":"model-a","prompt_cache_key":"session-1","messages":[{"role":"user","content":"hi"}]}`
	wrapped := `{"model":"model-a","extra_body":{"prompt_cache_key":"session-1"},"messages":[{"role":"user","content":"hi"}]}`

	for _, body := range []string{top, wrapped} {
		material := normalizeForAffinity(t, body, true)
		require.Equal(t, "session-1", material.PromptCacheKey)
		require.Empty(t, material.LeadingMessages)
	}
	require.Equal(t, AffinityMaterial{}, normalizeForAffinity(t, top, false))
}

// Test flow:
//  1. Normalize a body with a system prompt, a first user message, an assistant reply and a second user message, and no cache key.
//  2. Assert the leading messages hold the system prompt and the first user message only.
//  3. Normalize the same conversation one turn later and assert the leading messages are byte-identical.
//  4. Normalize a body whose system prompt is over the cap and assert the leading messages are cut at it.
func TestTheLeadingMessagesAreTheSystemPromptAndTheFirstUserMessage(t *testing.T) {
	turnOne := `{"model":"model-a","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"first"}]}`
	turnTwo := `{"model":"model-a","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}]}`

	first := normalizeForAffinity(t, turnOne, true)
	second := normalizeForAffinity(t, turnTwo, true)

	require.Empty(t, first.PromptCacheKey)
	require.Contains(t, string(first.LeadingMessages), "be brief")
	require.Contains(t, string(first.LeadingMessages), "first")
	require.NotContains(t, string(first.LeadingMessages), "second")
	require.Equal(t, first.LeadingMessages, second.LeadingMessages, "the next turn of a conversation must key to the same entry")

	huge := `{"model":"model-a","messages":[{"role":"system","content":"` + strings.Repeat("x", 2*MaxAffinityLeadingBytes) + `"},{"role":"user","content":"hi"}]}`
	require.Len(t, normalizeForAffinity(t, huge, true).LeadingMessages, MaxAffinityLeadingBytes)
}
