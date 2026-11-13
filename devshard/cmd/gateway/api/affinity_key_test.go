package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/filters"
)

// Test flow:
// 1. Derive a key for the same prompt_cache_key under two authenticated bearer keys, and under a bearer that did not authenticate.
// 2. Expect two different keys for the two bearers, the same key on a repeat, and no key for the bearer that did not authenticate.
func TestAffinityKeyIsPerAuthenticatedKeyAndAbsentOtherwise(t *testing.T) {
	normalized := filters.Result{Affinity: filters.AffinityMaterial{PromptCacheKey: "session-1"}}
	authenticated := credentials{apiKey: true}
	request := func(authorization string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		r.Header.Set("Authorization", authorization)
		return r
	}

	first := affinityKey(request("Bearer key-a"), authenticated, normalized)
	second := affinityKey(request("Bearer key-b"), authenticated, normalized)

	require.NotEqual(t, affinity.NoKey, first)
	require.NotEqual(t, first, second)
	require.Equal(t, first, affinityKey(request("Bearer key-a"), authenticated, normalized))
	require.Equal(t, affinity.NoKey, affinityKey(request("Bearer key-a"), credentials{}, normalized))
}

// Test flow:
// 1. Start a gateway with affinity on, the response cache off, the open model qwen and no keys configured, and send a chat completion with `Bearer EMPTY`.
// 2. Expect the race to be handed no affinity key.
// 3. Configure a client key and an admin key, then send the chat completion with each of them and again with `Bearer EMPTY`.
// 4. Expect a key for the client and the admin bearer, and still none for `Bearer EMPTY`.
func TestAffinityKeyOnlyForAuthenticatedCallers(t *testing.T) {
	live := newHarness(t, func(configuration *config.Config) {
		configuration.Affinity.Enabled = true
		configuration.Cache.ChatCacheMaxBytes = 0
		configuration.Server.APIKeys = nil
		configuration.Server.AdminAPIKey = ""
	})
	racedKey := func(bearer string) affinity.Key {
		live.inference.raced.Store(nil)
		recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, map[string]string{"Authorization": "Bearer " + bearer})
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		raced := live.inference.raced.Load()
		require.NotNil(t, raced, "the race was never handed a request")
		return raced.AffinityKey
	}

	require.Equal(t, affinity.NoKey, racedKey("EMPTY"))

	live.swapConfig(func(configuration *config.Config) {
		configuration.Server.APIKeys = []string{clientKey}
		configuration.Server.AdminAPIKey = adminKey
	})

	require.NotEqual(t, affinity.NoKey, racedKey(clientKey))
	require.NotEqual(t, affinity.NoKey, racedKey(adminKey))
	require.Equal(t, affinity.NoKey, racedKey("EMPTY"))
}

// Test flow:
// 1. Derive keys for the same authenticated bearer, prompt_cache_key and leading messages under two models.
// 2. Expect a key for each and the two keys to differ.
func TestAffinityKeyIsPerModel(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	request.Header.Set("Authorization", "Bearer key-a")
	material := filters.AffinityMaterial{PromptCacheKey: "session-1", LeadingMessages: []byte(`{"role":"user","content":"hi"}`)}
	authenticated := credentials{apiKey: true}

	first := affinityKey(request, authenticated, filters.Result{Model: "qwen", Affinity: material})
	second := affinityKey(request, authenticated, filters.Result{Model: "llama", Affinity: material})

	require.NotEqual(t, affinity.NoKey, first)
	require.NotEqual(t, affinity.NoKey, second)
	require.NotEqual(t, first, second)
}
