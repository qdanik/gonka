package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"common/completionapi"

	"devshard/e2e/testutil"
	"devshard/internal/e2econfig"
)

// The filters refuse a parameter the network does not serve, and that refusal has to cost nothing:
// no host ever sees the request, so no nonce may be reserved for it either.
func TestE2E_GatewayRejectsAnUnsupportedParameterWithoutSpendingANonce(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	before := gatewaySessionNonce(t, client, env.clientURL)
	body := testutil.ChatCompletionBody("unsupported parameter", false)
	body["audio"] = map[string]any{"voice": "alloy", "format": "wav"}
	rejected := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/chat/completions", body, testutil.AdminAPIKey)

	if rejected.StatusCode != http.StatusBadRequest {
		t.Fatalf("an unsupported parameter was answered %d %s, want 400", rejected.StatusCode, rejected.Body)
	}
	if !strings.Contains(rejected.Body, "audio") {
		t.Errorf("rejection = %s, want it to name the parameter the client has to remove", rejected.Body)
	}
	if after := gatewaySessionNonce(t, client, env.clientURL); after != before {
		t.Errorf("nonce moved %d -> %d on a request the filters refused", before, after)
	}
}

// echoingHosts turns the stub's echo on for every host, so whichever one wins the race answers with what it received.
func echoingHosts() map[int]map[string]string {
	hosts := make(map[int]map[string]string, e2eHostCount)
	for index := range e2eHostCount {
		hosts[index] = map[string]string{e2econfig.StubInferenceEchoRequestEnv: "1"}
	}
	return hosts
}

// echoedRequest reads the body the host received back out of the answer an echoing stub produced.
func echoedRequest(t *testing.T, resp testutil.RawResponse) map[string]any {
	t.Helper()
	require.Equal(t, http.StatusOK, resp.StatusCode, "completion should be served: %s", resp.Body)
	choices, ok := resp.JSON["choices"].([]any)
	require.True(t, ok, "echoed answer should carry choices: %s", resp.Body)
	require.NotEmpty(t, choices, "echoed answer should carry a choice: %s", resp.Body)
	choice, ok := choices[0].(map[string]any)
	require.True(t, ok, "echoed choice should be an object: %s", resp.Body)
	message, ok := choice["message"].(map[string]any)
	require.True(t, ok, "echoed choice should carry a message: %s", resp.Body)
	content, ok := message["content"].(string)
	require.True(t, ok, "echoed message should carry string content: %s", resp.Body)

	var received map[string]any
	require.NoError(t, json.Unmarshal([]byte(content), &received), "echoed content should be the request body: %s", content)
	return received
}

// The output-token cap is the network's, not the caller's: an ordinary request is clamped before any
// host sees the body, and the admin key is what lifts the clamp.
func TestE2E_GatewayCapsOutputTokensUntilTheAdminKeyLiftsIt(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{hostEnvOverrides: echoingHosts()})

	const requested = 8192
	body := testutil.ChatCompletionBody("output token cap", false)
	body["max_tokens"] = requested

	capped := echoedRequest(t, testutil.PostJSONRaw(t, client, env.clientURL+"/v1/chat/completions", body, ""))
	if got := capped["max_tokens"]; got != float64(e2eMaxTokensCap) {
		t.Errorf("an ordinary request reached the host with max_tokens = %v, want the cap %d", got, e2eMaxTokensCap)
	}

	lifted := echoedRequest(t, testutil.PostJSONRaw(t, client, env.clientURL+"/v1/chat/completions", body, testutil.AdminAPIKey))
	if got := lifted["max_tokens"]; got != float64(requested) {
		t.Errorf("an admin request reached the host with max_tokens = %v, want the requested %d", got, requested)
	}
}

// Whatever the client asks for, every host is asked the same way: streaming, with usage, and with the
// logprobs the validation path needs. Nothing below is a client choice, so the echo is where it shows.
func TestE2E_GatewayForcesTheUpstreamShapeEveryHostSees(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{hostEnvOverrides: echoingHosts()})

	body := testutil.ChatCompletionBody("forced upstream shape", false)
	received := echoedRequest(t, testutil.PostJSONRaw(t, client, env.clientURL+"/v1/chat/completions", body, testutil.AdminAPIKey))

	forced := []struct {
		field string
		want  any
	}{
		{"stream", true},
		{"logprobs", true},
		{"return_token_ids", true},
		{"top_logprobs", float64(completionapi.ForcedTopLogprobs)},
	}
	for _, expected := range forced {
		if got := received[expected.field]; got != expected.want {
			t.Errorf("host received %s = %v, want the forced %v", expected.field, got, expected.want)
		}
	}
	options, ok := received["stream_options"].(map[string]any)
	if !ok {
		t.Fatalf("host received stream_options = %v, want the forced object", received["stream_options"])
	}
	if got := options["include_usage"]; got != true {
		t.Errorf("host received stream_options.include_usage = %v, want true", got)
	}
}

// Every request in this suite asks for fewer output tokens than a host is allowed to answer with, and
// the floor has been quietly raising them all along. It is a contract, so it gets an assertion.
func TestE2E_GatewayFloorsTheOutputBudgetBeforeAHostSeesIt(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{hostEnvOverrides: echoingHosts()})

	body := testutil.ChatCompletionBody("below the floor", false)
	body["max_tokens"] = 1
	received := echoedRequest(t, testutil.PostJSONRaw(t, client, env.clientURL+"/v1/chat/completions", body, testutil.AdminAPIKey))

	if got := received["max_tokens"]; got != float64(completionapi.MinTokensFloor) {
		t.Errorf("host received max_tokens = %v, want the floor %d", got, completionapi.MinTokensFloor)
	}
	if got := received["min_tokens"]; got != float64(completionapi.MinTokensFloor) {
		t.Errorf("host received min_tokens = %v, want the floor %d", got, completionapi.MinTokensFloor)
	}
}

// extra_body is an SDK envelope, not a parameter: the gateway flattens it and forwards what was inside,
// so a host never has to know the envelope existed.
func TestE2E_GatewayUnwrapsExtraBodyBeforeAHostSeesIt(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{hostEnvOverrides: echoingHosts()})

	const nestedTopK = 40
	body := testutil.ChatCompletionBody("enveloped parameter", false)
	body["extra_body"] = map[string]any{"top_k": nestedTopK}
	received := echoedRequest(t, testutil.PostJSONRaw(t, client, env.clientURL+"/v1/chat/completions", body, testutil.AdminAPIKey))

	if got := received["top_k"]; got != float64(nestedTopK) {
		t.Errorf("host received top_k = %v, want the %d lifted out of extra_body", got, nestedTopK)
	}
	if _, present := received["extra_body"]; present {
		t.Errorf("host received extra_body = %v, want the envelope dropped", received["extra_body"])
	}
}
