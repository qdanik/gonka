package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func SendCompletion(t *testing.T, client *http.Client, clientURL, content string) map[string]any {
	t.Helper()
	DebugLogf(t, "sending completion request content=%q", content)
	resp := PostJSON(t, client, clientURL+"/v1/chat/completions", ChatCompletionBody(content, false))
	require.NotEmpty(t, resp["choices"], "completion response should include choices")
	return resp
}

func SendCompletionRaw(t *testing.T, client *http.Client, clientURL, content, bearerToken string) RawResponse {
	t.Helper()
	DebugLogf(t, "sending raw completion request content=%q bearer_set=%t", content, bearerToken != "")
	return PostJSONRaw(t, client, clientURL+"/v1/chat/completions", ChatCompletionBody(content, false), bearerToken)
}

func SendCompletionRawE(client *http.Client, clientURL, content, bearerToken string) (RawResponse, error) {
	return PostJSONRawE(client, clientURL+"/v1/chat/completions", ChatCompletionBody(content, false), bearerToken)
}

type StreamResponse struct {
	ContentType string
	Events      []string
}

func SendStreamingCompletion(t *testing.T, client *http.Client, clientURL, content string) StreamResponse {
	t.Helper()
	DebugLogf(t, "sending streaming completion request content=%q", content)

	data, err := json.Marshal(ChatCompletionBody(content, true))
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, clientURL+"/v1/chat/completions", strings.NewReader(string(data)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+AdminAPIKey)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, events := readSSEEvents(t, resp.Body)
	DebugLogf(t, "streaming completion status=%d content_type=%q body=%s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	require.Less(t, resp.StatusCode, 300, "streaming completion returned %d: %s", resp.StatusCode, body)

	return StreamResponse{
		ContentType: resp.Header.Get("Content-Type"),
		Events:      events,
	}
}

func SendCompletions(t *testing.T, client *http.Client, clientURL, contentPrefix string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		SendCompletion(t, client, clientURL, fmt.Sprintf("%s %d", contentPrefix, i+1))
	}
}

func ChatCompletionBody(content string, stream bool) map[string]any {
	body := map[string]any{
		"model": "stub-model",
		"messages": []map[string]string{
			{"role": "user", "content": content},
		},
		"max_tokens": 32,
	}
	if stream {
		body["stream"] = true
	}
	return body
}

const ToolChoiceUnsupportedMessage = "tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"

// The gateway classifies a state divergence off this wording, so a stub host has to reproduce it verbatim.
const StateRootDivergenceMessage = "apply diff nonce 1: post_state_root does not match computed state root: diff 00, computed 11"

func ToolCompletionBody(content string, stream bool) map[string]any {
	body := ChatCompletionBody(content, stream)
	body["tool_choice"] = "auto"
	body["tools"] = []map[string]any{{
		"type": "function",
		"function": map[string]any{
			"name":        "lookup_status",
			"description": "Return a test status string.",
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}}
	return body
}

func readSSEEvents(t *testing.T, body io.Reader) (string, []string) {
	t.Helper()
	var raw strings.Builder
	var events []string
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		raw.WriteString(line)
		raw.WriteByte('\n')
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		events = append(events, strings.TrimPrefix(line, "data: "))
	}
	require.NoError(t, scanner.Err())
	return raw.String(), events
}

func DriveUntilValidationObserved(t *testing.T, client *http.Client, clientURL string) {
	t.Helper()
	const maxExtraCompletions = 20
	const validationTarget = 2
	for attempt := 0; attempt <= maxExtraCompletions; attempt++ {
		state := GetJSON(t, client, clientURL+"/v1/debug/inferences")
		reached, summary := HasInferenceValidationTarget(t, state, validationTarget)
		DebugLogf(t, "inference validation evidence before finalize target=%d reached=%t (%s)",
			validationTarget, reached, summary)
		if reached {
			return
		}
		if attempt == maxExtraCompletions {
			t.Fatalf("no host reached at least %d completed validations before finalize after %d extra completion rounds: %s",
				validationTarget, maxExtraCompletions, summary)
		}
		SendCompletion(t, client, clientURL, fmt.Sprintf("validation probe %d", attempt+1))
		time.Sleep(250 * time.Millisecond)
	}
}

func LatestSessionNonce(t *testing.T, client *http.Client, clientURL string) uint64 {
	t.Helper()
	state := GetJSON(t, client, clientURL+"/v1/state")
	session, ok := state["session"].(map[string]any)
	require.True(t, ok, "state session should be an object")
	return NumericField(t, session, "latest_nonce")
}

func FinalizeSession(t *testing.T, client *http.Client, clientURL string) map[string]any {
	t.Helper()
	DebugLogf(t, "finalizing devshard session")
	settlement := PostJSON(t, client, clientURL+"/v1/finalize", map[string]any{})
	settlementJSON, err := json.MarshalIndent(settlement, "", "  ")
	require.NoError(t, err)
	t.Logf("SettlementContract:\n%s", settlementJSON)
	return settlement
}

// LeakyHostAnswer carries every field the response side hides, at the depths vLLM writes them.
const LeakyHostAnswer = `{"choices":[{"message":{"content":"stub"},"logprobs":{"content":[{"logprob":-0.5,"top_logprobs":[]}]},"token_ids":[1,2,3]}],"prompt_token_ids":[4,5],"prompt_logprobs":[null],"usage":{"prompt_tokens":80,"completion_tokens":40}}`

// EchoedRequest reads the request body back out of an echoing host's answer.
func EchoedRequest(t *testing.T, resp RawResponse) map[string]any {
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

// EscrowBalance reads what the live escrow session has left; the stored record carries no balance.
func EscrowBalance(t *testing.T, client *http.Client, clientURL, escrowID, bearerToken string) uint64 {
	t.Helper()
	resp := GetRaw(t, client, clientURL+"/v1/admin/state", bearerToken)
	require.Equal(t, http.StatusOK, resp.StatusCode, "reading gateway state: %s", resp.Body)
	var state struct {
		Status struct {
			Devshards []struct {
				EscrowID string `json:"escrow_id"`
				Balance  uint64 `json:"balance"`
			} `json:"devshards"`
		} `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &state), "parsing gateway state: %s", resp.Body)
	for _, escrow := range state.Status.Devshards {
		if escrow.EscrowID == escrowID {
			return escrow.Balance
		}
	}
	t.Fatalf("gateway state carries no escrow %s: %s", escrowID, resp.Body)
	return 0
}
