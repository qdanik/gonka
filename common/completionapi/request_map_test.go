package completionapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test flow:
// 1. Modify each body: plain, stop token ids, logprobs settings, tool calls, non-ASCII, nested content parts, caller-sent n/min_tokens/stream_options.
// 2. Decode NewBody into a map the way a caller would.
// 3. Require RequestMap to deep-equal that decode, so typed values the modify step stores do not leak out.
func TestModifyRequestBody_RequestMapEqualsDecodedNewBody(t *testing.T) {
	bodies := map[string]string{
		"plain":            `{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
		"stop token ids":   `{"model":"m","messages":[{"role":"user","content":"hi"}],"stop_token_ids":[1,2,151645]}`,
		"logprobs on":      `{"model":"m","messages":[{"role":"user","content":"hi"}],"logprobs":true,"top_logprobs":3}`,
		"logprobs off":     `{"model":"m","messages":[{"role":"user","content":"hi"}],"logprobs":false,"top_logprobs":0}`,
		"token limits":     `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":64,"min_tokens":8,"n":3,"seed":5}`,
		"fractional limit": `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":10.5,"min_tokens":2.5}`,
		"huge limit":       `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":1e15,"temperature":0.7}`,
		"stream":           `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":false,"x":1}}`,
		"stream bad opts":  `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":7}`,
		"logprobs mode":    `{"model":"m","messages":[{"role":"user","content":"hi"}],"logprobs_mode":"raw"}`,
		"tool calls": `{"model":"m","messages":[{"role":"user","content":"weather?"},` +
			`{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"w","arguments":"{\"city\":\"Oslo\"}"}}]},` +
			`{"role":"tool","tool_call_id":"c1","content":"3"}],` +
			`"tools":[{"type":"function","function":{"name":"w","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"tool_choice":"auto"}`,
		"non-ascii": `{"model":"m","messages":[{"role":"user","content":"Привет <b>мир</b> & 你好 é 😀   \"q\" \\ \n"}]}`,
		"content parts": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"http://x/y.png","detail":"low"}}]}]}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			for _, mode := range []string{"", "processed_logprobs"} {
				modified, err := ModifyRequestBodyWithLogprobsMode([]byte(body), 7, mode)
				require.NoError(t, err)

				var decoded map[string]interface{}
				require.NoError(t, json.Unmarshal(modified.NewBody, &decoded))
				require.Equal(t, decoded, modified.RequestMap)
			}
		})
	}
}
