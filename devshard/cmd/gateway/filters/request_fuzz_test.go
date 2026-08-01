package filters

import (
	"encoding/json"
	"testing"
)

// NormalizeRequest is the only place an untrusted client body reaches parsing, and whatever it emits is
// what a host is asked to run. Three properties have to hold for any input at all: it must not panic,
// an accepted body must still be valid JSON, and it must never forward a parameter the gateway forces
// for its own accounting -- a client that sets those decides what the network validates.
func FuzzNormalizeRequestHoldsItsContract(f *testing.F) {
	for _, seed := range []string{
		`{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","messages":[],"stream":true,"max_tokens":10}`,
		`{"model":"m","messages":[],"logprobs":false,"top_logprobs":0}`,
		`{"model":"m","messages":[],"structured_outputs":{"regex":"a+"}}`,
		`{"model":"m","messages":[],"n":3,"seed":9007199254740993}`,
		`{"model":"m","messages":[],"chat_template_kwargs":{"chat_template":"{{evil}}"}}`,
		`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"x"}]}]}`,
		`{}`, `[]`, `"x"`, `{"model":1}`, `{"messages":"no"}`,
	} {
		f.Add(seed)
	}

	options := Options{DefaultMaxTokens: 100, MaxTokensCap: 1000}

	f.Fuzz(func(t *testing.T, body string) {
		result, err := NormalizeRequest([]byte(body), options)
		if err != nil {
			return // a rejection is a valid outcome for anything a client sends
		}
		if !json.Valid(result.Body) {
			t.Fatalf("accepted %q and produced invalid JSON: %s", body, result.Body)
		}
		var decoded map[string]any
		if err := json.Unmarshal(result.Body, &decoded); err != nil {
			t.Fatalf("accepted body does not decode as an object: %s: %v", result.Body, err)
		}
		// The gateway forces these upstream for validation; a client-supplied value reaching a host
		// would let the caller choose what the network can check its own inference against.
		for field, forced := range map[string]any{"logprobs": true, "return_token_ids": true} {
			if got, held := decoded[field]; held && got != forced {
				t.Fatalf("client kept control of %q: got %v, want the forced %v, from %q", field, got, forced, body)
			}
		}
	})
}
