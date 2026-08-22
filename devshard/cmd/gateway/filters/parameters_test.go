package filters

import (
	"strings"
	"testing"
)

// The conversation is the one part of a request a log must not repeat, and it is also the part that
// would bury every parameter beside it.
func TestParametersAreRenderedWithoutTheConversation(t *testing.T) {
	body := []byte(`{"model":"kimi","messages":[{"role":"user","content":"a secret"}],` +
		`"max_tokens":4096,"stream":true,"temperature":0.7,"stream_options":{"include_usage":true}}`)

	rendered := ParametersOf(body)

	if strings.Contains(rendered, "secret") || strings.Contains(rendered, "messages") {
		t.Fatalf("rendered %q, want the conversation left out", rendered)
	}
	for _, parameter := range []string{`"max_tokens":4096`, `"stream":true`, `"temperature":0.7`, `"model":"kimi"`} {
		if !strings.Contains(rendered, parameter) {
			t.Errorf("rendered %q, want it to carry %s", rendered, parameter)
		}
	}
}

// A tools array has no size the client has to respect, so the line stops instead of growing with it.
func TestAnOversizedParameterStopsTheLineRatherThanGrowingIt(t *testing.T) {
	body := []byte(`{"model":"kimi","tools":["` + strings.Repeat("t", maxParameterBytes*2) + `"]}`)

	rendered := ParametersOf(body)

	if len(rendered) > maxParameterBytes+8 {
		t.Fatalf("rendered %d bytes, want it bounded near the %d-byte cap", len(rendered), maxParameterBytes)
	}
}

func TestABodyThatIsNotAnObjectRendersNothing(t *testing.T) {
	if rendered := ParametersOf([]byte(`["not-an-object"]`)); rendered != "" {
		t.Fatalf("rendered %q, want nothing", rendered)
	}
}
