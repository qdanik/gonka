package filters

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStripFields(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    string
		changed bool
	}{
		{name: "empty object", input: `{}`, want: `{}`},
		{name: "empty array", input: `[]`, want: `[]`},
		{name: "nothing to strip", input: `{"id":"c","choices":[]}`, want: `{"id":"c","choices":[]}`},
		{name: "only member stripped", input: `{"logprobs":{"a":1}}`, want: `{}`, changed: true},
		{name: "first of three", input: `{"logprobs":1,"a":2,"b":3}`, want: `{"a":2,"b":3}`, changed: true},
		{name: "last of three", input: `{"a":2,"b":3,"logprobs":1}`, want: `{"a":2,"b":3}`, changed: true},
		{name: "middle of three", input: `{"a":2,"logprobs":1,"b":3}`, want: `{"a":2,"b":3}`, changed: true},
		{name: "two adjacent stripped", input: `{"a":1,"logprob":2,"token_ids":3,"b":4}`, want: `{"a":1,"b":4}`, changed: true},
		{name: "nested deep", input: `{"choices":[{"delta":{"logprobs":9,"content":"hi"}}]}`, want: `{"choices":[{"delta":{"content":"hi"}}]}`, changed: true},
		{name: "inside an array of objects", input: `[{"logprobs":1},{"a":2}]`, want: `[{},{"a":2}]`, changed: true},
		{name: "field name as a string value is kept", input: `{"content":"logprobs"}`, want: `{"content":"logprobs"}`},
		{name: "field name inside prose is kept", input: `{"content":"what is a logprob"}`, want: `{"content":"what is a logprob"}`},
		{name: "escaped quote before a key", input: `{"a":"say \"logprobs\"","logprobs":1}`, want: `{"a":"say \"logprobs\""}`, changed: true},
		{name: "escaped key spelling is still stripped", input: `{"logpro\u0062":1,"a":2}`, want: `{"a":2}`, changed: true},
		{name: "escaped key that is not a stripped field is kept", input: `{"logpro\u0063":1}`, want: `{"logpro\u0063":1}`},
		{name: "large integer is exact", input: `{"seed":9007199254740993,"logprobs":1}`, want: `{"seed":9007199254740993}`, changed: true},
		{name: "float form is preserved", input: `{"x":1.50,"logprobs":1}`, want: `{"x":1.50}`, changed: true},
		{name: "exponent form is preserved", input: `{"x":1e10,"logprobs":1}`, want: `{"x":1e10}`, changed: true},
		{name: "literals", input: `{"a":true,"b":false,"c":null,"logprobs":1}`, want: `{"a":true,"b":false,"c":null}`, changed: true},
		{name: "key order is preserved", input: `{"z":1,"a":2,"m":3}`, want: `{"z":1,"a":2,"m":3}`},
		{name: "whitespace is compacted", input: "{\n  \"a\" : 1 ,\n  \"logprobs\" : 2\n}", want: `{"a":1}`, changed: true},
		{name: "unicode escape in a value is not rewritten", input: `{"a":"\u00e9","logprobs":1}`, want: `{"a":"\u00e9"}`, changed: true},
		{name: "raw multibyte value survives", input: `{"a":"é😀","logprobs":1}`, want: `{"a":"é😀"}`, changed: true},
		{name: "top level string", input: `"hello"`, want: `"hello"`},
		{name: "top level number", input: `42`, want: `42`},
		{name: "all six fields", input: `{"logprob":1,"logprobs":2,"top_logprobs":3,"token_ids":4,"prompt_token_ids":5,"prompt_logprobs":6,"keep":7}`, want: `{"keep":7}`, changed: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, changed, err := stripFields([]byte(testCase.input))

			if err != nil {
				t.Fatalf("stripFields(%s): %v", testCase.input, err)
			}
			if string(got) != testCase.want {
				t.Fatalf("stripFields(%s)\n got %s\nwant %s", testCase.input, got, testCase.want)
			}
			if changed != testCase.changed {
				t.Fatalf("stripFields(%s) changed = %v, want %v", testCase.input, changed, testCase.changed)
			}
			if !json.Valid(got) {
				t.Fatalf("stripFields(%s) produced invalid JSON: %s", testCase.input, got)
			}
		})
	}
}

func TestStripFieldsRejectsMalformedInput(t *testing.T) {
	malformed := []string{
		``,
		`{`,
		`}`,
		`{"a"}`,
		`{"a":}`,
		`{"a":1,}`,
		`{"a":1}{"b":2}`,
		`[1,2`,
		`{"a":"unterminated`,
		`{"logprobs":}`,
		`[,]`,
	}

	for _, input := range malformed {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			if _, _, err := stripFields([]byte(input)); err == nil {
				t.Fatalf("stripFields(%q) accepted malformed input", input)
			}
		})
	}
}

// deleteDecodedFields is the tree-walking strip the scanner replaced, kept here as an independent
// oracle: two implementations that agree are better evidence than one that passes its own tests.
func deleteDecodedFields(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for _, field := range clientStrippedFields {
			delete(typed, field)
		}
		for _, child := range typed {
			deleteDecodedFields(child)
		}
	case []any:
		for _, child := range typed {
			deleteDecodedFields(child)
		}
	}
}

// The scanner and a decode-then-encode round trip must agree on which fields survive, on bodies deep
// and wide enough that a comma or a nesting mistake would show up.
func TestStripFieldsAgreesWithADecodedRoundTrip(t *testing.T) {
	body := `{"id":"chat-1","object":"chat.completion","created":1700000000,` +
		`"prompt_token_ids":[1,2,3],"prompt_logprobs":null,` +
		`"choices":[{"index":0,"message":{"role":"assistant","content":"hi","token_ids":[7,8]},` +
		`"logprobs":{"content":[{"token":"hi","logprob":-0.5,"top_logprobs":[{"token":"h","logprob":-1.0}]}]},` +
		`"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1}}`

	got, changed, err := stripFields([]byte(body))
	if err != nil {
		t.Fatalf("stripFields: %v", err)
	}
	if !changed {
		t.Fatal("the body carried six stripped fields and the scanner reported no change")
	}
	for _, field := range clientStrippedFields {
		if strings.Contains(string(got), `"`+field+`"`) {
			t.Fatalf("field %q survived: %s", field, got)
		}
	}

	var scanned, decoded any
	if err := json.Unmarshal(got, &scanned); err != nil {
		t.Fatalf("scanner output is not valid JSON: %v", err)
	}
	var tree any
	if err := json.Unmarshal([]byte(body), &tree); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	deleteDecodedFields(tree)
	roundTripped, err := json.Marshal(tree)
	if err != nil {
		t.Fatalf("re-encoding the tree: %v", err)
	}
	if err := json.Unmarshal(roundTripped, &decoded); err != nil {
		t.Fatalf("round trip output is not valid JSON: %v", err)
	}
	if !jsonEqual(scanned, decoded) {
		t.Fatalf("scanner and round trip disagree:\n scanner %s\n roundtrip %s", got, roundTripped)
	}
}

func jsonEqual(left, right any) bool {
	leftEncoded, leftErr := json.Marshal(left)
	rightEncoded, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftEncoded) == string(rightEncoded)
}
