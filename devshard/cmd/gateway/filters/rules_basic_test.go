package filters

import (
	"encoding/json"
	"strings"
	"testing"
)

func parseTestDocument(t *testing.T, body string) *Document {
	t.Helper()
	document, err := ParseDocument([]byte(body))
	if err != nil {
		t.Fatalf("ParseDocument(%s): %v", body, err)
	}
	return document
}

func getFloat64(t *testing.T, document *Document, param string) float64 {
	t.Helper()
	raw, ok := document.Get(param)
	if !ok {
		t.Fatalf("Get(%s): field missing, want a float64", param)
	}
	value, ok := raw.(float64)
	if !ok {
		t.Fatalf("Get(%s) = %T(%v), want float64", param, raw, raw)
	}
	return value
}

func TestBasicRequireUint(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"absent passes through", `{}`, ""},
		{"null passes through", `{"seed":null}`, ""},
		{"zero accepted", `{"seed":0}`, ""},
		{"positive integer accepted", `{"seed":42}`, ""},
		{"negative rejected", `{"seed":-5}`, "seed: must be a non-negative integer"},
		{"string encoded rejected", `{"seed":"42"}`, "seed: must be a non-negative integer"},
		{"boolean rejected", `{"seed":true}`, "seed: must be a non-negative integer"},
		{"fractional number rejected", `{"seed":1.5}`, "seed: must be a non-negative integer"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := requireUint()(RuleContext{Document: document, Param: "seed"})
			if testCase.wantErr == "" {
				if err != nil {
					t.Fatalf("requireUint() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("requireUint() = nil, want error")
			}
			if err.Error() != testCase.wantErr {
				t.Errorf("requireUint() error = %q, want %q", err.Error(), testCase.wantErr)
			}
			if got := ErrorStatus(err, 0); got != 400 {
				t.Errorf("ErrorStatus() = %d, want 400", got)
			}
		})
	}
}

func TestBasicRequireBool(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		body    string
		wantErr string
	}{
		{"absent passes through", "skip_special_tokens", `{}`, ""},
		{"null passes through", "skip_special_tokens", `{"skip_special_tokens":null}`, ""},
		{"true accepted", "skip_special_tokens", `{"skip_special_tokens":true}`, ""},
		{"false accepted", "skip_special_tokens", `{"skip_special_tokens":false}`, ""},
		{"string rejected", "skip_special_tokens", `{"skip_special_tokens":"yes"}`, "skip_special_tokens: must be a boolean"},
		{"number rejected", "detokenize", `{"detokenize":1}`, "detokenize: must be a boolean"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := requireBool()(RuleContext{Document: document, Param: testCase.field})
			if testCase.wantErr == "" {
				if err != nil {
					t.Fatalf("requireBool() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("requireBool() = nil, want error")
			}
			if err.Error() != testCase.wantErr {
				t.Errorf("requireBool() error = %q, want %q", err.Error(), testCase.wantErr)
			}
		})
	}
}

func TestBasicRequireString(t *testing.T) {
	tests := []struct {
		name     string
		param    string
		maxBytes int
		body     string
		wantErr  string
	}{
		{"absent passes through", "note", 10, `{}`, ""},
		{"exactly at cap accepted", "note", 10, `{"note":"1234567890"}`, ""},
		{"one over cap rejected", "note", 10, `{"note":"12345678901"}`, "note: length exceeded: 11 > 10"},
		{"non string type rejected", "note", 10, `{"note":42}`, "note: invalid wrapper shape: must be a string"},
		{"null rejected, not treated as absent", "note", 10, `{"note":null}`, "note: invalid wrapper shape: must be a string"},
		{"user field pins the production bound", "user", 512, `{"user":"` + strings.Repeat("x", 513) + `"}`, "user: length exceeded: 513 > 512"},
		{"user field accepts a normal value", "user", 512, `{"user":"alice"}`, ""},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := requireString(testCase.maxBytes)(RuleContext{Document: document, Param: testCase.param})
			if testCase.wantErr == "" {
				if err != nil {
					t.Fatalf("requireString() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("requireString() = nil, want error")
			}
			if err.Error() != testCase.wantErr {
				t.Errorf("requireString() error = %q, want %q", err.Error(), testCase.wantErr)
			}
		})
	}
}

func TestBasicStripParameter(t *testing.T) {
	tests := []struct {
		name  string
		param string
		body  string
	}{
		{"present scalar deleted", "service_tier", `{"service_tier":"flex"}`},
		{"present object deleted", "provider", `{"provider":{"order":["openai"]}}`},
		{"absent is a no-op", "store", `{}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := stripParameter()(RuleContext{Document: document, Param: testCase.param}); err != nil {
				t.Fatalf("stripParameter() = %v, want nil", err)
			}
			if document.Has(testCase.param) {
				t.Errorf("Has(%s) = true after strip, want false", testCase.param)
			}
		})
	}
}

func TestBasicForceLiteral(t *testing.T) {
	tests := []struct {
		name  string
		param string
		value any
		body  string
	}{
		{"overwrites a present false value", "logprobs", true, `{"logprobs":false}`},
		{"sets on an absent field", "logprobs", true, `{}`},
		{"overwrites a present out-of-range int", "top_logprobs", 5, `{"top_logprobs":20}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := forceLiteral(testCase.value)(RuleContext{Document: document, Param: testCase.param}); err != nil {
				t.Fatalf("forceLiteral() = %v, want nil", err)
			}
			got, ok := document.Get(testCase.param)
			if !ok {
				t.Fatalf("Get(%s) after forceLiteral: missing", testCase.param)
			}
			if got != testCase.value {
				t.Errorf("Get(%s) = %v, want %v", testCase.param, got, testCase.value)
			}
		})
	}
}

func TestBasicClampFloat(t *testing.T) {
	const param = "temperature"
	tests := []struct {
		name       string
		body       string
		wantStrip  bool
		wantResult float64
	}{
		{"within range integer-shaped", `{"temperature":1}`, false, 1},
		{"within range string encoded", `{"temperature":"1.2"}`, false, 1.2},
		{"below min clamps to min", `{"temperature":-1}`, false, 0},
		{"above max clamps to max", `{"temperature":999999}`, false, 2},
		{"exactly at min boundary kept", `{"temperature":0}`, false, 0},
		{"exactly at max boundary kept", `{"temperature":2}`, false, 2},
		{"non-finite nan stripped", `{"temperature":"nan"}`, true, 0},
		{"non-finite infinity stripped", `{"temperature":"infinity"}`, true, 0},
		{"unparseable type stripped", `{"temperature":true}`, true, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := clampFloat(0, 2)(RuleContext{Document: document, Param: param}); err != nil {
				t.Fatalf("clampFloat() = %v, want nil", err)
			}
			if testCase.wantStrip {
				if document.Has(param) {
					t.Errorf("Has(%s) = true, want stripped", param)
				}
				return
			}
			if got := getFloat64(t, document, param); got != testCase.wantResult {
				t.Errorf("Get(%s) = %v, want %v", param, got, testCase.wantResult)
			}
		})
	}
	t.Run("absent is a no-op", func(t *testing.T) {
		document := parseTestDocument(t, `{}`)
		if err := clampFloat(0, 2)(RuleContext{Document: document, Param: param}); err != nil {
			t.Fatalf("clampFloat() = %v, want nil", err)
		}
		if document.Has(param) {
			t.Error("Has(temperature) = true, want absent to stay absent")
		}
	})
}

func TestBasicRejectNonPositiveThenClamp(t *testing.T) {
	const param = "repetition_penalty"
	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStrip  bool
		wantResult float64
	}{
		{"within range kept", `{"repetition_penalty":1.5}`, "", false, 1.5},
		{"exactly at max kept", `{"repetition_penalty":2}`, "", false, 2},
		{"above max string encoded clamps to max", `{"repetition_penalty":"5.0"}`, "", false, 2},
		{"zero rejected", `{"repetition_penalty":0}`, "repetition_penalty: must be greater than 0", false, 0},
		{"negative rejected", `{"repetition_penalty":-1}`, "repetition_penalty: must be greater than 0", false, 0},
		{"non-finite nan stripped not rejected", `{"repetition_penalty":"nan"}`, "", true, 0},
		{"unparseable type stripped not rejected", `{"repetition_penalty":[1]}`, "", true, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := rejectNonPositiveThenClamp(2)(RuleContext{Document: document, Param: param})
			if testCase.wantErr != "" {
				if err == nil {
					t.Fatal("rejectNonPositiveThenClamp() = nil, want error")
				}
				if err.Error() != testCase.wantErr {
					t.Errorf("error = %q, want %q", err.Error(), testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("rejectNonPositiveThenClamp() = %v, want nil", err)
			}
			if testCase.wantStrip {
				if document.Has(param) {
					t.Errorf("Has(%s) = true, want stripped", param)
				}
				return
			}
			if got := getFloat64(t, document, param); got != testCase.wantResult {
				t.Errorf("Get(%s) = %v, want %v", param, got, testCase.wantResult)
			}
		})
	}
	t.Run("absent is a no-op", func(t *testing.T) {
		document := parseTestDocument(t, `{}`)
		if err := rejectNonPositiveThenClamp(2)(RuleContext{Document: document, Param: param}); err != nil {
			t.Fatalf("rejectNonPositiveThenClamp() = %v, want nil", err)
		}
		if document.Has(param) {
			t.Error("Has(repetition_penalty) = true, want absent to stay absent")
		}
	})
}

func TestBasicValidTopK(t *testing.T) {
	const param = "top_k"
	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStrip  bool
		wantResult float64
	}{
		{"negative one disabled sentinel accepted", `{"top_k":-1}`, "", false, -1},
		{"exactly one accepted", `{"top_k":1}`, "", false, 1},
		{"large positive accepted", `{"top_k":50}`, "", false, 50},
		{"fractional above one accepted despite non-integer", `{"top_k":1.5}`, "", false, 1.5},
		{"zero rejected", `{"top_k":0}`, "top_k: must be -1 or a positive integer", false, 0},
		{"fraction between zero and one rejected", `{"top_k":0.5}`, "top_k: must be -1 or a positive integer", false, 0},
		{"negative two rejected", `{"top_k":-2}`, "top_k: must be -1 or a positive integer", false, 0},
		{"non-finite nan stripped not rejected", `{"top_k":"nan"}`, "", true, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := validTopK()(RuleContext{Document: document, Param: param})
			if testCase.wantErr != "" {
				if err == nil {
					t.Fatal("validTopK() = nil, want error")
				}
				if err.Error() != testCase.wantErr {
					t.Errorf("error = %q, want %q", err.Error(), testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validTopK() = %v, want nil", err)
			}
			if testCase.wantStrip {
				if document.Has(param) {
					t.Errorf("Has(%s) = true, want stripped", param)
				}
				return
			}
			if got := getFloat64(t, document, param); got != testCase.wantResult {
				t.Errorf("Get(%s) = %v, want %v", param, got, testCase.wantResult)
			}
		})
	}
	t.Run("absent is a no-op", func(t *testing.T) {
		document := parseTestDocument(t, `{}`)
		if err := validTopK()(RuleContext{Document: document, Param: param}); err != nil {
			t.Fatalf("validTopK() = %v, want nil", err)
		}
		if document.Has(param) {
			t.Error("Has(top_k) = true, want absent to stay absent")
		}
	})
}

func TestBasicCapUint(t *testing.T) {
	const param = "n"
	tests := []struct {
		name string
		body string
		want any
	}{
		{"below min clamps to min", `{"n":0}`, uint64(1)},
		{"above max clamps to max", `{"n":1638400}`, uint64(5)},
		{"exactly at min kept as original representation", `{"n":1}`, json.Number("1")},
		{"exactly at max kept as original representation", `{"n":5}`, json.Number("5")},
		{"within range kept as original representation", `{"n":3}`, json.Number("3")},
		{"non-numeric type passes through untouched", `{"n":"abc"}`, "abc"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := capUint(1, 5)(RuleContext{Document: document, Param: param}); err != nil {
				t.Fatalf("capUint() = %v, want nil", err)
			}
			got, ok := document.Get(param)
			if !ok {
				t.Fatalf("Get(%s): missing", param)
			}
			if got != testCase.want {
				t.Errorf("Get(%s) = %#v, want %#v", param, got, testCase.want)
			}
		})
	}
	t.Run("absent is a no-op", func(t *testing.T) {
		document := parseTestDocument(t, `{}`)
		if err := capUint(1, 5)(RuleContext{Document: document, Param: param}); err != nil {
			t.Fatalf("capUint() = %v, want nil", err)
		}
		if document.Has(param) {
			t.Error("Has(n) = true, want absent to stay absent")
		}
	})
}
