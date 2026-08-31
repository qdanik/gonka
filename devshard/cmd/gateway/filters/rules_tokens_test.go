package filters

import (
	"fmt"
	"net/http"
	"testing"

	"common/completionapi"
)

// Pins capOutputTokens's contract directly: 0 always means "unset, use default".
func TestTokensCapOutputTokens(t *testing.T) {
	limits := outputTokenLimits{DefaultMaxTokens: 100, MaxTokensCap: 200}
	tests := []struct {
		name        string
		value       uint64
		bypassLimit bool
		want        uint64
	}{
		{"zero value uses default", 0, false, 100},
		{"zero value uses default even under admin bypass", 0, true, 100},
		{"value under cap kept as-is", 150, false, 150},
		{"value over cap clamped", 300, false, 200},
		{"value over cap admin bypass keeps raw value", 300, true, 300},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := capOutputTokens(testCase.value, testCase.bypassLimit, limits)
			if got != testCase.want {
				t.Errorf("capOutputTokens(%d, %v) = %d, want %d", testCase.value, testCase.bypassLimit, got, testCase.want)
			}
		})
	}
}

// The deployed configuration pairs a 10000 default with a 4096 cap, and the two bound different
// things: the cap bounds what a client may ASK for, the default is what an operator grants a client
// that asks for nothing. Clamping the default to the cap would silently cut every unbounded request
// on the shipped template from 10000 tokens to 4096.
func TestTokensTheCapBoundsTheAskAndNotTheDefault(t *testing.T) {
	limits := outputTokenLimits{DefaultMaxTokens: 10_000, MaxTokensCap: 4_096}
	tests := []struct {
		name        string
		value       uint64
		bypassLimit bool
		want        uint64
	}{
		{"an unset request takes the operator's default in full", 0, false, 10_000},
		{"an admin's unset request takes it too", 0, true, 10_000},
		{"an explicit ask for the same number is still clamped", 10_000, false, 4_096},
		{"an admin's explicit ask bypasses the cap", 10_000, true, 10_000},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := capOutputTokens(testCase.value, testCase.bypassLimit, limits)
			if got != testCase.want {
				t.Errorf("capOutputTokens(%d, %v) = %d, want %d", testCase.value, testCase.bypassLimit, got, testCase.want)
			}
		})
	}
}

func TestTokensNormalizedLimitsFallBackWhenZero(t *testing.T) {
	got := normalizedOutputTokenLimits(outputTokenLimits{})
	want := outputTokenLimits{DefaultMaxTokens: DefaultRequestMaxTokens, MaxTokensCap: RequestMaxTokensCap}
	if got != want {
		t.Errorf("normalizedOutputTokenLimits({}) = %+v, want %+v", got, want)
	}
}

func TestTokensNormalizedLimitsKeepExplicitValues(t *testing.T) {
	got := normalizedOutputTokenLimits(outputTokenLimits{DefaultMaxTokens: 10, MaxTokensCap: 20})
	want := outputTokenLimits{DefaultMaxTokens: 10, MaxTokensCap: 20}
	if got != want {
		t.Errorf("normalizedOutputTokenLimits() = %+v, want %+v", got, want)
	}
}

// applyLimits runs the full decode+cap round trip so branch tests can assert on document and view.
func applyLimits(t *testing.T, body string, options Options) (*Document, requestView) {
	t.Helper()
	document, err := ParseDocument([]byte(body))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	view, err := decodeRequestView(document)
	if err != nil {
		t.Fatalf("decodeRequestView: %v", err)
	}
	applyOutputTokenLimits(document, &view, options, view.Model)
	return document, view
}

func TestTokensApplyLimitsNeitherSetUsesDefault(t *testing.T) {
	document, view := applyLimits(t, `{}`, Options{DefaultMaxTokens: 1000, MaxTokensCap: 2000})

	if view.MaxTokens != 1000 {
		t.Errorf("view.MaxTokens = %d, want 1000", view.MaxTokens)
	}
	if view.MaxCompletionTokens != 0 {
		t.Errorf("view.MaxCompletionTokens = %d, want 0", view.MaxCompletionTokens)
	}
	if got, _ := document.Get("max_tokens"); got != uint64(1000) {
		t.Errorf("document max_tokens = %v, want 1000", got)
	}
	if document.Has("max_completion_tokens") {
		t.Error("document must not gain max_completion_tokens when neither field was set")
	}
}

func TestTokensApplyLimitsOnlyMaxTokensSet(t *testing.T) {
	tests := []struct {
		name      string
		maxTokens uint64
		admin     bool
		want      uint64
	}{
		{"under cap kept as-is", 1500, false, 1500},
		{"over cap clamped", 3000, false, 2000},
		{"over cap admin bypasses", 3000, true, 3000},
		{"explicit zero treated as unset, uses default", 0, false, 1000},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"max_tokens":%d}`, testCase.maxTokens)
			document, view := applyLimits(t, body, Options{Admin: testCase.admin, DefaultMaxTokens: 1000, MaxTokensCap: 2000})

			if view.MaxTokens != testCase.want {
				t.Errorf("view.MaxTokens = %d, want %d", view.MaxTokens, testCase.want)
			}
			if view.MaxCompletionTokens != 0 {
				t.Errorf("view.MaxCompletionTokens = %d, want 0", view.MaxCompletionTokens)
			}
			if got, _ := document.Get("max_tokens"); got != testCase.want {
				t.Errorf("document max_tokens = %v, want %d", got, testCase.want)
			}
			if document.Has("max_completion_tokens") {
				t.Error("document must not gain max_completion_tokens in the max_tokens-only branch")
			}
		})
	}
}

func TestTokensApplyLimitsOnlyMaxCompletionTokensSet(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		admin bool
		want  uint64
	}{
		{"under cap kept as-is", 1500, false, 1500},
		{"over cap clamped", 3000, false, 2000},
		{"over cap admin bypasses", 3000, true, 3000},
		{"explicit zero treated as unset, uses default", 0, false, 1000},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"max_completion_tokens":%d}`, testCase.value)
			document, view := applyLimits(t, body, Options{Admin: testCase.admin, DefaultMaxTokens: 1000, MaxTokensCap: 2000})

			// max_completion_tokens-only mirrors into both fields.
			if view.MaxTokens != testCase.want {
				t.Errorf("view.MaxTokens = %d, want %d", view.MaxTokens, testCase.want)
			}
			if view.MaxCompletionTokens != testCase.want {
				t.Errorf("view.MaxCompletionTokens = %d, want %d", view.MaxCompletionTokens, testCase.want)
			}
			if got, _ := document.Get("max_tokens"); got != testCase.want {
				t.Errorf("document max_tokens (mirrored) = %v, want %d", got, testCase.want)
			}
			if got, _ := document.Get("max_completion_tokens"); got != testCase.want {
				t.Errorf("document max_completion_tokens = %v, want %d", got, testCase.want)
			}
		})
	}
}

func TestTokensApplyLimitsBothSet(t *testing.T) {
	tests := []struct {
		name                string
		maxTokens           uint64
		maxCompletionTokens uint64
		admin               bool
		want                uint64
	}{
		{"both under cap, completion smaller wins", 1800, 1200, false, 1200},
		{"both under cap, max_tokens smaller wins", 1200, 1800, false, 1200},
		{"both over cap collapse to the shared cap", 5000, 8000, false, 2000},
		{"admin bypass keeps the raw min uncapped", 5000, 8000, true, 5000},
		{"max_tokens explicit zero defaults before the min compare", 0, 5000, false, 1000},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"max_tokens":%d,"max_completion_tokens":%d}`, testCase.maxTokens, testCase.maxCompletionTokens)
			document, view := applyLimits(t, body, Options{Admin: testCase.admin, DefaultMaxTokens: 1000, MaxTokensCap: 2000})

			if view.MaxTokens != testCase.want {
				t.Errorf("view.MaxTokens = %d, want %d", view.MaxTokens, testCase.want)
			}
			if view.MaxCompletionTokens != testCase.want {
				t.Errorf("view.MaxCompletionTokens = %d, want %d", view.MaxCompletionTokens, testCase.want)
			}
			if got, _ := document.Get("max_tokens"); got != testCase.want {
				t.Errorf("document max_tokens = %v, want %d", got, testCase.want)
			}
			if got, _ := document.Get("max_completion_tokens"); got != testCase.want {
				t.Errorf("document max_completion_tokens = %v, want %d", got, testCase.want)
			}
		})
	}
}

func TestTokensDecodeRequestViewHappyPath(t *testing.T) {
	document, err := ParseDocument([]byte(`{"model":"qwen","stream":true,"max_tokens":10,"max_completion_tokens":20,"n":2}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	view, err := decodeRequestView(document)
	if err != nil {
		t.Fatalf("decodeRequestView: %v", err)
	}
	want := requestView{Model: "qwen", Stream: true, MaxTokens: 10, MaxCompletionTokens: 20, N: 2}
	if view != want {
		t.Errorf("decodeRequestView() = %+v, want %+v", view, want)
	}
}

func TestTokensDecodeRequestViewAbsentFieldsAreZeroValue(t *testing.T) {
	document, err := ParseDocument([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	view, err := decodeRequestView(document)
	if err != nil {
		t.Fatalf("decodeRequestView: %v", err)
	}
	if view != (requestView{}) {
		t.Errorf("decodeRequestView({}) = %+v, want zero value", view)
	}
}

// Pins the exact error text for each of the 5 typed fields.
func TestTokensDecodeRequestViewTypeErrors(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"model must be a string", `{"model":42}`, "parse request: model must be a string"},
		{"stream must be a boolean", `{"stream":"yes"}`, "parse request: stream must be a boolean"},
		{"max_tokens must be a non-negative integer", `{"max_tokens":"abc"}`, "parse request: max_tokens must be a non-negative integer"},
		{"max_completion_tokens must be a non-negative integer", `{"max_completion_tokens":-1}`, "parse request: max_completion_tokens must be a non-negative integer"},
		{"n must be a non-negative integer", `{"n":-1}`, "parse request: n must be a non-negative integer"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document, err := ParseDocument([]byte(testCase.body))
			if err != nil {
				t.Fatalf("ParseDocument: %v", err)
			}
			_, err = decodeRequestView(document)
			if err == nil {
				t.Fatal("decodeRequestView(): want error, got nil")
			}
			if err.Error() != testCase.wantErr {
				t.Errorf("decodeRequestView() error = %q, want %q", err.Error(), testCase.wantErr)
			}
			if got := ErrorStatus(err, 0); got != http.StatusBadRequest {
				t.Errorf("ErrorStatus() = %d, want %d", got, http.StatusBadRequest)
			}
		})
	}
}

func TestLiftNonPositiveOutputTokensRaisesAZeroToTheFloor(t *testing.T) {
	for _, param := range []string{"max_tokens", "max_completion_tokens"} {
		t.Run(param, func(t *testing.T) {
			document := parseTestDocument(t, `{"`+param+`":0}`)
			if err := liftNonPositiveOutputTokens()(RuleContext{Document: document, Param: param, Profile: kimiProfile}); err != nil {
				t.Fatalf("liftNonPositiveOutputTokens() = %v, want nil", err)
			}
			if got, _ := document.Uint(param); got != completionapi.MinTokensFloor {
				t.Errorf("%s = %d, want the floor %d", param, got, completionapi.MinTokensFloor)
			}
		})
	}
}

func TestLiftNonPositiveOutputTokensLeavesEveryOtherProfileToTheRefusal(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile, deepseekProfile} {
		document := parseTestDocument(t, `{"max_tokens":0}`)
		if err := liftNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_tokens", Profile: profile}); err != nil {
			t.Fatalf("liftNonPositiveOutputTokens() = %v, want nil", err)
		}
		if got, _ := document.Uint("max_tokens"); got != 0 {
			t.Errorf("max_tokens = %d for profile %v, want the zero left for the refusal", got, profile)
		}
	}
}

// The refusal stays absolute on every profile; a profile that wants its zero raised gets that from
// liftNonPositiveOutputTokens ahead of it, because capOutputTokens reads a zero as "unset" and would
// otherwise hand it the default rather than the floor.
func TestRejectNonPositiveOutputTokensRejectsZeroOnEveryProfile(t *testing.T) {
	for _, profile := range []*Profile{nil, kimiProfile, minimaxProfile, deepseekProfile} {
		document := parseTestDocument(t, `{"max_tokens":0}`)
		if err := rejectNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_tokens", Profile: profile}); err == nil {
			t.Errorf("rejectNonPositiveOutputTokens() = nil for profile %v, want a rejection", profile)
		}
	}
}

func TestRejectNonPositiveOutputTokensRejectsZeroAndNegative(t *testing.T) {
	for _, value := range []string{"0", "-1", "-100"} {
		t.Run(value, func(t *testing.T) {
			document := parseTestDocument(t, `{"max_tokens":`+value+`}`)
			err := rejectNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_tokens"})
			want := "max_tokens: must be greater than 0"
			if err == nil || err.Error() != want {
				t.Errorf("rejectNonPositiveOutputTokens() = %v, want %q", err, want)
			}
		})
	}
}

func TestRejectNonPositiveOutputTokensAcceptsPositive(t *testing.T) {
	document := parseTestDocument(t, `{"max_completion_tokens":5}`)
	err := rejectNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_completion_tokens"})
	if err != nil {
		t.Fatalf("rejectNonPositiveOutputTokens() = %v, want nil", err)
	}
}

func TestRejectNonPositiveOutputTokensAbsentIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{}`)
	if err := rejectNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_tokens"}); err != nil {
		t.Fatalf("rejectNonPositiveOutputTokens() = %v, want nil", err)
	}
}

func TestRejectNonPositiveOutputTokensNonNumericIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":"abc"}`)
	if err := rejectNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_tokens"}); err != nil {
		t.Fatalf("rejectNonPositiveOutputTokens() = %v, want nil (type-checked elsewhere)", err)
	}
}

func TestTokensPerModelOverrideBeatsTheGlobalDefaultAndCap(t *testing.T) {
	options := Options{
		DefaultMaxTokens: 1000,
		MaxTokensCap:     2000,
		ModelTokenLimits: func(model string) (uint64, uint64) {
			if model == "qwen" {
				return 5_000, 8_000
			}
			return 0, 0
		},
	}

	_, overridden := applyLimits(t, `{"model":"qwen"}`, options)
	_, global := applyLimits(t, `{"model":"other"}`, options)

	if overridden.MaxTokens != 5_000 {
		t.Errorf("qwen default = %d, want the per-model 5000", overridden.MaxTokens)
	}
	if global.MaxTokens != 1000 {
		t.Errorf("unlisted model default = %d, want the global 1000", global.MaxTokens)
	}
}

func TestTokensPerModelOverrideCapClampsTheRequestedValue(t *testing.T) {
	options := Options{
		DefaultMaxTokens: 1000,
		MaxTokensCap:     2000,
		ModelTokenLimits: func(string) (uint64, uint64) { return 0, 1500 },
	}

	_, view := applyLimits(t, `{"model":"qwen","max_tokens":1800}`, options)

	if view.MaxTokens != 1500 {
		t.Errorf("max_tokens = %d, want the per-model cap 1500", view.MaxTokens)
	}
}

// A cap under the floor buys a budget too small to reason in, which the floor exists to prevent, so
// the floor wins and the operator's cap is the one that gives way.
func TestTokensCapBelowTheFloorLosesToIt(t *testing.T) {
	options := Options{
		DefaultMaxTokens: 1000,
		MaxTokensCap:     2000,
		ModelTokenLimits: func(string) (uint64, uint64) { return 0, 50 },
	}

	_, view := applyLimits(t, `{"model":"qwen","max_tokens":1800}`, options)

	if view.MaxTokens != completionapi.MinTokensFloor {
		t.Errorf("max_tokens = %d, want the floor %d", view.MaxTokens, completionapi.MinTokensFloor)
	}
}

// Every route, not just a profile that declares its own floor: MiniMax and the default profile have
// none, and a reservation under the floor is refused for them exactly the same.
func TestTokensFloorAppliesToEveryRoute(t *testing.T) {
	for _, model := range []string{kimiModelID, minimaxModelID, "Qwen/Test"} {
		t.Run(model, func(t *testing.T) {
			result, err := NormalizeRequest([]byte(`{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`),
				Options{RoutedModel: model, DefaultMaxTokens: 3072, MaxTokensCap: 4096})
			if err != nil {
				t.Fatalf("NormalizeRequest() = %v, want nil", err)
			}
			if result.MaxTokens < completionapi.MinTokensFloor {
				t.Errorf("MaxTokens = %d, below the floor %d the chain refuses", result.MaxTokens, completionapi.MinTokensFloor)
			}
		})
	}
}
