package filters

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
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
	document, view := applyLimits(t, `{}`, Options{DefaultMaxTokens: 100, MaxTokensCap: 200})

	if view.MaxTokens != 100 {
		t.Errorf("view.MaxTokens = %d, want 100", view.MaxTokens)
	}
	if view.MaxCompletionTokens != 0 {
		t.Errorf("view.MaxCompletionTokens = %d, want 0", view.MaxCompletionTokens)
	}
	if got, _ := document.Get("max_tokens"); got != uint64(100) {
		t.Errorf("document max_tokens = %v, want 100", got)
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
		{"under cap kept as-is", 150, false, 150},
		{"over cap clamped", 300, false, 200},
		{"over cap admin bypasses", 300, true, 300},
		{"explicit zero treated as unset, uses default", 0, false, 100},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"max_tokens":%d}`, testCase.maxTokens)
			document, view := applyLimits(t, body, Options{Admin: testCase.admin, DefaultMaxTokens: 100, MaxTokensCap: 200})

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
		{"under cap kept as-is", 150, false, 150},
		{"over cap clamped", 300, false, 200},
		{"over cap admin bypasses", 300, true, 300},
		{"explicit zero treated as unset, uses default", 0, false, 100},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"max_completion_tokens":%d}`, testCase.value)
			document, view := applyLimits(t, body, Options{Admin: testCase.admin, DefaultMaxTokens: 100, MaxTokensCap: 200})

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
		want                uint64 // both fields always converge to the same value
	}{
		{"both under cap, completion smaller wins", 180, 120, false, 120},
		{"both under cap, max_tokens smaller wins", 120, 180, false, 120},
		{"both over cap collapse to the shared cap", 500, 800, false, 200},
		{"admin bypass keeps the raw min uncapped", 500, 800, true, 500},
		{"max_tokens explicit zero defaults before the min compare", 0, 500, false, 100},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"max_tokens":%d,"max_completion_tokens":%d}`, testCase.maxTokens, testCase.maxCompletionTokens)
			document, view := applyLimits(t, body, Options{Admin: testCase.admin, DefaultMaxTokens: 100, MaxTokensCap: 200})

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

func TestTokensClampUintToField(t *testing.T) {
	const param = "min_tokens"
	tests := []struct {
		name string
		body string
		want any
	}{
		{"under limit kept as original representation", `{"min_tokens":10,"max_tokens":20}`, json.Number("10")},
		{"exactly at limit kept as original representation", `{"min_tokens":20,"max_tokens":20}`, json.Number("20")},
		{"over limit clamped to limit", `{"min_tokens":30,"max_tokens":20}`, uint64(20)},
		{"limit field absent disables clamp", `{"min_tokens":30}`, json.Number("30")},
		{"limit field zero disables clamp", `{"min_tokens":30,"max_tokens":0}`, json.Number("30")},
		{"limit field non-numeric disables clamp", `{"min_tokens":30,"max_tokens":"abc"}`, json.Number("30")},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := clampUintToField("max_tokens")(RuleContext{Document: document, Param: param}); err != nil {
				t.Fatalf("clampUintToField() = %v, want nil", err)
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
	t.Run("absent param is a no-op", func(t *testing.T) {
		document := parseTestDocument(t, `{"max_tokens":20}`)
		if err := clampUintToField("max_tokens")(RuleContext{Document: document, Param: param}); err != nil {
			t.Fatalf("clampUintToField() = %v, want nil", err)
		}
		if document.Has(param) {
			t.Error("Has(min_tokens) = true, want absent to stay absent")
		}
	})
	t.Run("non-numeric param is a no-op", func(t *testing.T) {
		document := parseTestDocument(t, `{"min_tokens":"abc","max_tokens":20}`)
		if err := clampUintToField("max_tokens")(RuleContext{Document: document, Param: param}); err != nil {
			t.Fatalf("clampUintToField() = %v, want nil", err)
		}
		if got, _ := document.Get(param); got != "abc" {
			t.Errorf("Get(%s) = %v, want unchanged", param, got)
		}
	})
}

func TestTokensStripWhenFieldPresent(t *testing.T) {
	const param = "min_tokens"
	tests := []struct {
		name     string
		body     string
		wantDrop bool
	}{
		{"trigger field present deletes param", `{"min_tokens":1,"stop_token_ids":[1]}`, true},
		{"trigger field absent keeps param", `{"min_tokens":1}`, false},
		{"trigger field present, param already absent stays absent", `{"stop_token_ids":[1]}`, true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := stripWhenFieldPresent("stop_token_ids")(RuleContext{Document: document, Param: param}); err != nil {
				t.Fatalf("stripWhenFieldPresent() = %v, want nil", err)
			}
			if document.Has(param) != !testCase.wantDrop {
				t.Errorf("Has(%s) = %v, want %v", param, document.Has(param), !testCase.wantDrop)
			}
		})
	}
}

func TestTokensGreedySamplingForceOne(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantN    any
		wantNSet bool
	}{
		{"n above one, temperature json.Number zero forces one", `{"n":5,"temperature":0}`, uint64(1), true},
		{"n above one, temperature string zero forces one", `{"n":5,"temperature":"0"}`, uint64(1), true},
		{"n above one, temperature string zero-point-zero forces one", `{"n":5,"temperature":"0.0"}`, uint64(1), true},
		{"n above one, temperature nonzero leaves n untouched", `{"n":5,"temperature":0.7}`, json.Number("5"), true},
		{"n already one leaves n untouched", `{"n":1,"temperature":0}`, json.Number("1"), true},
		{"n absent leaves document without n", `{"temperature":0}`, nil, false},
		{"temperature absent leaves n untouched", `{"n":5}`, json.Number("5"), true},
		{"temperature non-numeric leaves n untouched", `{"n":5,"temperature":true}`, json.Number("5"), true},
		{"n non-numeric leaves n untouched", `{"n":"many","temperature":0}`, "many", true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := greedySamplingForceOne()(RuleContext{Document: document, Param: "n"}); err != nil {
				t.Fatalf("greedySamplingForceOne() = %v, want nil", err)
			}
			got, ok := document.Get("n")
			if ok != testCase.wantNSet {
				t.Fatalf("Has(n) = %v, want %v", ok, testCase.wantNSet)
			}
			if !testCase.wantNSet {
				return
			}
			if got != testCase.wantN {
				t.Errorf("Get(n) = %#v, want %#v", got, testCase.wantN)
			}
		})
	}
}

// Proves greedy sampling reads n AFTER its own cap has already clamped it: n=100 first
// clamps to the [1,5] cap, then temperature==0 forces the capped value down to 1.
func TestTokensGreedySamplingCapInterplay(t *testing.T) {
	document := parseTestDocument(t, `{"n":100,"temperature":0}`)
	ctx := RuleContext{Document: document, Param: "n"}
	if err := capUint(minChatChoices, maxChatChoices)(ctx); err != nil {
		t.Fatalf("capUint() = %v, want nil", err)
	}
	if got, _ := document.Get("n"); got != uint64(5) {
		t.Fatalf("Get(n) after capUint = %v, want 5 (cap applied before greedy)", got)
	}
	if err := greedySamplingForceOne()(ctx); err != nil {
		t.Fatalf("greedySamplingForceOne() = %v, want nil", err)
	}
	if got, _ := document.Get("n"); got != uint64(1) {
		t.Errorf("Get(n) after greedy = %v, want 1 (greedy overrides the cap)", got)
	}
}

func TestMaxTokensFloorLiftsBelowFloorForHookProfile(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":1}`)
	if err := maxTokensFloor()(RuleContext{Document: document, Param: "max_tokens", Profile: kimiProfile}); err != nil {
		t.Fatalf("maxTokensFloor() = %v, want nil", err)
	}
	if got, _ := document.Get("max_tokens"); got != uint64(16) {
		t.Errorf("max_tokens = %v, want 16", got)
	}
}

// 15 is below the floor and gets lifted; 16 is exactly at the floor and passes through untouched.
func TestMaxTokensFloorBoundary(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  any
	}{
		{"one below floor lifted", "15", uint64(16)},
		{"exactly at floor kept as original representation", "16", json.Number("16")},
		{"above floor kept as original representation", "100", json.Number("100")},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, `{"max_tokens":`+testCase.value+`}`)
			if err := maxTokensFloor()(RuleContext{Document: document, Param: "max_tokens", Profile: kimiProfile}); err != nil {
				t.Fatalf("maxTokensFloor() = %v, want nil", err)
			}
			if got, _ := document.Get("max_tokens"); got != testCase.want {
				t.Errorf("max_tokens = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestMaxTokensFloorLiftsMaxCompletionTokensIndependently(t *testing.T) {
	document := parseTestDocument(t, `{"max_completion_tokens":8}`)
	if err := maxTokensFloor()(RuleContext{Document: document, Param: "max_completion_tokens", Profile: kimiProfile}); err != nil {
		t.Fatalf("maxTokensFloor() = %v, want nil", err)
	}
	if got, _ := document.Get("max_completion_tokens"); got != uint64(16) {
		t.Errorf("max_completion_tokens = %v, want 16", got)
	}
}

func TestMaxTokensFloorNoOpWithoutHook(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"max_tokens":1}`)
		if err := maxTokensFloor()(RuleContext{Document: document, Param: "max_tokens", Profile: profile}); err != nil {
			t.Fatalf("maxTokensFloor() = %v, want nil", err)
		}
		if got, _ := document.Get("max_tokens"); got != json.Number("1") {
			t.Errorf("max_tokens = %v, want unchanged for profile %v", got, profile)
		}
	}
}

func TestMaxTokensFloorSkipsMissingField(t *testing.T) {
	document := parseTestDocument(t, `{}`)
	if err := maxTokensFloor()(RuleContext{Document: document, Param: "max_tokens", Profile: kimiProfile}); err != nil {
		t.Fatalf("maxTokensFloor() = %v, want nil", err)
	}
	if document.Has("max_tokens") {
		t.Error("maxTokensFloor must not create the field when absent")
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

func TestRejectNonPositiveOutputTokensSkippedForHookProfile(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":0}`)
	err := rejectNonPositiveOutputTokens()(RuleContext{Document: document, Param: "max_tokens", Profile: kimiProfile})
	if err != nil {
		t.Fatalf("rejectNonPositiveOutputTokens() = %v, want nil (kimi normalizes instead of rejecting)", err)
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
		DefaultMaxTokens: 100,
		MaxTokensCap:     200,
		ModelTokenLimits: func(model string) (uint64, uint64) {
			if model == "qwen" {
				return 1_000, 2_000
			}
			return 0, 0
		},
	}

	_, overridden := applyLimits(t, `{"model":"qwen"}`, options)
	_, global := applyLimits(t, `{"model":"other"}`, options)

	if overridden.MaxTokens != 1_000 {
		t.Errorf("qwen default = %d, want the per-model 1000", overridden.MaxTokens)
	}
	if global.MaxTokens != 100 {
		t.Errorf("unlisted model default = %d, want the global 100", global.MaxTokens)
	}
}

func TestTokensPerModelOverrideCapClampsTheRequestedValue(t *testing.T) {
	options := Options{
		DefaultMaxTokens: 100,
		MaxTokensCap:     200,
		ModelTokenLimits: func(string) (uint64, uint64) { return 0, 50 },
	}

	_, view := applyLimits(t, `{"model":"qwen","max_tokens":180}`, options)

	if view.MaxTokens != 50 {
		t.Errorf("max_tokens = %d, want the per-model cap 50", view.MaxTokens)
	}
}

func TestHalveMaxTokensRewritesTheFieldsTheBodyCarries(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		maxTokens   uint64
		routedModel string
		wantBody    string
		wantTokens  uint64
	}{
		{
			name:       "max_tokens alone is halved",
			body:       `{"max_tokens":800,"model":"model-a"}`,
			maxTokens:  800,
			wantBody:   `{"max_tokens":400,"model":"model-a"}`,
			wantTokens: 400,
		},
		{
			name:       "both fields move together",
			body:       `{"max_completion_tokens":800,"max_tokens":800}`,
			maxTokens:  800,
			wantBody:   `{"max_completion_tokens":400,"max_tokens":400}`,
			wantTokens: 400,
		},
		{
			name:       "max_completion_tokens alone leaves max_tokens absent",
			body:       `{"max_completion_tokens":800}`,
			maxTokens:  800,
			wantBody:   `{"max_completion_tokens":400}`,
			wantTokens: 400,
		},
		{
			name:       "a body carrying neither field still gets a bounded budget",
			body:       `{"model":"model-a"}`,
			maxTokens:  800,
			wantBody:   `{"max_tokens":400,"model":"model-a"}`,
			wantTokens: 400,
		},
		{
			name:       "an odd budget rounds down",
			body:       `{"max_tokens":9}`,
			maxTokens:  9,
			wantBody:   `{"max_tokens":4}`,
			wantTokens: 4,
		},
		{
			name:       "two is the smallest budget that can be halved",
			body:       `{"max_tokens":2}`,
			maxTokens:  2,
			wantBody:   `{"max_tokens":1}`,
			wantTokens: 1,
		},
		{
			name:        "a profile floor stops the halving short",
			body:        `{"max_tokens":20,"model":"` + kimiModelID + `"}`,
			maxTokens:   20,
			routedModel: kimiModelID,
			wantBody:    `{"max_tokens":16,"model":"` + kimiModelID + `"}`,
			wantTokens:  16,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body, maxTokens, ok := HalveMaxTokens([]byte(testCase.body), testCase.maxTokens, testCase.routedModel)
			if !ok {
				t.Fatalf("HalveMaxTokens(%s, %d) reported no budget to halve", testCase.body, testCase.maxTokens)
			}
			if string(body) != testCase.wantBody {
				t.Errorf("body = %s, want %s", body, testCase.wantBody)
			}
			if maxTokens != testCase.wantTokens {
				t.Errorf("max_tokens = %d, want %d", maxTokens, testCase.wantTokens)
			}
		})
	}
}

func TestHalveMaxTokensRefusesABudgetWithNothingLeftToGiveBack(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		maxTokens   uint64
		routedModel string
	}{
		{name: "an unset budget", body: `{"max_tokens":0}`, maxTokens: 0},
		{name: "a single token", body: `{"max_tokens":1}`, maxTokens: 1},
		{
			name:        "a budget already at the profile floor",
			body:        `{"max_tokens":16}`,
			maxTokens:   16,
			routedModel: kimiModelID,
		},
		{
			name:        "a budget below the profile floor",
			body:        `{"max_tokens":10}`,
			maxTokens:   10,
			routedModel: kimiModelID,
		},
		{name: "a body that is not JSON", body: `not json`, maxTokens: 800},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body, maxTokens, ok := HalveMaxTokens([]byte(testCase.body), testCase.maxTokens, testCase.routedModel)
			if ok {
				t.Fatalf("HalveMaxTokens = (%s, %d, true), want a refusal", body, maxTokens)
			}
		})
	}
}
