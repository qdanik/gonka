package filters

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestReasoningWrapperLiftsEffort(t *testing.T) {
	document := parseTestDocument(t, `{"reasoning":{"effort":"high"}}`)
	if err := reasoningWrapper()(RuleContext{Document: document}); err != nil {
		t.Fatalf("reasoningWrapper() = %v, want nil", err)
	}
	if document.Has("reasoning") {
		t.Error("reasoning wrapper must be deleted")
	}
	if got, _ := document.Get("reasoning_effort"); got != "high" {
		t.Errorf("reasoning_effort = %v, want %q", got, "high")
	}
}

func TestReasoningWrapperEnabledFalseRecordsTheRefusal(t *testing.T) {
	document := parseTestDocument(t, `{"reasoning":{"enabled":false,"effort":"high"}}`)
	if err := reasoningWrapper()(RuleContext{Document: document}); err != nil {
		t.Fatalf("reasoningWrapper() = %v, want nil", err)
	}
	if document.Has("reasoning") {
		t.Error("the wrapper must be dropped")
	}
	if got, _ := document.Get("reasoning_effort"); got != "none" {
		t.Errorf("reasoning_effort = %v, want %q: a dropped wrapper is indistinguishable from silence, which a defaulting route fills in", got, "none")
	}
}

func TestReasoningWrapperExistingTopLevelWins(t *testing.T) {
	document := parseTestDocument(t, `{"reasoning_effort":"medium","reasoning":{"effort":"high"}}`)
	if err := reasoningWrapper()(RuleContext{Document: document}); err != nil {
		t.Fatalf("reasoningWrapper() = %v, want nil", err)
	}
	if got, _ := document.Get("reasoning_effort"); got != "medium" {
		t.Errorf("reasoning_effort = %v, want %q (pre-existing value must win)", got, "medium")
	}
}

func TestReasoningWrapperSilentlyDropsNonObject(t *testing.T) {
	for _, body := range []string{`{"reasoning":"high"}`, `{"reasoning":42}`, `{"reasoning":true}`, `{"reasoning":[{"effort":"high"}]}`} {
		t.Run(body, func(t *testing.T) {
			document := parseTestDocument(t, body)
			if err := reasoningWrapper()(RuleContext{Document: document}); err != nil {
				t.Fatalf("reasoningWrapper() = %v, want nil", err)
			}
			if document.Has("reasoning") || document.Has("reasoning_effort") {
				t.Error("non-object wrapper must be dropped without lifting anything")
			}
		})
	}
}

func TestReasoningWrapperAbsentIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[]}`)
	if err := reasoningWrapper()(RuleContext{Document: document}); err != nil {
		t.Fatalf("reasoningWrapper() = %v, want nil", err)
	}
}

func TestReasoningEffortValidateAccepts(t *testing.T) {
	for _, value := range []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"} {
		t.Run(value, func(t *testing.T) {
			document := parseTestDocument(t, `{"reasoning_effort":"`+value+`"}`)
			if err := reasoningEffortValidate()(RuleContext{Document: document}); err != nil {
				t.Errorf("reasoningEffortValidate() = %v, want nil", err)
			}
		})
	}
}

func TestReasoningEffortValidateAbsentIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[]}`)
	if err := reasoningEffortValidate()(RuleContext{Document: document}); err != nil {
		t.Fatalf("reasoningEffortValidate() = %v, want nil", err)
	}
}

func TestReasoningEffortValidateRejectsWrongShape(t *testing.T) {
	for _, body := range []string{`{"reasoning_effort":5}`, `{"reasoning_effort":true}`, `{"reasoning_effort":{"effort":"high"}}`} {
		t.Run(body, func(t *testing.T) {
			document := parseTestDocument(t, body)
			err := reasoningEffortValidate()(RuleContext{Document: document})
			want := "reasoning_effort: invalid shape: must be a string"
			if err == nil || err.Error() != want {
				t.Errorf("reasoningEffortValidate() = %v, want %q", err, want)
			}
		})
	}
}

// Exact message is golden-pinned by profile_reasoning_effort_invalid_value_rejected.
func TestReasoningEffortValidateRejectsUnknownValue(t *testing.T) {
	document := parseTestDocument(t, `{"reasoning_effort":"maximum"}`)
	err := reasoningEffortValidate()(RuleContext{Document: document})
	want := `reasoning_effort: unsupported value: got "maximum"`
	if err == nil || err.Error() != want {
		t.Fatalf("reasoningEffortValidate() = %v, want %q", err, want)
	}
	if got := ErrorStatus(err, 0); got != 400 {
		t.Errorf("ErrorStatus() = %d, want 400", got)
	}
}

func TestEnableThinkingStripsForThinkingStripProfile(t *testing.T) {
	document := parseTestDocument(t, `{"enable_thinking":true}`)
	if err := enableThinking()(RuleContext{Document: document, Profile: minimaxProfile}); err != nil {
		t.Fatalf("enableThinking() = %v, want nil", err)
	}
	if document.Has("enable_thinking") || document.Has("chat_template_kwargs") {
		t.Error("enable_thinking must be stripped without ever touching chat_template_kwargs")
	}
}

func TestEnableThinkingMirrorsForDefaultAndKimiProfiles(t *testing.T) {
	for _, profile := range []*Profile{nil, kimiProfile} {
		for _, value := range []bool{true, false} {
			document := parseTestDocument(t, `{"enable_thinking":`+boolLiteral(value)+`}`)
			if err := enableThinking()(RuleContext{Document: document, Profile: profile}); err != nil {
				t.Fatalf("enableThinking() = %v, want nil", err)
			}
			if document.Has("enable_thinking") {
				t.Error("top-level enable_thinking must be deleted after a successful mirror")
			}
			kwargs, ok := document.Object("chat_template_kwargs")
			if !ok {
				t.Fatal("chat_template_kwargs must be created")
			}
			if kwargs["enable_thinking"] != value {
				t.Errorf("chat_template_kwargs.enable_thinking = %v, want %v", kwargs["enable_thinking"], value)
			}
		}
	}
}

func TestEnableThinkingPreservesExistingNestedValue(t *testing.T) {
	document := parseTestDocument(t, `{"enable_thinking":true,"chat_template_kwargs":{"enable_thinking":false}}`)
	if err := enableThinking()(RuleContext{Document: document}); err != nil {
		t.Fatalf("enableThinking() = %v, want nil", err)
	}
	kwargs, _ := document.Object("chat_template_kwargs")
	if kwargs["enable_thinking"] != false {
		t.Errorf("chat_template_kwargs.enable_thinking = %v, want false (pre-existing value must win)", kwargs["enable_thinking"])
	}
}

func TestEnableThinkingPreservesOtherKwargsEntries(t *testing.T) {
	document := parseTestDocument(t, `{"enable_thinking":true,"chat_template_kwargs":{"foo":"bar"}}`)
	if err := enableThinking()(RuleContext{Document: document}); err != nil {
		t.Fatalf("enableThinking() = %v, want nil", err)
	}
	kwargs, _ := document.Object("chat_template_kwargs")
	if kwargs["foo"] != "bar" {
		t.Errorf("chat_template_kwargs.foo = %v, want %q", kwargs["foo"], "bar")
	}
	if kwargs["enable_thinking"] != true {
		t.Errorf("chat_template_kwargs.enable_thinking = %v, want true", kwargs["enable_thinking"])
	}
}

func TestEnableThinkingRejectsWrongShape(t *testing.T) {
	for _, body := range []string{`{"enable_thinking":"true"}`, `{"enable_thinking":1}`, `{"enable_thinking":{}}`, `{"enable_thinking":[]}`} {
		t.Run(body, func(t *testing.T) {
			document := parseTestDocument(t, body)
			if err := enableThinking()(RuleContext{Document: document}); err == nil {
				t.Error("enableThinking() = nil, want an error")
			}
		})
	}
}

func TestEnableThinkingRejectsWrongShapeKwargsAndPreservesField(t *testing.T) {
	document := parseTestDocument(t, `{"enable_thinking":true,"chat_template_kwargs":"broken"}`)
	err := enableThinking()(RuleContext{Document: document})
	if err == nil {
		t.Fatal("enableThinking() = nil, want an error")
	}
	if !document.Has("enable_thinking") {
		t.Error("top-level field must be preserved when it cannot be translated safely")
	}
}

func TestEnableThinkingAbsentIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[]}`)
	if err := enableThinking()(RuleContext{Document: document}); err != nil {
		t.Fatalf("enableThinking() = %v, want nil", err)
	}
	if document.Has("chat_template_kwargs") {
		t.Error("chat_template_kwargs must not be created when enable_thinking is absent")
	}
}

func TestThinkingStripsForThinkingStripProfile(t *testing.T) {
	document := parseTestDocument(t, `{"thinking":{"type":"enabled"}}`)
	if err := thinking()(RuleContext{Document: document, Profile: minimaxProfile}); err != nil {
		t.Fatalf("thinking() = %v, want nil", err)
	}
	if document.Has("thinking") {
		t.Error("thinking must be stripped for a ThinkingStrip profile")
	}
}

func TestThinkingNormalizesTypeForDefaultProfile(t *testing.T) {
	tests := []struct {
		name     string
		typeIn   string
		wantType string
	}{
		{"adaptive normalizes to enabled", "adaptive", "enabled"},
		{"auto normalizes to enabled", "auto", "enabled"},
		{"enabled stays enabled", "enabled", "enabled"},
		{"disabled stays disabled", "disabled", "disabled"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, `{"thinking":{"type":"`+testCase.typeIn+`","display":"summarized"}}`)
			if err := thinking()(RuleContext{Document: document}); err != nil {
				t.Fatalf("thinking() = %v, want nil", err)
			}
			wrapper, ok := document.Object("thinking")
			if !ok {
				t.Fatal("thinking wrapper must survive for the default profile")
			}
			if wrapper["type"] != testCase.wantType {
				t.Errorf("thinking.type = %v, want %q", wrapper["type"], testCase.wantType)
			}
			if _, hasDisplay := wrapper["display"]; hasDisplay {
				t.Error("display hint must be dropped")
			}
		})
	}
}

func TestThinkingMirrorsForThinkingMirrorToKwargsProfile(t *testing.T) {
	tests := []struct {
		name        string
		typeIn      string
		wantEnabled bool
	}{
		{"enabled mirrors true", "enabled", true},
		{"adaptive mirrors true", "adaptive", true},
		{"auto mirrors true", "auto", true},
		{"disabled mirrors false", "disabled", false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, `{"thinking":{"type":"`+testCase.typeIn+`"}}`)
			if err := thinking()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
				t.Fatalf("thinking() = %v, want nil", err)
			}
			if document.Has("thinking") {
				t.Error("top-level thinking must be dropped after a mirror")
			}
			kwargs, ok := document.Object("chat_template_kwargs")
			if !ok {
				t.Fatal("chat_template_kwargs must be created")
			}
			if kwargs["thinking"] != testCase.wantEnabled {
				t.Errorf("chat_template_kwargs.thinking = %v, want %v", kwargs["thinking"], testCase.wantEnabled)
			}
		})
	}
}

func TestThinkingMirrorPreservesExistingNestedValue(t *testing.T) {
	document := parseTestDocument(t, `{"thinking":{"type":"enabled"},"chat_template_kwargs":{"thinking":false}}`)
	if err := thinking()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinking() = %v, want nil", err)
	}
	kwargs, _ := document.Object("chat_template_kwargs")
	if kwargs["thinking"] != false {
		t.Errorf("chat_template_kwargs.thinking = %v, want false (pre-existing value must win)", kwargs["thinking"])
	}
}

func TestThinkingRejects(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"wrapper not object", `{"thinking":"enabled"}`},
		{"wrapper is array", `{"thinking":[]}`},
		{"type missing", `{"thinking":{}}`},
		{"type is bool", `{"thinking":{"type":true}}`},
		{"type is unknown string", `{"thinking":{"type":"on"}}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := thinking()(RuleContext{Document: document}); err == nil {
				t.Error("thinking() = nil, want an error")
			}
		})
	}
}

func TestThinkingAbsentIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[]}`)
	if err := thinking()(RuleContext{Document: document}); err != nil {
		t.Fatalf("thinking() = %v, want nil", err)
	}
}

// The budget reaches the host for every model, not only the profile that resolves one of its own.
func TestThinkingTokenBudgetSurvivesForEveryModel(t *testing.T) {
	for _, model := range []string{deepseekModelID, minimaxModelID, kimiModelID, "Qwen/Unknown"} {
		t.Run(model, func(t *testing.T) {
			body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"thinking_token_budget":128}`, model)
			result, err := NormalizeRequest([]byte(body), Options{RoutedModel: model})
			if err != nil {
				t.Fatalf("NormalizeRequest() = %v, want nil", err)
			}
			var document map[string]any
			if err := json.Unmarshal(result.Body, &document); err != nil {
				t.Fatalf("unmarshal normalized body: %v", err)
			}
			if got := document["thinking_token_budget"]; got != float64(128) {
				t.Errorf("thinking_token_budget = %v, want 128", got)
			}
		})
	}
}

func TestThinkingTokenBudgetRejectsANonNumericValue(t *testing.T) {
	body := `{"model":"Qwen/Unknown","messages":[{"role":"user","content":"x"}],"thinking_token_budget":"lots"}`
	if _, err := NormalizeRequest([]byte(body), Options{RoutedModel: "Qwen/Unknown"}); err == nil {
		t.Error("NormalizeRequest() = nil, want a rejection: the budget is no longer stripped, so it has to be validated")
	}
}

// Without a profile resolution the caller's budget is kept, but still clamped to leave room for content.
func TestThinkingTokenBudgetResolveClampsWithoutHook(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"max_tokens":4096,"thinking_token_budget":50}`)
		if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: profile}); err != nil {
			t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
		}
		if got, _ := document.Get("thinking_token_budget"); got != uint64(50) {
			t.Errorf("thinking_token_budget = %v, want 50 for profile %v", got, profile)
		}
	}
}

func TestThinkingTokenBudgetResolveWithoutHookNeverEatsTheContentHeadroom(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":300,"thinking_token_budget":9000}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: nil}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != 300-thinkingBudgetContentHeadroom {
		t.Errorf("thinking_token_budget = %v, want the headroom cap %d", got, 300-thinkingBudgetContentHeadroom)
	}
}

// A profile without a resolution must not gain one: nothing is invented when the caller asked for nothing.
func TestThinkingTokenBudgetResolveInventsNoBudgetWithoutHook(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":4096}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: nil}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, present := document.Get("thinking_token_budget"); present {
		t.Errorf("thinking_token_budget = %v, want absent", got)
	}
}

func TestThinkingTokenBudgetResolveDefaultsToHalf(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":4096}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(2048) {
		t.Errorf("thinking_token_budget = %v, want 2048", got)
	}
}

func TestThinkingTokenBudgetResolveForcesZeroBelowThreshold(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":100,"thinking_token_budget":50}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(0) {
		t.Errorf("thinking_token_budget = %v, want 0 (client value overridden)", got)
	}
}

func TestThinkingTokenBudgetResolveForceZeroOverwritesCallerThinking(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":100,"chat_template_kwargs":{"thinking":true,"enable_thinking":true}}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	kwargs, _ := document.Get("chat_template_kwargs")
	want := map[string]any{"thinking": false, "enable_thinking": true}
	if !reflect.DeepEqual(kwargs, want) {
		t.Errorf("chat_template_kwargs = %v, want %v", kwargs, want)
	}
}

// The rule tests above cannot reach this: the thinking rule mirrors the caller's answer into the kwargs
// during PreValidation, so by the time the budget is forced to zero at PostLimits the key already exists.
// A fill-only write there leaves the template thinking with no budget to think in, which is the
// empty-content burn the force-zero exists to prevent.
func TestNormalizeRequestKimiForceZeroSilencesThinkingThroughEveryEntryPoint(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
	}{
		{"top-level thinking", `{"messages":[{"role":"user","content":"x"}],"max_tokens":100,"thinking":{"type":"enabled"}}`},
		{"template kwargs directly", `{"messages":[{"role":"user","content":"x"}],"max_tokens":100,"chat_template_kwargs":{"thinking":true}}`},
		{"adaptive from the CLI", `{"messages":[{"role":"user","content":"x"}],"max_tokens":100,"thinking":{"type":"adaptive"}}`},
		{"nothing asked at all", `{"messages":[{"role":"user","content":"x"}],"max_tokens":100}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := NormalizeRequest([]byte(testCase.body), Options{RoutedModel: kimiModelID})
			if err != nil {
				t.Fatalf("NormalizeRequest() = %v, want nil", err)
			}
			var document map[string]any
			if err := json.Unmarshal(result.Body, &document); err != nil {
				t.Fatalf("unmarshal normalized body: %v", err)
			}
			kwargs, ok := document["chat_template_kwargs"].(map[string]any)
			if !ok {
				t.Fatalf("chat_template_kwargs missing from %s", result.Body)
			}
			if kwargs["thinking"] != false {
				t.Errorf("chat_template_kwargs.thinking = %v, want false: a zero budget with a thinking template is the burn being fixed", kwargs["thinking"])
			}
			if budget := document["thinking_token_budget"]; budget != float64(0) {
				t.Errorf("thinking_token_budget = %v, want 0", budget)
			}
		})
	}
}

// max_tokens==255 IS below the 256 force-zero threshold: the budget is forced to 0.
// max_tokens==255 IS below the 256 force-zero threshold: the budget is forced to 0.
func TestThinkingTokenBudgetResolveJustBelowThresholdForcesZero(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":255}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(0) {
		t.Errorf("thinking_token_budget = %v, want 0", got)
	}
}

// max_tokens==256 is NOT below the 256 force-zero threshold: the half-split default applies.
func TestThinkingTokenBudgetResolveBoundaryAtThresholdKeepsHalfSplit(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":256}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(128) {
		t.Errorf("thinking_token_budget = %v, want 128", got)
	}
}

func TestThinkingTokenBudgetResolveContentHeadroomClamp(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":4096,"thinking_token_budget":10000}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(4032) {
		t.Errorf("thinking_token_budget = %v, want 4032 (max_tokens - 64 headroom)", got)
	}
}

// max_tokens-headroom (199936) exceeds the absolute max (96000), so the absolute max wins.
func TestThinkingTokenBudgetResolveAbsoluteMaxClamp(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":200000,"thinking_token_budget":150000}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(96_000) {
		t.Errorf("thinking_token_budget = %v, want 96000", got)
	}
}

func TestThinkingTokenBudgetResolvePreservesClientValueUnderHeadroom(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":4096,"thinking_token_budget":500}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != uint64(500) {
		t.Errorf("thinking_token_budget = %v, want 500 (unclamped)", got)
	}
}

// Documents the corpus surprise: a string-typed budget fails the uint64 coercion and is
// left completely untouched, bypassing every clamp below.
func TestThinkingTokenBudgetResolveStringValueBypassesClamp(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":1000,"thinking_token_budget":"99999999"}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != "99999999" {
		t.Errorf("thinking_token_budget = %#v, want the original string untouched", got)
	}
}

func TestThinkingTokenBudgetResolveSkipsWhenMaxTokensAbsent(t *testing.T) {
	document := parseTestDocument(t, `{}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if document.Has("thinking_token_budget") {
		t.Error("thinking_token_budget must not be created when max_tokens is absent")
	}
}

func TestThinkingTokenBudgetResolveSkipsWhenMaxTokensZero(t *testing.T) {
	document := parseTestDocument(t, `{"max_tokens":0}`)
	if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
	}
	if document.Has("thinking_token_budget") {
		t.Error("thinking_token_budget must not be created when max_tokens is zero")
	}
}

func TestSafetyIdentifierKeepsAndValidatesForHookProfile(t *testing.T) {
	document := parseTestDocument(t, `{"safety_identifier":"hashed-user-abc"}`)
	if err := safetyIdentifier()(RuleContext{Document: document, Param: "safety_identifier", Profile: kimiProfile}); err != nil {
		t.Fatalf("safetyIdentifier() = %v, want nil", err)
	}
	if got, _ := document.Get("safety_identifier"); got != "hashed-user-abc" {
		t.Errorf("safety_identifier = %v, want unchanged", got)
	}
}

func TestSafetyIdentifierRejectsOverLengthForHookProfile(t *testing.T) {
	long := make([]byte, safetyIdentifierMaxLen+1)
	for i := range long {
		long[i] = 'x'
	}
	document := parseTestDocument(t, `{"safety_identifier":"`+string(long)+`"}`)
	err := safetyIdentifier()(RuleContext{Document: document, Param: "safety_identifier", Profile: kimiProfile})
	if err == nil {
		t.Fatal("safetyIdentifier() = nil, want an error")
	}
}

func TestSafetyIdentifierStripsWithoutHook(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"safety_identifier":"anything"}`)
		if err := safetyIdentifier()(RuleContext{Document: document, Param: "safety_identifier", Profile: profile}); err != nil {
			t.Fatalf("safetyIdentifier() = %v, want nil", err)
		}
		if document.Has("safety_identifier") {
			t.Errorf("safety_identifier must be stripped for profile %v", profile)
		}
	}
}

func TestReasoningSplitPassesThroughForHookProfile(t *testing.T) {
	document := parseTestDocument(t, `{"reasoning_split":false}`)
	if err := reasoningSplit()(RuleContext{Document: document, Param: "reasoning_split", Profile: minimaxProfile}); err != nil {
		t.Fatalf("reasoningSplit() = %v, want nil", err)
	}
	if got, _ := document.Get("reasoning_split"); got != false {
		t.Errorf("reasoning_split = %v, want unchanged (false)", got)
	}
}

func TestReasoningSplitStripsWithoutHook(t *testing.T) {
	for _, profile := range []*Profile{nil, kimiProfile} {
		document := parseTestDocument(t, `{"reasoning_split":true}`)
		if err := reasoningSplit()(RuleContext{Document: document, Param: "reasoning_split", Profile: profile}); err != nil {
			t.Fatalf("reasoningSplit() = %v, want nil", err)
		}
		if document.Has("reasoning_split") {
			t.Errorf("reasoning_split must be stripped for profile %v", profile)
		}
	}
}

func TestForceZeroPenaltyOverwritesPresentFieldForHookProfile(t *testing.T) {
	document := parseTestDocument(t, `{"frequency_penalty":0.5}`)
	if err := forceZeroPenalty()(RuleContext{Document: document, Param: "frequency_penalty", Profile: kimiProfile}); err != nil {
		t.Fatalf("forceZeroPenalty() = %v, want nil", err)
	}
	if got, _ := document.Get("frequency_penalty"); got != 0.0 {
		t.Errorf("frequency_penalty = %v, want 0", got)
	}
}

func TestForceZeroPenaltyLeavesAbsentFieldAbsent(t *testing.T) {
	document := parseTestDocument(t, `{}`)
	if err := forceZeroPenalty()(RuleContext{Document: document, Param: "frequency_penalty", Profile: kimiProfile}); err != nil {
		t.Fatalf("forceZeroPenalty() = %v, want nil", err)
	}
	if document.Has("frequency_penalty") {
		t.Error("forceZeroPenalty must not create the field when absent (overwrite-only)")
	}
}

func TestForceZeroPenaltyNoOpWithoutHook(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"presence_penalty":-0.5}`)
		if err := forceZeroPenalty()(RuleContext{Document: document, Param: "presence_penalty", Profile: profile}); err != nil {
			t.Fatalf("forceZeroPenalty() = %v, want nil", err)
		}
		if got, _ := document.Get("presence_penalty"); got != json.Number("-0.5") {
			t.Errorf("presence_penalty = %v, want unchanged for profile %v", got, profile)
		}
	}
}

func boolLiteral(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func TestASmallBudgetSilencesKimiInTheTemplateToo(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		body         string
		wantThinking any
	}{
		{
			name:         "below the cutoff the template is told not to think",
			body:         `{"max_tokens":144}`,
			wantThinking: false,
		},
		{
			name: "at the cutoff the template is left alone",
			body: `{"max_tokens":256}`,
		},
		{
			name:         "a client that asked to think is overruled",
			body:         `{"max_tokens":144,"chat_template_kwargs":{"thinking":true}}`,
			wantThinking: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			document := parseTestDocument(t, testCase.body)

			if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Param: "thinking_token_budget", Profile: kimiProfile}); err != nil {
				t.Fatalf("resolve = %v, want nil", err)
			}

			kwargs, _, _ := document.ObjectField("chat_template_kwargs")
			if testCase.wantThinking == nil {
				if _, held := kwargs["thinking"]; held {
					t.Fatalf("chat_template_kwargs = %v, want no thinking flag", kwargs)
				}
				return
			}
			if kwargs["thinking"] != testCase.wantThinking {
				t.Fatalf("chat_template_kwargs.thinking = %v, want %v", kwargs["thinking"], testCase.wantThinking)
			}
		})
	}
}

// The level reaches every route: only DeepSeek's renderer reads it, but a caller asking for one is not
// second-guessed on the routes that ignore it.
func TestReasoningEffortReachesEveryRoute(t *testing.T) {
	for _, model := range []string{"", kimiModelID, minimaxModelID, deepseekModelID} {
		for _, effort := range []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"} {
			body := `{"messages":[{"role":"user","content":"x"}],"max_tokens":4096,"reasoning_effort":"` + effort + `"}`
			document := normalizedDocument(t, body, model)
			if document["reasoning_effort"] != effort {
				t.Errorf("model %q: reasoning_effort = %v, want %q", model, document["reasoning_effort"], effort)
			}
		}
	}
}
