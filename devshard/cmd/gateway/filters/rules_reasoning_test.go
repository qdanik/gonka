package filters

import (
	"encoding/json"
	"testing"
)

// --- reasoningWrapper ---

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

func TestReasoningWrapperEnabledFalseBlocksLift(t *testing.T) {
	document := parseTestDocument(t, `{"reasoning":{"enabled":false,"effort":"high"}}`)
	if err := reasoningWrapper()(RuleContext{Document: document}); err != nil {
		t.Fatalf("reasoningWrapper() = %v, want nil", err)
	}
	if document.Has("reasoning") || document.Has("reasoning_effort") {
		t.Error("enabled:false must drop the wrapper without lifting effort")
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

// --- reasoningEffortValidate ---

func TestReasoningEffortValidateAccepts(t *testing.T) {
	for _, value := range []string{"none", "minimal", "low", "medium", "high", "xhigh"} {
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
	document := parseTestDocument(t, `{"reasoning_effort":"max"}`)
	err := reasoningEffortValidate()(RuleContext{Document: document})
	want := `reasoning_effort: unsupported value: got "max"`
	if err == nil || err.Error() != want {
		t.Fatalf("reasoningEffortValidate() = %v, want %q", err, want)
	}
	if got := ErrorStatus(err, 0); got != 400 {
		t.Errorf("ErrorStatus() = %d, want 400", got)
	}
}

// --- enableThinking ---

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

// --- thinking ---

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

// --- thinkingTokenBudgetStrip / thinkingTokenBudgetResolve ---

func TestThinkingTokenBudgetStrip(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"thinking_token_budget":123}`)
		if err := thinkingTokenBudgetStrip()(RuleContext{Document: document, Param: "thinking_token_budget", Profile: profile}); err != nil {
			t.Fatalf("thinkingTokenBudgetStrip() = %v, want nil", err)
		}
		if document.Has("thinking_token_budget") {
			t.Errorf("thinking_token_budget must be stripped for profile %v", profile)
		}
	}
}

func TestThinkingTokenBudgetStripKeepsForHookProfile(t *testing.T) {
	document := parseTestDocument(t, `{"thinking_token_budget":123}`)
	if err := thinkingTokenBudgetStrip()(RuleContext{Document: document, Param: "thinking_token_budget", Profile: kimiProfile}); err != nil {
		t.Fatalf("thinkingTokenBudgetStrip() = %v, want nil", err)
	}
	if got, _ := document.Get("thinking_token_budget"); got != json.Number("123") {
		t.Errorf("thinking_token_budget = %v, want unchanged", got)
	}
}

func TestThinkingTokenBudgetResolveNoOpWithoutHook(t *testing.T) {
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"max_tokens":100,"thinking_token_budget":50}`)
		if err := thinkingTokenBudgetResolve()(RuleContext{Document: document, Profile: profile}); err != nil {
			t.Fatalf("thinkingTokenBudgetResolve() = %v, want nil", err)
		}
		if got, _ := document.Get("thinking_token_budget"); got != json.Number("50") {
			t.Errorf("thinking_token_budget = %v, want unchanged for profile %v", got, profile)
		}
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

// --- safetyIdentifier ---

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

// --- reasoningSplit ---

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

// --- forceZeroPenalty ---

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
