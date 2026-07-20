package filters

import (
	"reflect"
	"strings"
	"testing"
)

// --- messageRolePolicies table ---

func TestMessageRolePoliciesTableIsComplete(t *testing.T) {
	want := map[string]messageRolePolicy{
		roleDeveloper: {disallowedFields: []string{"tool_calls", "tool_call_id", "function_call"}},
		roleSystem:    {disallowedFields: []string{"tool_calls", "tool_call_id", "function_call"}},
		roleUser:      {disallowedFields: []string{"tool_calls", "tool_call_id", "function_call"}},
		roleAssistant: {disallowedFields: []string{"tool_call_id"}},
		roleTool:      {disallowedFields: []string{"tool_calls", "function_call"}, requireToolCallID: true},
		roleFunction:  {disallowedFields: []string{"tool_calls", "tool_call_id", "function_call"}, requireName: true},
	}
	if len(messageRolePolicies) != len(want) {
		t.Fatalf("messageRolePolicies has %d roles, want %d", len(messageRolePolicies), len(want))
	}
	for role, wantPolicy := range want {
		gotPolicy, ok := messageRolePolicies[role]
		if !ok {
			t.Errorf("role %q missing from messageRolePolicies", role)
			continue
		}
		if !reflect.DeepEqual(gotPolicy, wantPolicy) {
			t.Errorf("messageRolePolicies[%q] = %+v, want %+v", role, gotPolicy, wantPolicy)
		}
	}
}

// --- dropOrphanToolMessages ---

func TestDropOrphanToolMessages(t *testing.T) {
	tests := []struct {
		name        string
		messages    []any
		wantDropped bool
		wantRoles   string
	}{
		{
			name: "matched id is kept",
			messages: []any{
				assistantWithToolCalls("c1"),
				toolMessage("c1", "result"),
			},
			wantDropped: false,
			wantRoles:   "assistant,tool",
		},
		{
			name: "unmatched id is dropped",
			messages: []any{
				map[string]any{"role": "user", "content": "hi"},
				toolMessage("nobody", "stray"),
				map[string]any{"role": "assistant", "content": "hello"},
			},
			wantDropped: true,
			wantRoles:   "user,assistant",
		},
		{
			name: "second tool reply to the same id is orphaned: the first consumed it",
			messages: []any{
				assistantWithToolCalls("c1"),
				toolMessage("c1", "first"),
				toolMessage("c1", "second"),
			},
			wantDropped: true,
			wantRoles:   "assistant,tool",
		},
		{
			name: "multiple distinct pending ids all match, in any order",
			messages: []any{
				assistantWithToolCalls("c1", "c2"),
				toolMessage("c2", "b"),
				toolMessage("c1", "a"),
			},
			wantDropped: false,
			wantRoles:   "assistant,tool,tool",
		},
		{
			name: "one matched and one orphaned among several tool messages",
			messages: []any{
				assistantWithToolCalls("c1"),
				toolMessage("c1", "ok"),
				toolMessage("c2", "orphan"),
			},
			wantDropped: true,
			wantRoles:   "assistant,tool",
		},
		{
			name: "non-map entry passes through untouched",
			messages: []any{
				"not-a-message",
				map[string]any{"role": "user", "content": "hi"},
			},
			wantDropped: false,
			wantRoles:   ",user",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			out, changed, err := dropOrphanToolMessages(testCase.messages)
			if err != nil {
				t.Fatalf("dropOrphanToolMessages() error = %v, want nil", err)
			}
			if changed != testCase.wantDropped {
				t.Errorf("changed = %v, want %v", changed, testCase.wantDropped)
			}
			if got := rolesOf(t, out); got != testCase.wantRoles {
				t.Errorf("survivor roles = %q, want %q", got, testCase.wantRoles)
			}
		})
	}
}

// --- dropEmptyAssistantTurns ---

func TestDropEmptyAssistantTurns(t *testing.T) {
	tests := []struct {
		name              string
		message           map[string]any
		wantChanged       bool
		wantSurvivorCount int
	}{
		{"no fields at all", map[string]any{"role": "assistant"}, true, 0},
		{"empty string content", map[string]any{"role": "assistant", "content": ""}, true, 0},
		{"nil content", map[string]any{"role": "assistant", "content": nil}, true, 0},
		{"empty content parts array", map[string]any{"role": "assistant", "content": []any{}}, true, 0},
		{"empty tool_calls array", map[string]any{"role": "assistant", "tool_calls": []any{}}, true, 0},
		{"nil tool_calls", map[string]any{"role": "assistant", "tool_calls": nil}, true, 0},
		{"empty function_call object", map[string]any{"role": "assistant", "function_call": map[string]any{}}, true, 0},
		{"non-empty content is kept", map[string]any{"role": "assistant", "content": "answer"}, false, 1},
		{"non-empty tool_calls is kept", map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": "c1"}}}, false, 1},
		{"non-empty function_call is kept", map[string]any{"role": "assistant", "function_call": map[string]any{"name": "fn"}}, false, 1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			out, changed, err := dropEmptyAssistantTurns([]any{testCase.message})
			if err != nil {
				t.Fatalf("dropEmptyAssistantTurns() error = %v, want nil", err)
			}
			if changed != testCase.wantChanged {
				t.Errorf("changed = %v, want %v", changed, testCase.wantChanged)
			}
			if len(out) != testCase.wantSurvivorCount {
				t.Errorf("survivor count = %d, want %d", len(out), testCase.wantSurvivorCount)
			}
		})
	}
}

func TestDropEmptyAssistantTurnsLeavesOtherRolesAlone(t *testing.T) {
	// An empty-looking user message is the validator's concern, not the normalizer's.
	out, changed, err := dropEmptyAssistantTurns([]any{map[string]any{"role": "user", "content": ""}})
	if err != nil {
		t.Fatalf("dropEmptyAssistantTurns() error = %v, want nil", err)
	}
	if changed || len(out) != 1 {
		t.Errorf("changed = %v, len(out) = %d, want unchanged single survivor", changed, len(out))
	}
}

func TestDropEmptyAssistantTurnsSkipsNonMapEntries(t *testing.T) {
	out, changed, err := dropEmptyAssistantTurns([]any{"not-a-message"})
	if err != nil {
		t.Fatalf("dropEmptyAssistantTurns() error = %v, want nil", err)
	}
	if changed || len(out) != 1 {
		t.Errorf("changed = %v, len(out) = %d, want the non-map entry passed through", changed, len(out))
	}
}

// --- normalizeEmptyMessageContent ---

func TestNormalizeEmptyMessageContentFillsToolSentinel(t *testing.T) {
	tests := []struct {
		name    string
		message map[string]any
	}{
		{"missing content", map[string]any{"role": "tool", "tool_call_id": "c1"}},
		{"nil content", map[string]any{"role": "tool", "tool_call_id": "c1", "content": nil}},
		{"empty string content", map[string]any{"role": "tool", "tool_call_id": "c1", "content": ""}},
		{"empty content parts array", map[string]any{"role": "tool", "tool_call_id": "c1", "content": []any{}}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, changed, err := normalizeEmptyMessageContent([]any{testCase.message})
			if err != nil {
				t.Fatalf("normalizeEmptyMessageContent() error = %v, want nil", err)
			}
			if !changed {
				t.Fatal("changed = false, want true")
			}
			if testCase.message["content"] != emptyToolResultContent {
				t.Errorf("content = %v, want sentinel %q", testCase.message["content"], emptyToolResultContent)
			}
		})
	}
}

func TestNormalizeEmptyMessageContentNullifiesAssistantWithCallPayload(t *testing.T) {
	tests := []struct {
		name    string
		message map[string]any
	}{
		{"empty string content with tool_calls", map[string]any{"role": "assistant", "content": "", "tool_calls": []any{map[string]any{"id": "c1"}}}},
		{"empty content parts array with tool_calls", map[string]any{"role": "assistant", "content": []any{}, "tool_calls": []any{map[string]any{"id": "c1"}}}},
		{"empty string content with function_call", map[string]any{"role": "assistant", "content": "", "function_call": map[string]any{"name": "fn"}}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, changed, err := normalizeEmptyMessageContent([]any{testCase.message})
			if err != nil {
				t.Fatalf("normalizeEmptyMessageContent() error = %v, want nil", err)
			}
			if !changed {
				t.Fatal("changed = false, want true")
			}
			if testCase.message["content"] != nil {
				t.Errorf("content = %v, want nil", testCase.message["content"])
			}
		})
	}
}

func TestNormalizeEmptyMessageContentLeavesAssistantWithoutCallsAlone(t *testing.T) {
	// No tool_calls/function_call payload: validateMessages rejects this. The normalizer
	// must not paper over it by inventing content.
	message := map[string]any{"role": "assistant", "content": ""}
	_, changed, err := normalizeEmptyMessageContent([]any{message})
	if err != nil {
		t.Fatalf("normalizeEmptyMessageContent() error = %v, want nil", err)
	}
	if changed || message["content"] != "" {
		t.Errorf("changed = %v, content = %v, want untouched empty string", changed, message["content"])
	}
}

func TestNormalizeEmptyMessageContentLeavesUserRoleAlone(t *testing.T) {
	message := map[string]any{"role": "user", "content": ""}
	_, changed, err := normalizeEmptyMessageContent([]any{message})
	if err != nil {
		t.Fatalf("normalizeEmptyMessageContent() error = %v, want nil", err)
	}
	if changed {
		t.Error("user content emptiness is the validator's concern, not the normalizer's")
	}
}

// --- stripLegacyToolName ---

func TestStripLegacyToolName(t *testing.T) {
	tests := []struct {
		name        string
		message     map[string]any
		wantChanged bool
		wantHasName bool
	}{
		{"tool with name is stripped", map[string]any{"role": "tool", "tool_call_id": "c1", "content": "r", "name": "legacy"}, true, false},
		{"tool without name is a no-op", map[string]any{"role": "tool", "tool_call_id": "c1", "content": "r"}, false, false},
		{"user name is kept", map[string]any{"role": "user", "content": "hi", "name": "kept"}, false, true},
		{"assistant name is kept", map[string]any{"role": "assistant", "content": "hi", "name": "kept"}, false, true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, changed, err := stripLegacyToolName([]any{testCase.message})
			if err != nil {
				t.Fatalf("stripLegacyToolName() error = %v, want nil", err)
			}
			if changed != testCase.wantChanged {
				t.Errorf("changed = %v, want %v", changed, testCase.wantChanged)
			}
			_, hasName := testCase.message["name"]
			if hasName != testCase.wantHasName {
				t.Errorf("has name = %v, want %v", hasName, testCase.wantHasName)
			}
		})
	}
}

// --- flattenMessageTextParts ---

func TestFlattenMessageTextPartsJoinsMultipleParts(t *testing.T) {
	message := map[string]any{"role": "user", "content": []any{
		map[string]any{"type": "text", "text": "hello"},
		map[string]any{"type": "text", "text": "world"},
	}}
	_, changed, err := flattenMessageTextParts([]any{message})
	if err != nil {
		t.Fatalf("flattenMessageTextParts() error = %v, want nil", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if message["content"] != "hello\nworld" {
		t.Errorf("content = %v, want %q", message["content"], "hello\nworld")
	}
}

func TestFlattenMessageTextPartsSinglePart(t *testing.T) {
	message := map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "solo"}}}
	_, changed, err := flattenMessageTextParts([]any{message})
	if err != nil {
		t.Fatalf("flattenMessageTextParts() error = %v, want nil", err)
	}
	if !changed || message["content"] != "solo" {
		t.Errorf("changed = %v, content = %v, want (true, %q)", changed, message["content"], "solo")
	}
}

func TestFlattenMessageTextPartsLeavesStringContentAlone(t *testing.T) {
	message := map[string]any{"role": "user", "content": "already a string"}
	_, changed, err := flattenMessageTextParts([]any{message})
	if err != nil {
		t.Fatalf("flattenMessageTextParts() error = %v, want nil", err)
	}
	if changed {
		t.Error("changed = true, want false: string content must not trigger a rewrite")
	}
}

func TestFlattenMessageTextPartsLeavesEmptyArrayAlone(t *testing.T) {
	// combineTextContentParts returns "" for an empty slice; the flattener treats "" as
	// "nothing to write back", so an explicitly empty array survives unflattened.
	message := map[string]any{"role": "user", "content": []any{}}
	_, changed, err := flattenMessageTextParts([]any{message})
	if err != nil {
		t.Fatalf("flattenMessageTextParts() error = %v, want nil", err)
	}
	if changed {
		t.Error("changed = true, want false: empty array must not become an empty string")
	}
}

func TestFlattenMessageTextPartsSkipsMissingOrNilContent(t *testing.T) {
	for _, message := range []map[string]any{
		{"role": "user"},
		{"role": "user", "content": nil},
	} {
		_, changed, err := flattenMessageTextParts([]any{message})
		if err != nil {
			t.Fatalf("flattenMessageTextParts() error = %v, want nil", err)
		}
		if changed {
			t.Errorf("changed = true for %v, want false", message)
		}
	}
}

func TestFlattenMessageTextPartsSkipsNonMapEntry(t *testing.T) {
	_, changed, err := flattenMessageTextParts([]any{"not-a-message"})
	if err != nil {
		t.Fatalf("flattenMessageTextParts() error = %v, want nil", err)
	}
	if changed {
		t.Error("changed = true, want false")
	}
}

func TestFlattenMessageTextPartsReportsMessageAndPartIndexOnNonTextPart(t *testing.T) {
	messages := []any{
		map[string]any{"role": "user", "content": "ok"},
		map[string]any{"role": "user", "content": []any{map[string]any{"type": "image_url", "text": "x"}}},
	}
	_, _, err := flattenMessageTextParts(messages)
	want := `messages[1].content[0].type has unsupported value "image_url"`
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// --- normalizer chain order ---

func TestMessageNormalizerChainDropsOrphanBeforeFlatteningContent(t *testing.T) {
	// The real chain order must silently drop this orphan tool message -- including its
	// malformed content -- rather than surface a flatten error for a message on its way out.
	document := parseTestDocument(t, `{"messages":[
		{"role":"user","content":"q"},
		{"role":"tool","tool_call_id":"ghost","content":[{"type":"image_url","text":"x"}]}
	]}`)
	if err := normalizeMessages(document, ""); err != nil {
		t.Fatalf("normalizeMessages() = %v, want nil (orphan drop must precede the flatten error)", err)
	}
	messages, _ := document.Array("messages")
	if len(messages) != 1 {
		t.Fatalf("want the orphan tool message dropped, got %d survivors: %v", len(messages), messages)
	}
}

func TestMessageNormalizerChainOrderIsObservable(t *testing.T) {
	// The same input, with the flatten step run before the orphan drop instead of after,
	// surfaces an error -- proving the chain's fixed order changes the outcome.
	messages := []any{
		map[string]any{"role": "user", "content": "q"},
		map[string]any{"role": "tool", "tool_call_id": "ghost", "content": []any{
			map[string]any{"type": "image_url", "text": "x"},
		}},
	}
	_, _, err := flattenMessageTextParts(messages)
	want := `messages[1].content[0].type has unsupported value "image_url"`
	if err == nil || err.Error() != want {
		t.Fatalf("flattenMessageTextParts() run before the orphan drop: error = %v, want %q", err, want)
	}
}

// --- normalizeMessages ---

func TestNormalizeMessagesAbsentIsNoOp(t *testing.T) {
	document := parseTestDocument(t, `{}`)
	if err := normalizeMessages(document, ""); err != nil {
		t.Fatalf("normalizeMessages() = %v, want nil", err)
	}
	if document.Has("messages") {
		t.Error("normalizeMessages must not create a messages field")
	}
}

func TestNormalizeMessagesNoOpWhenNothingToNormalize(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[{"role":"user","content":"hi"}]}`)
	if err := normalizeMessages(document, ""); err != nil {
		t.Fatalf("normalizeMessages() = %v, want nil", err)
	}
	messages, _ := document.Array("messages")
	if len(messages) != 1 {
		t.Fatalf("want 1 untouched message, got %d", len(messages))
	}
}

func TestNormalizeMessagesRunsTheFullChain(t *testing.T) {
	// One pass strips a legacy tool name and flattens a text-parts array together.
	document := parseTestDocument(t, `{"messages":[
		{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]},
		{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"fn"}}]},
		{"role":"tool","tool_call_id":"c1","name":"legacy","content":"4"}
	]}`)
	if err := normalizeMessages(document, ""); err != nil {
		t.Fatalf("normalizeMessages() = %v, want nil", err)
	}
	messages, _ := document.Array("messages")
	first := messages[0].(map[string]any)
	if first["content"] != "a\nb" {
		t.Errorf("messages[0].content = %v, want %q", first["content"], "a\nb")
	}
	last := messages[2].(map[string]any)
	if _, hasName := last["name"]; hasName {
		t.Error("messages[2].name must be stripped")
	}
}

func TestNormalizeMessagesPropagatesFlattenErrorAsRejection(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[{"role":"user","content":[{"type":"image_url","text":"x"}]}]}`)
	err := normalizeMessages(document, "")
	if err == nil {
		t.Fatal("normalizeMessages() = nil, want a rejection")
	}
	if got := ErrorStatus(err, 0); got != 400 {
		t.Errorf("ErrorStatus() = %d, want 400", got)
	}
	want := `messages[0].content[0].type has unsupported value "image_url"`
	if err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

// --- validateMessages: shape ---

func TestValidateMessagesRequiredWhenAbsent(t *testing.T) {
	assertValidateMessagesRejects(t, `{}`, "messages is required")
}

func TestValidateMessagesNonArrayTreatedAsAbsent(t *testing.T) {
	// Document.Array's type assertion fails the same way for the wrong type as for a
	// missing key, so a non-array "messages" is reported as absent, not malformed.
	assertValidateMessagesRejects(t, `{"messages":"not-an-array"}`, "messages is required")
}

func TestValidateMessagesMustNotBeEmpty(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":[]}`, "messages must not be empty")
}

func TestValidateMessagesRejectsNonObjectEntry(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":["not-an-object"]}`, "messages[0] must be an object")
}

func TestValidateMessagesRoleRequired(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMessage string
	}{
		{"missing role", `{"messages":[{"content":"hi"}]}`, "messages[0].role: is required"},
		{"null role", `{"messages":[{"role":null,"content":"hi"}]}`, "messages[0].role: is required"},
		{"non-string role", `{"messages":[{"role":5,"content":"hi"}]}`, "messages[0].role: must be a string"},
		{"blank role", `{"messages":[{"role":"  ","content":"hi"}]}`, "messages[0].role: must not be empty"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertValidateMessagesRejects(t, testCase.body, testCase.wantMessage)
		})
	}
}

func TestValidateMessagesRoleEnumRejectsUnknownValue(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":[{"role":"pirate","content":"arr"}]}`, `messages[0].role has unsupported value "pirate"`)
}

func TestValidateMessagesAcceptsEveryKnownRole(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[
		{"role":"developer","content":"a"},
		{"role":"system","content":"b"},
		{"role":"user","content":"c"},
		{"role":"assistant","content":"d"},
		{"role":"function","name":"fn","content":"e"}
	]}`)
	if err := validateMessages(document, ""); err != nil {
		t.Fatalf("validateMessages() = %v, want nil", err)
	}
}

// --- validateMessages: per-role disallowed fields ---

func TestValidateMessagesRejectsDisallowedFieldPerRole(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMessage string
	}{
		{"developer/tool_calls", `{"messages":[{"role":"developer","content":"hi","tool_calls":[]}]}`, "messages[0]: tool_calls is not allowed for this role"},
		{"developer/tool_call_id", `{"messages":[{"role":"developer","content":"hi","tool_call_id":"x"}]}`, "messages[0]: tool_call_id is not allowed for this role"},
		{"developer/function_call", `{"messages":[{"role":"developer","content":"hi","function_call":{}}]}`, "messages[0]: function_call is not allowed for this role"},
		{"system/tool_calls", `{"messages":[{"role":"system","content":"hi","tool_calls":[]}]}`, "messages[0]: tool_calls is not allowed for this role"},
		{"system/tool_call_id", `{"messages":[{"role":"system","content":"hi","tool_call_id":"x"}]}`, "messages[0]: tool_call_id is not allowed for this role"},
		{"system/function_call", `{"messages":[{"role":"system","content":"hi","function_call":{}}]}`, "messages[0]: function_call is not allowed for this role"},
		{"user/tool_calls", `{"messages":[{"role":"user","content":"hi","tool_calls":[]}]}`, "messages[0]: tool_calls is not allowed for this role"},
		{"user/tool_call_id", `{"messages":[{"role":"user","content":"hi","tool_call_id":"x"}]}`, "messages[0]: tool_call_id is not allowed for this role"},
		{"user/function_call", `{"messages":[{"role":"user","content":"hi","function_call":{}}]}`, "messages[0]: function_call is not allowed for this role"},
		{"assistant/tool_call_id", `{"messages":[{"role":"assistant","content":"hi","tool_call_id":"x"}]}`, "messages[0]: tool_call_id is not allowed for this role"},
		{"tool/tool_calls", `{"messages":[{"role":"tool","tool_call_id":"c1","content":"hi","tool_calls":[]}]}`, "messages[0]: tool_calls is not allowed for this role"},
		{"tool/function_call", `{"messages":[{"role":"tool","tool_call_id":"c1","content":"hi","function_call":{}}]}`, "messages[0]: function_call is not allowed for this role"},
		{"function/tool_calls", `{"messages":[{"role":"function","name":"fn","content":"hi","tool_calls":[]}]}`, "messages[0]: tool_calls is not allowed for this role"},
		{"function/tool_call_id", `{"messages":[{"role":"function","name":"fn","content":"hi","tool_call_id":"x"}]}`, "messages[0]: tool_call_id is not allowed for this role"},
		{"function/function_call", `{"messages":[{"role":"function","name":"fn","content":"hi","function_call":{}}]}`, "messages[0]: function_call is not allowed for this role"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertValidateMessagesRejects(t, testCase.body, testCase.wantMessage)
		})
	}
}

// --- validateMessages: content rules ---

func TestValidateMessagesRequiresContentForSimpleRoles(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMessage string
	}{
		{"developer missing content", `{"messages":[{"role":"developer"}]}`, "messages[0].content: is required"},
		{"developer null content", `{"messages":[{"role":"developer","content":null}]}`, "messages[0].content: is required"},
		{"system empty content", `{"messages":[{"role":"system","content":""}]}`, "messages[0].content: must not be empty"},
		{"user missing content", `{"messages":[{"role":"user"}]}`, "messages[0].content: is required"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertValidateMessagesRejects(t, testCase.body, testCase.wantMessage)
		})
	}
}

func TestValidateMessagesToolRequiresToolCallIDBeforeContent(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":[{"role":"tool","content":"result"}]}`, "messages[0].tool_call_id: is required")
}

func TestValidateMessagesToolContentRequiredAfterMatch(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":[
		{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"fn"}}]},
		{"role":"tool","tool_call_id":"c1"}
	]}`, "messages[1].content: is required")
}

func TestValidateMessagesFunctionRequiresName(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":[{"role":"function","content":"result"}]}`, "messages[0].name: is required")
}

func TestValidateMessagesFunctionContentRequiredAfterName(t *testing.T) {
	assertValidateMessagesRejects(t, `{"messages":[{"role":"function","name":"fn"}]}`, "messages[0].content: is required")
}

func TestValidateMessagesAssistantContentRules(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMessage string // empty means accepted
	}{
		{
			"content required without tool_calls or function_call",
			`{"messages":[{"role":"assistant"}]}`,
			"messages[0].content: is required unless tool_calls or function_call is provided",
		},
		{
			"content omitted is fine when tool_calls is present",
			`{"messages":[{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"fn"}}]}]}`,
			"",
		},
		{
			"null content is fine when function_call is present",
			`{"messages":[{"role":"assistant","content":null,"function_call":{"name":"fn"}}]}`,
			"",
		},
		{
			"present empty content is rejected even with tool_calls present",
			`{"messages":[{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"fn"}}]}]}`,
			"messages[0].content: must not be empty",
		},
		{
			"plain content without any call payload is accepted",
			`{"messages":[{"role":"assistant","content":"hi"}]}`,
			"",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := validateMessages(document, "")
			if testCase.wantMessage == "" {
				if err != nil {
					t.Fatalf("validateMessages() = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != testCase.wantMessage {
				t.Errorf("validateMessages() error = %v, want %q", err, testCase.wantMessage)
			}
		})
	}
}

// --- validateMessages: assistant tool_calls / function_call shape ---

func TestValidateMessagesAssistantToolCallsShapeErrors(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMessage string
	}{
		{"not an array", `{"messages":[{"role":"assistant","content":null,"tool_calls":"nope"}]}`, "messages[0].tool_calls must be an array"},
		{"empty array", `{"messages":[{"role":"assistant","content":null,"tool_calls":[]}]}`, "messages[0].tool_calls must not be empty"},
		{"element not an object", `{"messages":[{"role":"assistant","content":null,"tool_calls":["x"]}]}`, "messages[0].tool_calls[0] must be an object"},
		{"missing id", `{"messages":[{"role":"assistant","content":null,"tool_calls":[{"type":"function","function":{"name":"fn"}}]}]}`, "messages[0].tool_calls[0].id: is required"},
		{"duplicate id", `{"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"x","type":"function","function":{"name":"fn"}},{"id":"x","type":"function","function":{"name":"fn"}}]}]}`, "messages[0].tool_calls[1].id is duplicated"},
		{"wrong type", `{"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"x","type":"code_interpreter","function":{"name":"fn"}}]}]}`, `messages[0].tool_calls[0].type must be "function"`},
		{"function not an object", `{"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"x","type":"function","function":"nope"}]}]}`, "messages[0].tool_calls[0].function must be an object"},
		{"function missing name", `{"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"x","type":"function","function":{}}]}]}`, "messages[0].tool_calls[0].function.name: is required"},
		{"function arguments not a string", `{"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"x","type":"function","function":{"name":"fn","arguments":42}}]}]}`, "messages[0].tool_calls[0].function.arguments: must be a string"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertValidateMessagesRejects(t, testCase.body, testCase.wantMessage)
		})
	}
}

func TestValidateMessagesAssistantFunctionCallShapeErrors(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMessage string
	}{
		{"not an object", `{"messages":[{"role":"assistant","content":null,"function_call":"x"}]}`, "messages[0].function_call must be an object"},
		{"missing name", `{"messages":[{"role":"assistant","content":null,"function_call":{}}]}`, "messages[0].function_call.name: is required"},
		{"arguments not a string", `{"messages":[{"role":"assistant","content":null,"function_call":{"name":"fn","arguments":42}}]}`, "messages[0].function_call.arguments: must be a string"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertValidateMessagesRejects(t, testCase.body, testCase.wantMessage)
		})
	}
}

func TestValidateMessagesAssistantNullToolCallsAndFunctionCallTreatedAsAbsent(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[{"role":"assistant","content":"hello","tool_calls":null,"function_call":null}]}`)
	if err := validateMessages(document, ""); err != nil {
		t.Fatalf("validateMessages() = %v, want nil", err)
	}
	messages, _ := document.Array("messages")
	message := messages[0].(map[string]any)
	if _, has := message["tool_calls"]; has {
		t.Error("null tool_calls must be deleted from the message")
	}
	if _, has := message["function_call"]; has {
		t.Error("null function_call must be deleted from the message")
	}
}

// --- validateMessages: tool_call_id pending match ---

func TestValidateMessagesToolCallIDMustMatchPendingAssistantCall(t *testing.T) {
	// Reachable only when validateMessages runs without normalizeMessages first: in the
	// real pipeline the orphan-tool-message dropper removes this case earlier.
	assertValidateMessagesRejects(t, `{"messages":[{"role":"tool","tool_call_id":"ghost","content":"x"}]}`,
		"messages[0].tool_call_id does not match any previous assistant tool_calls")
}

func TestValidateMessagesToolCallIDMatchesAndConsumesPending(t *testing.T) {
	document := parseTestDocument(t, `{"messages":[
		{"role":"user","content":"q"},
		{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"fn"}}]},
		{"role":"tool","tool_call_id":"c1","content":"result"}
	]}`)
	if err := validateMessages(document, ""); err != nil {
		t.Fatalf("validateMessages() = %v, want nil", err)
	}
}

func TestValidateMessagesToolCallIDConsumedOnlyOnce(t *testing.T) {
	// The second tool reply to the same id has nothing left pending to match.
	assertValidateMessagesRejects(t, `{"messages":[
		{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"fn"}}]},
		{"role":"tool","tool_call_id":"c1","content":"first"},
		{"role":"tool","tool_call_id":"c1","content":"second"}
	]}`, "messages[2].tool_call_id does not match any previous assistant tool_calls")
}

// --- isEmptyContent / isAssistantTurnEmpty ---

func TestIsEmptyContent(t *testing.T) {
	tests := []struct {
		name    string
		content any
		want    bool
	}{
		{"empty string", "", true},
		{"whitespace string", "   ", true},
		{"non-empty string", "x", false},
		{"empty array", []any{}, true},
		{"non-empty array", []any{1}, false},
		{"nil is absent, not empty", nil, false},
		{"number is not empty", 42, false},
		{"object is not empty", map[string]any{}, false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isEmptyContent(testCase.content); got != testCase.want {
				t.Errorf("isEmptyContent(%#v) = %v, want %v", testCase.content, got, testCase.want)
			}
		})
	}
}

func TestIsAssistantTurnEmpty(t *testing.T) {
	tests := []struct {
		name    string
		message map[string]any
		want    bool
	}{
		{"no fields", map[string]any{}, true},
		{"has content", map[string]any{"content": "hi"}, false},
		{"non-empty tool_calls", map[string]any{"tool_calls": []any{map[string]any{"id": "x"}}}, false},
		{"empty tool_calls array", map[string]any{"tool_calls": []any{}}, true},
		{"nil tool_calls", map[string]any{"tool_calls": nil}, true},
		{"non-empty function_call", map[string]any{"function_call": map[string]any{"name": "fn"}}, false},
		{"empty function_call object", map[string]any{"function_call": map[string]any{}}, true},
		{"nil content", map[string]any{"content": nil}, true},
		{"empty content array", map[string]any{"content": []any{}}, true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isAssistantTurnEmpty(testCase.message); got != testCase.want {
				t.Errorf("isAssistantTurnEmpty(%#v) = %v, want %v", testCase.message, got, testCase.want)
			}
		})
	}
}

// --- fixtures ---

func assistantWithToolCalls(ids ...string) map[string]any {
	calls := make([]any, 0, len(ids))
	for _, id := range ids {
		calls = append(calls, map[string]any{"id": id, "type": "function", "function": map[string]any{"name": "fn"}})
	}
	return map[string]any{"role": "assistant", "content": "", "tool_calls": calls}
}

func toolMessage(toolCallID, content string) map[string]any {
	return map[string]any{"role": "tool", "tool_call_id": toolCallID, "content": content}
}

// rolesOf joins the role of every map entry in messages with ",", using "" for entries
// that aren't messages -- so callers can assert survivor identity and order in one string.
func rolesOf(t *testing.T, messages []any) string {
	t.Helper()
	roles := make([]string, len(messages))
	for i, raw := range messages {
		if message, ok := raw.(map[string]any); ok {
			roles[i], _ = message["role"].(string)
		}
	}
	return strings.Join(roles, ",")
}

func assertValidateMessagesRejects(t *testing.T, body, wantMessage string) {
	t.Helper()
	document := parseTestDocument(t, body)
	err := validateMessages(document, "")
	if err == nil || err.Error() != wantMessage {
		t.Errorf("validateMessages() error = %v, want %q", err, wantMessage)
	}
}
