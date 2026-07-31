package filters

import (
	"encoding/json"
	"strings"
	"testing"
)

func mustMarshalJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(raw)
}

func toolWithParams(t *testing.T, schema map[string]any) string {
	t.Helper()
	return `{"type":"function","function":{"name":"x","description":"x","parameters":` + mustMarshalJSON(t, schema) + `}}`
}

func testToolsBounds() SchemaBounds {
	return SchemaBounds{MaxDepth: toolsMaxDepth, MaxNodes: toolsMaxNodes, MaxSizeBytes: toolsMaxSizeBytes, MaxBranch: toolsMaxBranch, MaxEnum: toolsMaxEnum, MaxPatternLen: toolsMaxPatternLen}
}

func runRule(t *testing.T, document *Document, param string, rule RuleFunc) error {
	t.Helper()
	return rule(RuleContext{Document: document, Param: param})
}

func TestValidToolsAccepts(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	tests := []struct {
		name string
		body string
	}{
		{"absent", `{"messages":[]}`},
		{"empty array", `{"tools":[]}`},
		{"parameter-less tool with name", `{"tools":[{"type":"function","function":{"name":"x"}}]}`},
		{"simple parameters", `{"tools":[` + toolWithParams(t, map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}}) + `]}`},
		{"two tools both valid", `{"tools":[` + toolWithParams(t, map[string]any{"type": "object"}) + `,` + toolWithParams(t, map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "number"}}}) + `]}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := runRule(t, document, "tools", rule); err != nil {
				t.Fatalf("validTools() = %v, want nil", err)
			}
		})
	}
}

func TestValidToolsRejects(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"tools is object", `{"tools":{"x":1}}`, "tools: invalid array shape: must be an array"},
		{"tools element is not object", `{"tools":["x"]}`, "tools[i]: invalid tool shape: tools[0] must be an object"},
		{"type missing", `{"tools":[{"function":{"name":"x"}}]}`, "tools[i].type: must be \"function\" (tools[0])"},
		{"type not a string", `{"tools":[{"type":1,"function":{"name":"x"}}]}`, "tools[i].type: must be \"function\" (tools[0])"},
		{"type unknown value", `{"tools":[{"type":"plugin","function":{"name":"x"}}]}`, "tools[i].type: must be \"function\" (tools[0])"},
		{"function missing", `{"tools":[{"type":"function"}]}`, "tools[i].function: invalid wrapper shape: tools[0].function must be an object"},
		{"function not an object", `{"tools":[{"type":"function","function":"x"}]}`, "tools[i].function: invalid wrapper shape: tools[0].function must be an object"},
		{"function name missing", `{"tools":[{"type":"function","function":{}}]}`, "tools[i].function.name: must be a non-empty string (tools[0])"},
		{"function name empty", `{"tools":[{"type":"function","function":{"name":""}}]}`, "tools[i].function.name: must be a non-empty string (tools[0])"},
		{"function name whitespace", `{"tools":[{"type":"function","function":{"name":"   "}}]}`, "tools[i].function.name: must be a non-empty string (tools[0])"},
		{"function name not a string", `{"tools":[{"type":"function","function":{"name":42}}]}`, "tools[i].function.name: must be a non-empty string (tools[0])"},
		{"ref hidden in tool parameters", `{"tools":[` + toolWithParams(t, map[string]any{"$ref": "#/foo"}) + `]}`, `tools[0].function.parameters: schema reference keyword is forbidden: "$ref" is not allowed`},
		{"rejects bad schema in second tool", `{"tools":[` + toolWithParams(t, map[string]any{"type": "object"}) + `,` + toolWithParams(t, map[string]any{"$ref": "#/x"}) + `]}`, `tools[1].function.parameters: schema reference keyword is forbidden: "$ref" is not allowed`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := runRule(t, document, "tools", rule)
			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("validTools() = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}

// Proves validTools wires bounds.Check with the tools[i].function.parameters prefix; a
// small MaxDepth keeps the body under the framework's own 32-level nesting cap.
func TestValidToolsAppliesBoundsWithFieldPathPrefix(t *testing.T) {
	shallow := SchemaBounds{MaxDepth: 3, MaxNodes: 100, MaxSizeBytes: 100000, MaxBranch: 100, MaxEnum: 100}
	rule := validTools(shallow, "auto")
	document := parseTestDocument(t, `{"tools":[`+toolWithParams(t, nestedPropertiesSchema(4))+`]}`)
	err := runRule(t, document, "tools", rule)
	want := "tools[0].function.parameters: nesting depth exceeded: limit 3"
	if err == nil || err.Error() != want {
		t.Fatalf("validTools() = %v, want %q", err, want)
	}
}

func TestValidToolsStripsBothWhenToolsEmpty(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[],"tool_choice":"auto"}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if document.Has("tools") || document.Has("tool_choice") {
		t.Fatal("want both tools and tool_choice dropped")
	}
}

func TestValidToolsStripsBothWhenToolsEmptyEvenWithBadToolChoice(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[],"tool_choice":"required"}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if document.Has("tools") || document.Has("tool_choice") {
		t.Fatal("want both tools and tool_choice dropped")
	}
}

func TestValidToolsDefaultsToolChoiceToAutoWhenAbsent(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[{"type":"function","function":{"name":"x"}}]}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if got, _ := document.Get("tool_choice"); got != "auto" {
		t.Fatalf("tool_choice = %v, want auto", got)
	}
}

func TestValidToolsCoercesRequiredToDefault(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[{"type":"function","function":{"name":"x"}}],"tool_choice":"required"}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if got, _ := document.Get("tool_choice"); got != "auto" {
		t.Fatalf("tool_choice = %v, want auto", got)
	}
}

func TestValidToolsCoercesRequiredEvenWithoutTools(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tool_choice":"required"}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if got, _ := document.Get("tool_choice"); got != "auto" {
		t.Fatalf("tool_choice = %v, want auto", got)
	}
}

func TestValidToolsDoesNotOverrideExplicitToolChoice(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[{"type":"function","function":{"name":"x"}}],"tool_choice":"none"}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if got, _ := document.Get("tool_choice"); got != "none" {
		t.Fatalf("tool_choice = %v, want none", got)
	}
}

func TestValidToolsDoesNotTouchToolChoiceWhenToolsAbsent(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"messages":[]}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	if document.Has("tool_choice") {
		t.Fatal("tool_choice must stay absent")
	}
}

func TestValidToolsStripsFunctionStrict(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[{"type":"function","function":{"name":"x","strict":true,"parameters":{"type":"object"}}}]}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	tools, _ := document.Get("tools")
	function := tools.([]any)[0].(map[string]any)["function"].(map[string]any)
	if _, ok := function["strict"]; ok {
		t.Fatal("function.strict must be deleted")
	}
	if function["name"] != "x" {
		t.Fatalf("function.name = %v, want x", function["name"])
	}
	if _, ok := function["parameters"]; !ok {
		t.Fatal("function.parameters must survive")
	}
}

func TestValidToolsStripsFunctionStrictAcrossMultipleTools(t *testing.T) {
	rule := validTools(testToolsBounds(), "auto")
	document := parseTestDocument(t, `{"tools":[{"type":"function","function":{"name":"a","strict":true}},{"type":"function","function":{"name":"b","strict":false}}]}`)
	if err := runRule(t, document, "tools", rule); err != nil {
		t.Fatalf("validTools() = %v, want nil", err)
	}
	tools, _ := document.Get("tools")
	for _, item := range tools.([]any) {
		function := item.(map[string]any)["function"].(map[string]any)
		if _, ok := function["strict"]; ok {
			t.Fatal("function.strict must be deleted on every tool")
		}
	}
}

func TestValidToolChoiceAccepts(t *testing.T) {
	rule := validToolChoice(toolChoiceMaxNameLen)
	tests := []struct {
		name string
		body string
	}{
		{"absent", `{"messages":[]}`},
		{"auto", `{"tool_choice":"auto"}`},
		{"none", `{"tool_choice":"none"}`},
		{"function object", `{"tool_choice":{"type":"function","function":{"name":"web_search"}}}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := runRule(t, document, "tool_choice", rule); err != nil {
				t.Fatalf("validToolChoice() = %v, want nil", err)
			}
		})
	}
}

func TestValidToolChoiceRejects(t *testing.T) {
	rule := validToolChoice(toolChoiceMaxNameLen)
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"unknown string", `{"tool_choice":"force"}`, "tool_choice: invalid value: must be \"auto\", \"none\", or a function object"},
		{"required reaches validator without upstream coerce", `{"tool_choice":"required"}`, "tool_choice: invalid value: must be \"auto\", \"none\", or a function object"},
		{"number", `{"tool_choice":42}`, "tool_choice: invalid value: must be \"auto\", \"none\", or a function object"},
		{"boolean", `{"tool_choice":true}`, "tool_choice: invalid value: must be \"auto\", \"none\", or a function object"},
		{"array", `{"tool_choice":["auto"]}`, "tool_choice: invalid value: must be \"auto\", \"none\", or a function object"},
		{"object missing type", `{"tool_choice":{"function":{"name":"x"}}}`, "tool_choice.function: invalid shape: type must be \"function\""},
		{"object wrong type", `{"tool_choice":{"type":"plugin","function":{"name":"x"}}}`, "tool_choice.function: invalid shape: type must be \"function\""},
		{"object missing function", `{"tool_choice":{"type":"function"}}`, "tool_choice.function: invalid shape: function must be an object"},
		{"object function not object", `{"tool_choice":{"type":"function","function":"x"}}`, "tool_choice.function: invalid shape: function must be an object"},
		{"object missing name", `{"tool_choice":{"type":"function","function":{}}}`, "tool_choice.function: invalid shape: function.name must be a non-empty string"},
		{"object empty name", `{"tool_choice":{"type":"function","function":{"name":""}}}`, "tool_choice.function: invalid shape: function.name must be a non-empty string"},
		{"object whitespace name", `{"tool_choice":{"type":"function","function":{"name":"   "}}}`, "tool_choice.function: invalid shape: function.name must be a non-empty string"},
		{"object non-string name", `{"tool_choice":{"type":"function","function":{"name":42}}}`, "tool_choice.function: invalid shape: function.name must be a non-empty string"},
		{"name exceeds length cap", `{"tool_choice":{"type":"function","function":{"name":"` + strings.Repeat("x", 65) + `"}}}`, "tool_choice.function: invalid shape: function.name length 65 exceeds limit 64"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := runRule(t, document, "tool_choice", rule)
			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("validToolChoice() = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}

func testResponseFormatBounds() SchemaBounds {
	return SchemaBounds{MaxDepth: responseFormatMaxDepth, MaxNodes: responseFormatMaxNodes, MaxSizeBytes: responseFormatMaxSizeBytes, MaxBranch: responseFormatMaxBranch, MaxEnum: responseFormatMaxEnum, MaxPatternLen: responseFormatMaxPatternLen}
}

func jsonSchemaResponseFormatBody(t *testing.T, schema map[string]any) string {
	t.Helper()
	return `{"response_format":{"type":"json_schema","json_schema":{"name":"r","schema":` + mustMarshalJSON(t, schema) + `}}}`
}

func TestValidResponseFormatAccepts(t *testing.T) {
	rule := validResponseFormat(testResponseFormatBounds(), responseFormatMaxNameLen)
	tests := []struct {
		name string
		body string
	}{
		{"absent", `{"messages":[]}`},
		{"type text", `{"response_format":{"type":"text"}}`},
		{"type json_object", `{"response_format":{"type":"json_object"}}`},
		{"json_schema simple", `{"response_format":{"type":"json_schema","json_schema":{"name":"weather_v1","schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}}`},
		{"json_schema name with dots dashes underscores", `{"response_format":{"type":"json_schema","json_schema":{"name":"abc_DEF-1.2","schema":{"type":"object"}}}}`},
		{"schema type as array of primitives", jsonSchemaResponseFormatBody(t, map[string]any{"type": []any{"string", "null"}})},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := runRule(t, document, "response_format", rule); err != nil {
				t.Fatalf("validResponseFormat() = %v, want nil", err)
			}
		})
	}
}

func TestValidResponseFormatRejects(t *testing.T) {
	rule := validResponseFormat(testResponseFormatBounds(), responseFormatMaxNameLen)
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"response_format not an object", `{"response_format":"hi"}`, "response_format: invalid wrapper shape: must be an object"},
		{"type missing", `{"response_format":{"json_schema":{"name":"r","schema":{"type":"object"}}}}`, "response_format.type: missing or unsupported: must be a non-empty string"},
		{"type empty string", `{"response_format":{"type":""}}`, "response_format.type: missing or unsupported: must be a non-empty string"},
		{"unknown type", `{"response_format":{"type":"banana"}}`, `response_format.type: missing or unsupported: "banana" is not supported (allowed: text, json_object, json_schema)`},
		{"json_schema wrapper missing", `{"response_format":{"type":"json_schema"}}`, "response_format.json_schema: invalid wrapper shape: must be an object"},
		{"json_schema missing name", `{"response_format":{"type":"json_schema","json_schema":{"schema":{"type":"object"}}}}`, "response_format.json_schema.name: invalid: must be a non-empty string"},
		{"json_schema whitespace name", `{"response_format":{"type":"json_schema","json_schema":{"name":"   ","schema":{"type":"object"}}}}`, "response_format.json_schema.name: invalid: must be a non-empty string"},
		{"json_schema name has bad chars", `{"response_format":{"type":"json_schema","json_schema":{"name":"bad name","schema":{"type":"object"}}}}`, "response_format.json_schema.name: invalid: must match ^[A-Za-z0-9_.-]+$"},
		{"json_schema name too long", `{"response_format":{"type":"json_schema","json_schema":{"name":"` + strings.Repeat("a", 65) + `","schema":{"type":"object"}}}}`, "response_format.json_schema.name: invalid: must be 64 characters or fewer"},
		{"json_schema missing schema", `{"response_format":{"type":"json_schema","json_schema":{"name":"r"}}}`, "response_format.json_schema.schema: invalid shape: must be an object"},
		{"schema not an object", `{"response_format":{"type":"json_schema","json_schema":{"name":"r","schema":"x"}}}`, "response_format.json_schema.schema: invalid shape: must be an object"},
		{"ref not allowed", jsonSchemaResponseFormatBody(t, map[string]any{"$ref": "#/foo"}), `response_format.json_schema.schema: schema reference keyword is forbidden: "$ref" is not allowed`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := runRule(t, document, "response_format", rule)
			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("validResponseFormat() = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}

// Proves validResponseFormat wires bounds.Check with the response_format.json_schema.schema
// prefix; a small MaxDepth keeps the body under the framework's 32-level nesting cap.
func TestValidResponseFormatAppliesBoundsWithFieldPathPrefix(t *testing.T) {
	shallow := SchemaBounds{MaxDepth: 3, MaxNodes: 100, MaxSizeBytes: 100000, MaxBranch: 100, MaxEnum: 100}
	rule := validResponseFormat(shallow, responseFormatMaxNameLen)
	document := parseTestDocument(t, jsonSchemaResponseFormatBody(t, nestedPropertiesSchema(4)))
	err := runRule(t, document, "response_format", rule)
	want := "response_format.json_schema.schema: nesting depth exceeded: limit 3"
	if err == nil || err.Error() != want {
		t.Fatalf("validResponseFormat() = %v, want %q", err, want)
	}
}

func testStructuredOutputsBounds() structuredOutputsBounds {
	return structuredOutputsBounds{
		SchemaBounds:        SchemaBounds{MaxDepth: structuredOutputsMaxDepth, MaxNodes: structuredOutputsMaxNodes, MaxSizeBytes: structuredOutputsMaxSizeBytes, MaxBranch: structuredOutputsMaxBranch, MaxEnum: structuredOutputsMaxEnum, MaxPatternLen: structuredOutputsMaxPatternLen},
		MaxChoiceEntries:    structuredOutputsMaxChoiceEntries,
		MaxChoiceEntryLen:   structuredOutputsMaxChoiceEntryLen,
		MaxGrammarLen:       structuredOutputsMaxGrammarLen,
		MaxGrammarNesting:   structuredOutputsMaxGrammarNesting,
		MaxStructuralTagLen: structuredOutputsMaxStructuralTagLen,
	}
}

// TestStructuredOutputsConstraintValidatorsCoverAllFields is the lockstep test: every field in
// structuredOutputsConstraintFields must have a validator, so the two can't drift apart.
func TestStructuredOutputsConstraintValidatorsCoverAllFields(t *testing.T) {
	validators := structuredOutputsConstraintValidators(testStructuredOutputsBounds())
	for _, field := range structuredOutputsConstraintFields {
		if validators[field] == nil {
			t.Errorf("structured_outputs constraint %q has no validator", field)
		}
	}
}

func TestValidStructuredOutputsAbsent(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	document := parseTestDocument(t, `{"messages":[]}`)
	if err := runRule(t, document, "structured_outputs", rule); err != nil {
		t.Fatalf("validStructuredOutputs() = %v, want nil", err)
	}
}

func TestValidStructuredOutputsRejectsNonObjectWrapper(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	for _, body := range []string{`{"structured_outputs":"x"}`, `{"structured_outputs":42}`, `{"structured_outputs":[]}`} {
		t.Run(body, func(t *testing.T) {
			document := parseTestDocument(t, body)
			err := runRule(t, document, "structured_outputs", rule)
			want := "structured_outputs: invalid wrapper shape: must be an object"
			if err == nil || err.Error() != want {
				t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
			}
		})
	}
}

func TestValidStructuredOutputsRejectsResponseFormatConflict(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	document := parseTestDocument(t, `{"response_format":{"type":"json_object"},"structured_outputs":{"json_object":true}}`)
	err := runRule(t, document, "structured_outputs", rule)
	want := "structured_outputs: cannot be combined with response_format"
	if err == nil || err.Error() != want {
		t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
	}
}

// Exact message is golden-pinned by schema_structured_outputs_rejected_for_kimi and
// profile_kimi_structured_outputs_rejected.
func TestValidStructuredOutputsRejectsForProfileHook(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	document := parseTestDocument(t, `{"structured_outputs":{"json_object":true}}`)
	err := rule(RuleContext{Document: document, Param: "structured_outputs", Profile: kimiProfile})
	want := "structured_outputs: not supported on this model — use response_format instead"
	if err == nil || err.Error() != want {
		t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
	}
}

func TestValidStructuredOutputsAcceptsWithoutProfileHook(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	for _, profile := range []*Profile{nil, minimaxProfile} {
		document := parseTestDocument(t, `{"structured_outputs":{"json_object":true}}`)
		if err := rule(RuleContext{Document: document, Param: "structured_outputs", Profile: profile}); err != nil {
			t.Errorf("validStructuredOutputs() = %v, want nil for profile %v", err, profile)
		}
	}
}

func TestValidStructuredOutputsEnforcesExactlyOne(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"empty envelope", `{"structured_outputs":{}}`, "structured_outputs: exactly one of json/regex/choice/grammar/json_object/structural_tag must be set (got 0)"},
		{"zero constraints", `{"structured_outputs":{"disable_any_whitespace":true}}`, "structured_outputs: exactly one of json/regex/choice/grammar/json_object/structural_tag must be set (got 0)"},
		{"two constraints", `{"structured_outputs":{"json":{"type":"string"},"regex":"\\d+"}}`, "structured_outputs: exactly one of json/regex/choice/grammar/json_object/structural_tag must be set (got 2: json, regex)"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := runRule(t, document, "structured_outputs", rule)
			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("validStructuredOutputs() = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}

func TestValidStructuredOutputsTreatsExplicitNullAsAbsent(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("null plus one set counts as one", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":null,"regex":"\\d+"}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("all six null counts as zero", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":null,"regex":null,"choice":null,"grammar":null,"json_object":null,"structural_tag":null}}`)
		err := runRule(t, document, "structured_outputs", rule)
		if err == nil || !strings.Contains(err.Error(), "got 0") {
			t.Fatalf("validStructuredOutputs() = %v, want it to contain %q", err, "got 0")
		}
	})
}

func TestValidStructuredOutputsStripsPrivateFields(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	document := parseTestDocument(t, `{"structured_outputs":{"json_object":true,"_backend":"xgrammar","_backend_was_auto":true}}`)
	if err := runRule(t, document, "structured_outputs", rule); err != nil {
		t.Fatalf("validStructuredOutputs() = %v, want nil", err)
	}
	structuredOutputs, _ := document.Object("structured_outputs")
	if _, ok := structuredOutputs["_backend"]; ok {
		t.Fatal("_backend must be stripped")
	}
	if _, ok := structuredOutputs["_backend_was_auto"]; ok {
		t.Fatal("_backend_was_auto must be stripped")
	}
	if structuredOutputs["json_object"] != true {
		t.Fatal("json_object must survive the strip")
	}
}

func TestValidStructuredOutputsRejectsUnknownSubField(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	document := parseTestDocument(t, `{"structured_outputs":{"json_object":true,"backend":"outlines"}}`)
	err := runRule(t, document, "structured_outputs", rule)
	want := `structured_outputs: invalid wrapper shape: unknown sub-field "backend"`
	if err == nil || err.Error() != want {
		t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
	}
}

func TestValidStructuredOutputsJSON(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts a bounded schema", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":{"type":"object","properties":{"x":{"type":"string"}}}}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects a string-encoded schema", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":"{\"type\":\"object\"}"}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.json: must be an object (string-encoded schemas are not accepted)"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("rejects a $ref with the field-path prefix", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":{"type":"object","properties":{"x":{"$ref":"#/definitions/x"}}}}}`)
		err := runRule(t, document, "structured_outputs", rule)
		if err == nil || !strings.HasPrefix(err.Error(), "structured_outputs.json: ") {
			t.Fatalf("validStructuredOutputs() = %v, want it prefixed with %q", err, "structured_outputs.json: ")
		}
	})
}

func TestValidStructuredOutputsRegex(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts a normal pattern", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"regex":"^[a-z]+$"}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects non-string", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"regex":42}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.regex: invalid shape: must be a string"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("rejects over-length", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"regex":"`+strings.Repeat("a", 513)+`"}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.regex: length exceeded: 513 > 512"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("rejects uncompilable", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"regex":"["}}`)
		err := runRule(t, document, "structured_outputs", rule)
		if err == nil || !strings.HasPrefix(err.Error(), "structured_outputs.regex: must compile as a regex: ") {
			t.Fatalf("validStructuredOutputs() = %v, want prefix %q", err, "structured_outputs.regex: must compile as a regex: ")
		}
	})
}

func TestValidStructuredOutputsChoice(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts a bounded list", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"choice":["yes","no","maybe"]}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects empty array", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"choice":[]}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.choice: must be a non-empty string array"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("rejects non-string item", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"choice":["a",42]}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.choice: must be a non-empty string array: choice[1] must be a string"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("accepts exactly the entry-count limit", func(t *testing.T) {
		entries := make([]string, structuredOutputsMaxChoiceEntries)
		for i := range entries {
			entries[i] = `"x"`
		}
		document := parseTestDocument(t, `{"structured_outputs":{"choice":[`+strings.Join(entries, ",")+`]}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects too many entries", func(t *testing.T) {
		entries := make([]string, 257)
		for i := range entries {
			entries[i] = `"x"`
		}
		document := parseTestDocument(t, `{"structured_outputs":{"choice":[`+strings.Join(entries, ",")+`]}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.choice: exceeded size limits: 257 entries > 256"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("accepts a single entry at the exact per-entry byte limit", func(t *testing.T) {
		entry := `"` + strings.Repeat("a", structuredOutputsMaxChoiceEntryLen) + `"`
		document := parseTestDocument(t, `{"structured_outputs":{"choice":[`+entry+`]}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	// Sums entries capped at the per-entry limit up to exactly the shared total-bytes cap.
	t.Run("accepts total length exactly at the shared size cap", func(t *testing.T) {
		var entries []string
		remaining := structuredOutputsMaxSizeBytes
		for remaining > 0 {
			entryLen := structuredOutputsMaxChoiceEntryLen
			if remaining < entryLen {
				entryLen = remaining
			}
			entries = append(entries, `"`+strings.Repeat("a", entryLen)+`"`)
			remaining -= entryLen
		}
		document := parseTestDocument(t, `{"structured_outputs":{"choice":[`+strings.Join(entries, ",")+`]}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects when total length exceeds the shared size cap", func(t *testing.T) {
		entry := `"` + strings.Repeat("a", 1024) + `"`
		entries := make([]string, 20)
		for i := range entries {
			entries[i] = entry
		}
		document := parseTestDocument(t, `{"structured_outputs":{"choice":[`+strings.Join(entries, ",")+`]}}`)
		err := runRule(t, document, "structured_outputs", rule)
		if err == nil || !strings.HasPrefix(err.Error(), "structured_outputs.choice: exceeded size limits: total length ") {
			t.Fatalf("validStructuredOutputs() = %v, want prefix about total length", err)
		}
	})
}

func TestValidStructuredOutputsGrammar(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts a simple grammar", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"grammar":"start: \"hello\""}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("accepts nesting depth exactly at the limit", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"grammar":"`+strings.Repeat("(", structuredOutputsMaxGrammarNesting)+`"}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects deep nesting (CVE-2026-25048 PoC pattern)", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"grammar":"`+strings.Repeat("(", 201)+`"}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.grammar: nesting depth exceeded: 201 > 200"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	t.Run("balanced brackets do not accumulate depth", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"grammar":"`+strings.Repeat("()", 250)+`"}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
}

func TestValidStructuredOutputsJSONObject(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts true and false", func(t *testing.T) {
		for _, body := range []string{`{"structured_outputs":{"json_object":true}}`, `{"structured_outputs":{"json_object":false}}`} {
			document := parseTestDocument(t, body)
			if err := runRule(t, document, "structured_outputs", rule); err != nil {
				t.Fatalf("validStructuredOutputs() = %v, want nil", err)
			}
		}
	})
	t.Run("rejects non-bool", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json_object":"true"}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.json_object: must be a boolean"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
}

func TestValidStructuredOutputsStructuralTag(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts the object form", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"structural_tag":{"type":"structural_tag","structures":[],"triggers":[]}}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects the engine-crashing string form", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"structural_tag":"<tool>...</tool>"}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs.structural_tag: invalid shape: must be an object (a JSON-encoded string crashes the engine)"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
	// Pads a same-shape template to the byte target so the fixture accounts for JSON
	// structural overhead instead of hand-counted padding.
	t.Run("accepts a structural_tag object at exactly the byte limit", func(t *testing.T) {
		template := map[string]any{"type": "structural_tag", "blob": ""}
		baseSize, err := jsonMarshaledSize(template)
		if err != nil {
			t.Fatalf("jsonMarshaledSize: %v", err)
		}
		template["blob"] = strings.Repeat("a", structuredOutputsMaxStructuralTagLen-baseSize)
		document := parseTestDocument(t, `{"structured_outputs":{"structural_tag":`+mustMarshalJSON(t, template)+`}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects an oversize object", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"structural_tag":{"type":"structural_tag","blob":"`+strings.Repeat("a", 4*1024+1)+`"}}}`)
		err := runRule(t, document, "structured_outputs", rule)
		if err == nil || !strings.HasPrefix(err.Error(), "structured_outputs.structural_tag: length exceeded: ") {
			t.Fatalf("validStructuredOutputs() = %v, want prefix about length exceeded", err)
		}
	})
}

func TestValidStructuredOutputsWhitespacePattern(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts a pattern alongside json", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":{"type":"object"},"whitespace_pattern":"\\s*"}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects uncompilable", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json":{"type":"object"},"whitespace_pattern":"["}}`)
		err := runRule(t, document, "structured_outputs", rule)
		if err == nil || !strings.HasPrefix(err.Error(), "structured_outputs.whitespace_pattern: must compile as a regex: ") {
			t.Fatalf("validStructuredOutputs() = %v, want prefix about compiling as a regex", err)
		}
	})
}

func TestValidStructuredOutputsBoolFlags(t *testing.T) {
	rule := validStructuredOutputs(testStructuredOutputsBounds())
	t.Run("accepts bools on both flags", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json_object":true,"disable_any_whitespace":true,"disable_additional_properties":false}}`)
		if err := runRule(t, document, "structured_outputs", rule); err != nil {
			t.Fatalf("validStructuredOutputs() = %v, want nil", err)
		}
	})
	t.Run("rejects a non-bool flag", func(t *testing.T) {
		document := parseTestDocument(t, `{"structured_outputs":{"json_object":true,"disable_any_whitespace":"yes"}}`)
		err := runRule(t, document, "structured_outputs", rule)
		want := "structured_outputs: flag must be a boolean: disable_any_whitespace"
		if err == nil || err.Error() != want {
			t.Fatalf("validStructuredOutputs() = %v, want %q", err, want)
		}
	})
}

func testChatTemplateKwargsBounds() ObjectBounds {
	return ObjectBounds{MaxDepth: chatTemplateKwargsMaxDepth, MaxNodes: chatTemplateKwargsMaxNodes, MaxSizeBytes: chatTemplateKwargsMaxSizeBytes}
}

func TestValidChatTemplateKwargsAccepts(t *testing.T) {
	rule := validChatTemplateKwargs(testChatTemplateKwargsBounds())
	tests := []struct {
		name string
		body string
	}{
		{"absent", `{"messages":[]}`},
		{"empty object", `{"chat_template_kwargs":{}}`},
		{"kimi thinking shape", `{"chat_template_kwargs":{"thinking":true}}`},
		{"qwen enable_thinking plus preserve", `{"chat_template_kwargs":{"enable_thinking":true,"preserve_thinking":false}}`},
		{"add_generation_prompt is allowed", `{"chat_template_kwargs":{"add_generation_prompt":true}}`},
		{"array of strings", `{"chat_template_kwargs":{"tags":["a","b","c"]}}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			if err := runRule(t, document, "chat_template_kwargs", rule); err != nil {
				t.Fatalf("validChatTemplateKwargs() = %v, want nil", err)
			}
		})
	}
}

func TestValidChatTemplateKwargsRejects(t *testing.T) {
	rule := validChatTemplateKwargs(testChatTemplateKwargsBounds())
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"wrapper not object", `{"chat_template_kwargs":"hi"}`, "chat_template_kwargs: invalid wrapper shape: must be an object"},
		{"wrapper is array", `{"chat_template_kwargs":[1,2]}`, "chat_template_kwargs: invalid wrapper shape: must be an object"},
		{"forbidden chat_template key", `{"chat_template_kwargs":{"chat_template":"{% for x in range(99999) %}{% endfor %}"}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "chat_template"`},
		{"forbidden tokenize key", `{"chat_template_kwargs":{"tokenize":true}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "tokenize"`},
		{"forbidden tools key", `{"chat_template_kwargs":{"tools":[]}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "tools"`},
		{"forbidden documents key", `{"chat_template_kwargs":{"documents":[]}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "documents"`},
		{"forbidden conversation key", `{"chat_template_kwargs":{"conversation":[]}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "conversation"`},
		{"forbidden continue_final_message", `{"chat_template_kwargs":{"continue_final_message":true}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "continue_final_message"`},
		{"forbidden max_length", `{"chat_template_kwargs":{"max_length":1024}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "max_length"`},
		{"forbidden alongside legit", `{"chat_template_kwargs":{"thinking":true,"chat_template":"bomb"}}`, `chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): "chat_template"`},
		{"depth exceeds limit", `{"chat_template_kwargs":` + mustMarshalJSON(t, nestedObjectMap(chatTemplateKwargsMaxDepth+1)) + `}`, "chat_template_kwargs: nesting depth exceeded: limit 16"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			document := parseTestDocument(t, testCase.body)
			err := runRule(t, document, "chat_template_kwargs", rule)
			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("validChatTemplateKwargs() = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}
