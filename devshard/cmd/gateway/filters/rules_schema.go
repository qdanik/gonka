package filters

import (
	"regexp"
	"strings"
)

var (
	responseFormatNameRegex = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

	// structuredOutputsConstraintFields are mutually exclusive per vLLM's StructuredOutputsParams.__post_init__.
	structuredOutputsConstraintFields = []string{"json", "regex", "choice", "grammar", "json_object", "structural_tag"}

	// structuredOutputsAuxiliaryFields may accompany any single constraint field.
	structuredOutputsAuxiliaryFields = []string{"whitespace_pattern", "disable_any_whitespace", "disable_additional_properties"}

	// structuredOutputsPrivateFields are internal backend state, never client-settable; stripped silently.
	structuredOutputsPrivateFields = []string{"_backend", "_backend_was_auto"}

	structuredOutputsKnownFields = buildStructuredOutputsKnownFields()

	// forbiddenChatTemplateKwargsKeys override apply_hf_chat_template's positional arguments; add_generation_prompt is not banned.
	forbiddenChatTemplateKwargsKeys = map[string]struct{}{
		"chat_template":          {}, // CVE-2025-61620: arbitrary Jinja template
		"tokenize":               {}, // CVE-2025-62426: stalls the request handler
		"tools":                  {},
		"documents":              {},
		"conversation":           {},
		"continue_final_message": {},
		"padding":                {},
		"truncation":             {},
		"max_length":             {},
		"return_tensors":         {},
		"return_dict":            {},
	}
)

// validTools enforces the OpenAI tool contract plus tool_choice cross-field cleanup. See README.md, "`tools` and `tool_choice`".
func validTools(bounds SchemaBounds, defaultToolChoice string) RuleFunc {
	return func(ctx RuleContext) error {
		if choice, _ := ctx.Document.Get("tool_choice"); choice == "required" {
			ctx.Document.Set("tool_choice", defaultToolChoice)
		}
		raw, exists := ctx.Document.Get("tools")
		if !exists {
			return nil
		}
		tools, ok := raw.([]any)
		if !ok {
			return Reject("tools: invalid array shape: must be an array")
		}
		if len(tools) == 0 {
			ctx.Document.Delete("tools")
			ctx.Document.Delete("tool_choice")
			return nil
		}
		if !ctx.Document.Has("tool_choice") && defaultToolChoice != "" {
			ctx.Document.Set("tool_choice", defaultToolChoice)
		}
		for index, item := range tools {
			tool, ok := item.(map[string]any)
			if !ok {
				return Reject("tools[i]: invalid tool shape: tools[%d] must be an object", index)
			}
			toolType, ok := tool["type"].(string)
			if !ok || toolType != "function" {
				return Reject("tools[i].type: must be \"function\" (tools[%d])", index)
			}
			function, ok := tool["function"].(map[string]any)
			if !ok {
				return Reject("tools[i].function: invalid wrapper shape: tools[%d].function must be an object", index)
			}
			name, ok := function["name"].(string)
			if !ok || strings.TrimSpace(name) == "" {
				return Reject("tools[i].function.name: must be a non-empty string (tools[%d])", index)
			}
			delete(function, "strict")
			parameters, ok := function["parameters"].(map[string]any)
			if !ok {
				continue
			}
			if err := bounds.Check(parameters); err != nil {
				return Reject("tools[%d].function.parameters: %v", index, err)
			}
		}
		return nil
	}
}

// validToolChoice accepts "auto" | "none" | {type:"function", function:{name}}; "required" is coerced by validTools first.
func validToolChoice(maxNameLen int) RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get("tool_choice")
		if !exists {
			return nil
		}
		switch typed := raw.(type) {
		case string:
			if typed != "auto" && typed != "none" {
				return Reject("tool_choice: invalid value: must be \"auto\", \"none\", or a function object")
			}
			return nil
		case map[string]any:
			choiceType, _ := typed["type"].(string)
			if choiceType != "function" {
				return Reject("tool_choice.function: invalid shape: type must be \"function\"")
			}
			function, ok := typed["function"].(map[string]any)
			if !ok {
				return Reject("tool_choice.function: invalid shape: function must be an object")
			}
			name, ok := function["name"].(string)
			if !ok || strings.TrimSpace(name) == "" {
				return Reject("tool_choice.function: invalid shape: function.name must be a non-empty string")
			}
			if maxNameLen > 0 && len(name) > maxNameLen {
				return Reject("tool_choice.function: invalid shape: function.name length %d exceeds limit %d", len(name), maxNameLen)
			}
			return nil
		default:
			return Reject("tool_choice: invalid value: must be \"auto\", \"none\", or a function object")
		}
	}
}

// validResponseFormat accepts text/json_object as-is; json_schema also needs a valid name and a bounded schema.
func validResponseFormat(bounds SchemaBounds, maxNameLen int) RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get("response_format")
		if !exists {
			return nil
		}
		responseFormat, ok := raw.(map[string]any)
		if !ok {
			return Reject("response_format: invalid wrapper shape: must be an object")
		}
		responseType, ok := responseFormat["type"].(string)
		if !ok || strings.TrimSpace(responseType) == "" {
			return Reject("response_format.type: missing or unsupported: must be a non-empty string")
		}
		switch responseType {
		case "text", "json_object":
			return nil
		case "json_schema":
			return validResponseFormatJSONSchema(responseFormat, bounds, maxNameLen)
		default:
			return Reject("response_format.type: missing or unsupported: %q is not supported (allowed: text, json_object, json_schema)", responseType)
		}
	}
}

func validResponseFormatJSONSchema(responseFormat map[string]any, bounds SchemaBounds, maxNameLen int) error {
	wrapper, ok := responseFormat["json_schema"].(map[string]any)
	if !ok {
		return Reject("response_format.json_schema: invalid wrapper shape: must be an object")
	}
	name, ok := wrapper["name"].(string)
	if !ok || strings.TrimSpace(name) == "" {
		return Reject("response_format.json_schema.name: invalid: must be a non-empty string")
	}
	if len(name) > maxNameLen {
		return Reject("response_format.json_schema.name: invalid: must be %d characters or fewer", maxNameLen)
	}
	if !responseFormatNameRegex.MatchString(name) {
		return Reject("response_format.json_schema.name: invalid: must match %s", responseFormatNameRegex.String())
	}
	schema, ok := wrapper["schema"].(map[string]any)
	if !ok {
		return Reject("response_format.json_schema.schema: invalid shape: must be an object")
	}
	if err := bounds.Check(schema); err != nil {
		return Reject("response_format.json_schema.schema: %v", err)
	}
	return nil
}

// structuredOutputsBounds bundles the shared JSON-schema bounds plus the choice/grammar/structural_tag caps.
type structuredOutputsBounds struct {
	SchemaBounds
	MaxChoiceEntries    int
	MaxChoiceEntryLen   int
	MaxGrammarLen       int
	MaxGrammarNesting   int
	MaxStructuralTagLen int
}

func buildStructuredOutputsKnownFields() map[string]struct{} {
	known := make(map[string]struct{}, len(structuredOutputsConstraintFields)+len(structuredOutputsAuxiliaryFields))
	for _, field := range structuredOutputsConstraintFields {
		known[field] = struct{}{}
	}
	for _, field := range structuredOutputsAuxiliaryFields {
		known[field] = struct{}{}
	}
	return known
}

// structuredOutputsConstraintValidators pairs each constraint field with its validator so a new one can't be added without.
func structuredOutputsConstraintValidators(bounds structuredOutputsBounds) map[string]func(value any) error {
	return map[string]func(value any) error{
		"json": func(value any) error {
			schema, ok := value.(map[string]any)
			if !ok {
				return Reject("structured_outputs.json: must be an object (string-encoded schemas are not accepted)")
			}
			if err := bounds.Check(schema); err != nil {
				return Reject("structured_outputs.json: %v", err)
			}
			return nil
		},
		"regex": func(value any) error {
			return validStructuredOutputsPattern(value, bounds.MaxPatternLen, "regex")
		},
		"choice": func(value any) error {
			return validStructuredOutputsChoice(value, bounds.MaxChoiceEntries, bounds.MaxChoiceEntryLen, bounds.MaxSizeBytes)
		},
		"grammar": func(value any) error {
			return validStructuredOutputsGrammar(value, bounds.MaxGrammarLen, bounds.MaxGrammarNesting)
		},
		"json_object": func(value any) error {
			if _, ok := value.(bool); !ok {
				return Reject("structured_outputs.json_object: must be a boolean")
			}
			return nil
		},
		"structural_tag": func(value any) error {
			return validStructuredOutputsStructuralTag(value, bounds.MaxStructuralTagLen)
		},
	}
}

// validStructuredOutputs enforces the closed sub-field whitelist and exactly one constraint. See README.md, "`structured_outputs`".
func validStructuredOutputs(bounds structuredOutputsBounds) RuleFunc {
	constraintValidators := structuredOutputsConstraintValidators(bounds)
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get("structured_outputs")
		if !exists {
			return nil
		}
		if ctx.Profile != nil && ctx.Profile.RejectStructuredOutput {
			return Reject("structured_outputs: not supported on this model — use response_format instead")
		}
		object, ok := raw.(map[string]any)
		if !ok {
			return Reject("structured_outputs: invalid wrapper shape: must be an object")
		}
		if ctx.Document.Has("response_format") {
			return Reject("structured_outputs: cannot be combined with response_format")
		}
		for _, key := range structuredOutputsPrivateFields {
			delete(object, key)
		}
		for key := range object {
			if _, known := structuredOutputsKnownFields[key]; !known {
				return Reject("structured_outputs: invalid wrapper shape: unknown sub-field %q", key)
			}
		}
		set := 0
		setNames := make([]string, 0, len(structuredOutputsConstraintFields))
		for _, field := range structuredOutputsConstraintFields {
			if value, ok := object[field]; ok && value != nil {
				set++
				setNames = append(setNames, field)
			}
		}
		if set != 1 {
			if set == 0 {
				return Reject("structured_outputs: exactly one of json/regex/choice/grammar/json_object/structural_tag must be set (got 0)")
			}
			return Reject("structured_outputs: exactly one of json/regex/choice/grammar/json_object/structural_tag must be set (got %d: %s)", set, strings.Join(setNames, ", "))
		}
		for _, field := range structuredOutputsConstraintFields {
			value, ok := object[field]
			if !ok || value == nil {
				continue
			}
			if err := constraintValidators[field](value); err != nil {
				return err
			}
		}
		if value, ok := object["whitespace_pattern"]; ok && value != nil {
			if err := validStructuredOutputsPattern(value, bounds.MaxPatternLen, "whitespace_pattern"); err != nil {
				return err
			}
		}
		for _, flag := range []string{"disable_any_whitespace", "disable_additional_properties"} {
			if value, ok := object[flag]; ok && value != nil {
				if _, ok := value.(bool); !ok {
					return Reject("structured_outputs: flag must be a boolean: %s", flag)
				}
			}
		}
		return nil
	}
}

// validStructuredOutputsPattern backs both "regex" and "whitespace_pattern"; only the message prefix differs.
func validStructuredOutputsPattern(value any, maxLen int, fieldName string) error {
	pattern, ok := value.(string)
	if !ok {
		return Reject("structured_outputs.%s: invalid shape: must be a string", fieldName)
	}
	if len(pattern) > maxLen {
		return Reject("structured_outputs.%s: length exceeded: %d > %d", fieldName, len(pattern), maxLen)
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return Reject("structured_outputs.%s: must compile as a regex: %v", fieldName, err)
	}
	return nil
}

func validStructuredOutputsChoice(value any, maxEntries, maxEntryLen, maxTotalBytes int) error {
	choices, ok := value.([]any)
	if !ok || len(choices) == 0 {
		return Reject("structured_outputs.choice: must be a non-empty string array")
	}
	if len(choices) > maxEntries {
		return Reject("structured_outputs.choice: exceeded size limits: %d entries > %d", len(choices), maxEntries)
	}
	total := 0
	for index, item := range choices {
		entry, ok := item.(string)
		if !ok {
			return Reject("structured_outputs.choice: must be a non-empty string array: choice[%d] must be a string", index)
		}
		if len(entry) > maxEntryLen {
			return Reject("structured_outputs.choice: exceeded size limits: choice[%d] length %d > %d", index, len(entry), maxEntryLen)
		}
		total += len(entry)
		if total > maxTotalBytes {
			return Reject("structured_outputs.choice: exceeded size limits: total length %d > %d", total, maxTotalBytes)
		}
	}
	return nil
}

// validStructuredOutputsGrammar tracks active bracket depth: unmatched opens (the CVE-2026-25048 shape) never come back down.
func validStructuredOutputsGrammar(value any, maxLen, maxNesting int) error {
	grammar, ok := value.(string)
	if !ok {
		return Reject("structured_outputs.grammar: invalid shape: must be a string")
	}
	if len(grammar) > maxLen {
		return Reject("structured_outputs.grammar: length exceeded: %d > %d", len(grammar), maxLen)
	}
	depth, maxDepth := 0, 0
	for _, r := range grammar {
		switch r {
		case '(', '[', '{':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		}
		if maxDepth > maxNesting {
			return Reject("structured_outputs.grammar: nesting depth exceeded: %d > %d", maxDepth, maxNesting)
		}
	}
	return nil
}

// validStructuredOutputsStructuralTag accepts only the object form; a JSON-encoded string crashes the engine.
func validStructuredOutputsStructuralTag(value any, maxLen int) error {
	object, ok := value.(map[string]any)
	if !ok {
		return Reject("structured_outputs.structural_tag: invalid shape: must be an object (a JSON-encoded string crashes the engine)")
	}
	size, err := jsonMarshaledSize(object)
	if err != nil {
		return Reject("structured_outputs.structural_tag: invalid shape: %v", err)
	}
	if size > maxLen {
		return Reject("structured_outputs.structural_tag: length exceeded: %d > %d", size, maxLen)
	}
	return nil
}

// validChatTemplateKwargs uses plain object bounds: this feeds a Jinja renderer, not a grammar compiler.
func validChatTemplateKwargs(bounds ObjectBounds) RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get("chat_template_kwargs")
		if !exists {
			return nil
		}
		object, ok := raw.(map[string]any)
		if !ok {
			return Reject("chat_template_kwargs: invalid wrapper shape: must be an object")
		}
		for key := range object {
			if _, forbidden := forbiddenChatTemplateKwargsKeys[key]; forbidden {
				return Reject("chat_template_kwargs: forbidden key (overrides apply_hf_chat_template positional argument): %q", key)
			}
		}
		if err := bounds.Check(object); err != nil {
			return Reject("chat_template_kwargs: %v", err)
		}
		return nil
	}
}
