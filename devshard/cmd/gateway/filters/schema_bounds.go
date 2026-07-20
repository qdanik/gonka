package filters

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// Bound families for each schema-carrying field. Kept separate per field even where
// values currently match, so tuning one never silently retunes another.
const (
	chatTemplateKwargsMaxDepth     = 16
	chatTemplateKwargsMaxSizeBytes = 16 * 1024
	chatTemplateKwargsMaxNodes     = 128

	toolsMaxDepth      = 16
	toolsMaxSizeBytes  = 16 * 1024
	toolsMaxNodes      = 256
	toolsMaxBranch     = 16
	toolsMaxEnum       = 256
	toolsMaxPatternLen = 512

	toolChoiceMaxNameLen = 64

	responseFormatMaxDepth      = 16
	responseFormatMaxSizeBytes  = 16 * 1024
	responseFormatMaxNodes      = 128
	responseFormatMaxBranch     = 16
	responseFormatMaxEnum       = 256
	responseFormatMaxNameLen    = 64
	responseFormatMaxPatternLen = 512

	structuredOutputsMaxDepth            = 16
	structuredOutputsMaxSizeBytes        = 16 * 1024
	structuredOutputsMaxNodes            = 128
	structuredOutputsMaxBranch           = 16
	structuredOutputsMaxEnum             = 256
	structuredOutputsMaxPatternLen       = 512
	structuredOutputsMaxChoiceEntries    = 256
	structuredOutputsMaxChoiceEntryLen   = 1024
	structuredOutputsMaxGrammarLen       = 8 * 1024
	structuredOutputsMaxGrammarNesting   = 200
	structuredOutputsMaxStructuralTagLen = 4 * 1024
)

// validSchemaTypes are the JSON-Schema primitives; anything else crashes xgrammar's
// grammar compiler (CVE-2025-48944).
var validSchemaTypes = map[string]struct{}{
	"string": {}, "number": {}, "integer": {}, "object": {}, "boolean": {}, "array": {}, "null": {},
}

var forbiddenSchemaKeys = []string{"$ref", "$defs", "definitions"}
var branchSchemaKeys = []string{"anyOf", "oneOf", "allOf"}

// schemaDataKeys hold literal data, not child schemas — the walker must not recurse into
// them (a $ref hidden inside an enum/default value is just data, not a real reference).
var schemaDataKeys = map[string]struct{}{
	"enum": {}, "const": {}, "default": {}, "examples": {}, "required": {}, "dependentRequired": {},
}

// schemaChildMapKeys hold name->schema maps; each value is walked as its own child schema
// and the wrapper map itself is not counted as an extra node.
var schemaChildMapKeys = map[string]struct{}{
	"properties": {}, "patternProperties": {}, "dependentSchemas": {},
}

// SchemaBounds bounds a JSON-Schema payload's depth, node count, serialized size,
// anyOf/oneOf/allOf arm count, and enum size; bans $ref/$defs/definitions; and validates `type`/`pattern`.
type SchemaBounds struct {
	MaxDepth      int
	MaxNodes      int
	MaxSizeBytes  int
	MaxBranch     int
	MaxEnum       int
	MaxPatternLen int
}

// Check walks schema for structural violations, then checks serialized size — walk runs
// first so a deep/wide attack payload never pays for a full json.Marshal.
func (b SchemaBounds) Check(schema map[string]any) error {
	var nodes int
	if err := b.walk(schema, 1, &nodes); err != nil {
		return err
	}
	return checkSize(schema, b.MaxSizeBytes)
}

func (b SchemaBounds) walk(schema any, depth int, nodes *int) error {
	if depth > b.MaxDepth {
		return depthExceededError(b.MaxDepth)
	}
	object, ok := schema.(map[string]any)
	if !ok {
		return nil
	}
	*nodes++
	if *nodes > b.MaxNodes {
		return nodesExceededError(b.MaxNodes)
	}
	for _, forbidden := range forbiddenSchemaKeys {
		if _, exists := object[forbidden]; exists {
			return fmt.Errorf("schema reference keyword is forbidden: %q is not allowed", forbidden)
		}
	}
	if enum, ok := object["enum"].([]any); ok && len(enum) > b.MaxEnum {
		return fmt.Errorf("enum size exceeded: limit %d", b.MaxEnum)
	}
	if err := validateSchemaType(object); err != nil {
		return err
	}
	if err := b.validateSchemaPattern(object); err != nil {
		return err
	}
	for _, branchKey := range branchSchemaKeys {
		if arms, ok := object[branchKey].([]any); ok && len(arms) > b.MaxBranch {
			return fmt.Errorf("schema branch arms exceeded: %s limit %d", branchKey, b.MaxBranch)
		}
	}
	for key, value := range object {
		if _, isData := schemaDataKeys[key]; isData {
			continue
		}
		switch typed := value.(type) {
		case map[string]any:
			if _, isChildMap := schemaChildMapKeys[key]; isChildMap {
				for _, child := range typed {
					if err := b.walk(child, depth+1, nodes); err != nil {
						return err
					}
				}
			} else if err := b.walk(typed, depth+1, nodes); err != nil {
				return err
			}
		case []any:
			for _, child := range typed {
				if err := b.walk(child, depth+1, nodes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateSchemaType rejects a `type` that is not a JSON-Schema primitive, or an array
// containing one — an absent `type` is fine. See CVE-2025-48944.
func validateSchemaType(object map[string]any) error {
	raw, present := object["type"]
	if !present {
		return nil
	}
	switch typed := raw.(type) {
	case string:
		if _, ok := validSchemaTypes[typed]; !ok {
			return fmt.Errorf("schema type is not a valid JSON-Schema primitive: %q", typed)
		}
	case []any:
		for _, entry := range typed {
			name, ok := entry.(string)
			if !ok {
				return fmt.Errorf("schema type is not a valid JSON-Schema primitive: array elements must be strings")
			}
			if _, ok := validSchemaTypes[name]; !ok {
				return fmt.Errorf("schema type is not a valid JSON-Schema primitive: %q", name)
			}
		}
	default:
		return fmt.Errorf("schema type is not a valid JSON-Schema primitive: must be a string or array of strings")
	}
	return nil
}

// validateSchemaPattern rejects a `pattern` over MaxPatternLen or that fails to compile
// (CVE-2025-48944); MaxPatternLen <= 0 disables the check.
func (b SchemaBounds) validateSchemaPattern(object map[string]any) error {
	if b.MaxPatternLen <= 0 {
		return nil
	}
	raw, present := object["pattern"]
	if !present {
		return nil
	}
	pattern, ok := raw.(string)
	if !ok {
		return fmt.Errorf("schema pattern is not a valid regular expression: must be a string")
	}
	if len(pattern) > b.MaxPatternLen {
		return fmt.Errorf("schema pattern is not a valid regular expression: length %d exceeds limit %d", len(pattern), b.MaxPatternLen)
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return fmt.Errorf("schema pattern is not a valid regular expression: %v", err)
	}
	return nil
}

// ObjectBounds bounds an arbitrary nested JSON object with no JSON-Schema semantics — no
// $ref ban, no type/pattern/enum/branch checks.
type ObjectBounds struct {
	MaxDepth     int
	MaxNodes     int
	MaxSizeBytes int
}

// Check walks obj for depth/node violations, then checks serialized size.
func (b ObjectBounds) Check(obj map[string]any) error {
	var nodes int
	if err := b.walk(obj, 1, &nodes); err != nil {
		return err
	}
	return checkSize(obj, b.MaxSizeBytes)
}

func (b ObjectBounds) walk(value any, depth int, nodes *int) error {
	if depth > b.MaxDepth {
		return depthExceededError(b.MaxDepth)
	}
	switch typed := value.(type) {
	case map[string]any:
		*nodes++
		if *nodes > b.MaxNodes {
			return nodesExceededError(b.MaxNodes)
		}
		for _, child := range typed {
			if err := b.walk(child, depth+1, nodes); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := b.walk(child, depth+1, nodes); err != nil {
				return err
			}
		}
	}
	return nil
}

// depthExceededError and nodesExceededError: shared so both walkers render identical rejections.
func depthExceededError(maxDepth int) error {
	return fmt.Errorf("nesting depth exceeded: limit %d", maxDepth)
}

func nodesExceededError(maxNodes int) error {
	return fmt.Errorf("node count exceeded: limit %d", maxNodes)
}

// checkSize rejects when v's marshaled size exceeds maxSizeBytes; maxSizeBytes <= 0 disables it.
func checkSize(v any, maxSizeBytes int) error {
	if maxSizeBytes <= 0 {
		return nil
	}
	size, err := jsonMarshaledSize(v)
	if err != nil {
		return fmt.Errorf("cannot be serialized: %v", err)
	}
	if size > maxSizeBytes {
		return fmt.Errorf("serialized size exceeded: limit %d bytes", maxSizeBytes)
	}
	return nil
}

type countingWriter struct{ n int }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += len(p)
	return len(p), nil
}

// jsonMarshaledSize returns len(json.Marshal(v)) without allocating the output slice.
// Encoder.Encode trails a newline that Marshal omits, so subtract one.
func jsonMarshaledSize(v any) (int, error) {
	var counter countingWriter
	if err := json.NewEncoder(&counter).Encode(v); err != nil {
		return 0, err
	}
	return counter.n - 1, nil
}
