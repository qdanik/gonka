package filters

import (
	"strconv"
	"strings"
	"testing"
)

// nestedPropertiesSchema builds a properties.x chain `depth` levels deep.
func nestedPropertiesSchema(depth int) map[string]any {
	if depth <= 1 {
		return map[string]any{"type": "object"}
	}
	return map[string]any{"type": "object", "properties": map[string]any{"x": nestedPropertiesSchema(depth - 1)}}
}

// nestedKeywordSchema builds a chain `depth` levels deep through an arbitrary schema-valued keyword.
func nestedKeywordSchema(keyword string, depth int) map[string]any {
	if depth <= 1 {
		return map[string]any{"type": "object"}
	}
	return map[string]any{keyword: nestedKeywordSchema(keyword, depth-1)}
}

// manyPropertiesSchema is a root object with `count` empty-schema properties: count+1 nodes.
func manyPropertiesSchema(count int) map[string]any {
	properties := make(map[string]any, count)
	for i := range count {
		properties["k"+strconv.Itoa(i)] = map[string]any{}
	}
	return map[string]any{"properties": properties}
}

// nestedObjectMap builds a plain (non-schema) object chain `depth` levels deep.
func nestedObjectMap(depth int) map[string]any {
	if depth <= 1 {
		return map[string]any{}
	}
	return map[string]any{"x": nestedObjectMap(depth - 1)}
}

// flatObjectMap is a root object with `count` empty-map entries: count+1 nodes.
func flatObjectMap(count int) map[string]any {
	object := make(map[string]any, count)
	for i := range count {
		object["k"+strconv.Itoa(i)] = map[string]any{}
	}
	return object
}

func generousBounds() SchemaBounds {
	return SchemaBounds{MaxDepth: 100, MaxNodes: 100, MaxSizeBytes: 1 << 20, MaxBranch: 100, MaxEnum: 100, MaxPatternLen: 100}
}

func TestSchemaBoundsCheckDepth(t *testing.T) {
	bounds := generousBounds()
	bounds.MaxDepth = 4
	wantErr := "nesting depth exceeded: limit 4"

	t.Run("properties chain at limit accepted", func(t *testing.T) {
		if err := bounds.Check(nestedPropertiesSchema(4)); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("properties chain one over limit rejected", func(t *testing.T) {
		err := bounds.Check(nestedPropertiesSchema(5))
		if err == nil || err.Error() != wantErr {
			t.Fatalf("Check() = %v, want %q", err, wantErr)
		}
	})
	t.Run("keyword chain (not) at limit accepted", func(t *testing.T) {
		if err := bounds.Check(nestedKeywordSchema("not", 4)); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("keyword chain (not) one over limit rejected", func(t *testing.T) {
		err := bounds.Check(nestedKeywordSchema("not", 5))
		if err == nil || err.Error() != wantErr {
			t.Fatalf("Check() = %v, want %q", err, wantErr)
		}
	})
	t.Run("keyword chain (if) one over limit rejected", func(t *testing.T) {
		// A second, distinct keyword proves the walker isn't special-casing "not" alone.
		err := bounds.Check(nestedKeywordSchema("if", 5))
		if err == nil || err.Error() != wantErr {
			t.Fatalf("Check() = %v, want %q", err, wantErr)
		}
	})
}

func TestSchemaBoundsCheckNodes(t *testing.T) {
	bounds := generousBounds()
	bounds.MaxNodes = 5

	t.Run("at limit accepted", func(t *testing.T) {
		if err := bounds.Check(manyPropertiesSchema(4)); err != nil { // 1 root + 4 children = 5
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("one over limit rejected", func(t *testing.T) {
		err := bounds.Check(manyPropertiesSchema(5)) // 1 root + 5 children = 6
		want := "node count exceeded: limit 5"
		if err == nil || err.Error() != want {
			t.Fatalf("Check() = %v, want %q", err, want)
		}
	})
}

func TestSchemaBoundsCheckSize(t *testing.T) {
	schema := map[string]any{"type": "string", "description": "abcdef"}
	size, err := jsonMarshaledSize(schema)
	if err != nil {
		t.Fatalf("jsonMarshaledSize: %v", err)
	}
	base := generousBounds()

	t.Run("exactly at limit accepted", func(t *testing.T) {
		bounds := base
		bounds.MaxSizeBytes = size
		if err := bounds.Check(schema); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("one byte over limit rejected", func(t *testing.T) {
		bounds := base
		bounds.MaxSizeBytes = size - 1
		err := bounds.Check(schema)
		want := "serialized size exceeded: limit " + strconv.Itoa(size-1) + " bytes"
		if err == nil || err.Error() != want {
			t.Fatalf("Check() = %v, want %q", err, want)
		}
	})
	t.Run("zero disables the check", func(t *testing.T) {
		bounds := base
		bounds.MaxSizeBytes = 0
		if err := bounds.Check(schema); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
}

func TestSchemaBoundsCheckBranch(t *testing.T) {
	for _, branchKey := range []string{"anyOf", "oneOf", "allOf"} {
		t.Run(branchKey, func(t *testing.T) {
			arms := []any{map[string]any{"type": "string"}, map[string]any{"type": "string"}, map[string]any{"type": "string"}}
			schema := map[string]any{branchKey: arms}

			bounds := generousBounds()
			bounds.MaxBranch = 3
			if err := bounds.Check(schema); err != nil {
				t.Fatalf("Check() at limit = %v, want nil", err)
			}

			bounds.MaxBranch = 2
			err := bounds.Check(schema)
			want := "schema branch arms exceeded: " + branchKey + " limit 2"
			if err == nil || err.Error() != want {
				t.Fatalf("Check() over limit = %v, want %q", err, want)
			}
		})
	}
}

func TestSchemaBoundsCheckEnum(t *testing.T) {
	bounds := generousBounds()
	bounds.MaxEnum = 3
	t.Run("at limit accepted", func(t *testing.T) {
		if err := bounds.Check(map[string]any{"enum": []any{1, 2, 3}}); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("one over limit rejected", func(t *testing.T) {
		err := bounds.Check(map[string]any{"enum": []any{1, 2, 3, 4}})
		want := "enum size exceeded: limit 3"
		if err == nil || err.Error() != want {
			t.Fatalf("Check() = %v, want %q", err, want)
		}
	})
}

func TestSchemaBoundsCheckForbiddenRef(t *testing.T) {
	bounds := generousBounds()
	tests := []struct {
		name   string
		schema map[string]any
		want   string
	}{
		{"$ref at top level", map[string]any{"$ref": "#/foo"}, `schema reference keyword is forbidden: "$ref" is not allowed`},
		{"$defs at top level", map[string]any{"$defs": map[string]any{"x": map[string]any{}}}, `schema reference keyword is forbidden: "$defs" is not allowed`},
		{"definitions at top level", map[string]any{"definitions": map[string]any{"x": map[string]any{}}}, `schema reference keyword is forbidden: "definitions" is not allowed`},
		{"$ref hidden under not", map[string]any{"not": map[string]any{"$ref": "#/x"}}, `schema reference keyword is forbidden: "$ref" is not allowed`},
		{"$ref hidden under a properties value", map[string]any{"properties": map[string]any{"a": map[string]any{"$ref": "#/x"}}}, `schema reference keyword is forbidden: "$ref" is not allowed`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := bounds.Check(testCase.schema)
			if err == nil || err.Error() != testCase.want {
				t.Fatalf("Check() = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestSchemaBoundsCheckType(t *testing.T) {
	bounds := generousBounds()
	acceptTests := []struct {
		name   string
		schema map[string]any
	}{
		{"valid primitive string", map[string]any{"type": "string"}},
		{"valid array of primitives", map[string]any{"type": []any{"string", "null"}}},
		{"absent type", map[string]any{}},
	}
	for _, testCase := range acceptTests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := bounds.Check(testCase.schema); err != nil {
				t.Fatalf("Check() = %v, want nil", err)
			}
		})
	}

	rejectTests := []struct {
		name   string
		schema map[string]any
		want   string
	}{
		{"unknown type string", map[string]any{"type": "something"}, `schema type is not a valid JSON-Schema primitive: "something"`},
		{"unknown type in array", map[string]any{"type": []any{"string", "weird"}}, `schema type is not a valid JSON-Schema primitive: "weird"`},
		{"non-string array element", map[string]any{"type": []any{"string", 1}}, "schema type is not a valid JSON-Schema primitive: array elements must be strings"},
		{"non-string non-array", map[string]any{"type": true}, "schema type is not a valid JSON-Schema primitive: must be a string or array of strings"},
		{"nested bad type", map[string]any{"properties": map[string]any{"x": map[string]any{"type": "not_a_type"}}}, `schema type is not a valid JSON-Schema primitive: "not_a_type"`},
	}
	for _, testCase := range rejectTests {
		t.Run(testCase.name, func(t *testing.T) {
			err := bounds.Check(testCase.schema)
			if err == nil || err.Error() != testCase.want {
				t.Fatalf("Check() = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestSchemaBoundsCheckPattern(t *testing.T) {
	bounds := generousBounds()
	bounds.MaxPatternLen = 16

	t.Run("compiling pattern accepted", func(t *testing.T) {
		if err := bounds.Check(map[string]any{"type": "string", "pattern": "^[a-z]+$"}); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("non-compiling pattern rejected", func(t *testing.T) {
		err := bounds.Check(map[string]any{"type": "string", "pattern": "("})
		want := "schema pattern is not a valid regular expression: error parsing regexp: missing closing ): `(`"
		if err == nil || err.Error() != want {
			t.Fatalf("Check() = %v, want %q", err, want)
		}
	})
	t.Run("pattern length exactly at limit accepted", func(t *testing.T) {
		if err := bounds.Check(map[string]any{"pattern": strings.Repeat("a", bounds.MaxPatternLen)}); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
	t.Run("too long pattern rejected", func(t *testing.T) {
		err := bounds.Check(map[string]any{"pattern": strings.Repeat("a", 17)})
		want := "schema pattern is not a valid regular expression: length 17 exceeds limit 16"
		if err == nil || err.Error() != want {
			t.Fatalf("Check() = %v, want %q", err, want)
		}
	})
	t.Run("non-string pattern rejected", func(t *testing.T) {
		err := bounds.Check(map[string]any{"pattern": 42})
		want := "schema pattern is not a valid regular expression: must be a string"
		if err == nil || err.Error() != want {
			t.Fatalf("Check() = %v, want %q", err, want)
		}
	})
	t.Run("zero MaxPatternLen disables the check", func(t *testing.T) {
		disabled := bounds
		disabled.MaxPatternLen = 0
		if err := disabled.Check(map[string]any{"pattern": "("}); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	})
}

func TestSchemaBoundsCheckDataKeysNotWalked(t *testing.T) {
	// MaxDepth=1 would reject any nested schema; a $ref hidden inside a data key must survive.
	bounds := generousBounds()
	bounds.MaxDepth = 1
	for _, key := range []string{"enum", "const", "default", "examples", "required", "dependentRequired"} {
		t.Run(key, func(t *testing.T) {
			schema := map[string]any{key: []any{map[string]any{"$ref": "#/x"}}}
			if err := bounds.Check(schema); err != nil {
				t.Fatalf("Check() = %v, want nil (data key %q must not be walked as schema)", err, key)
			}
		})
	}
}

func TestSchemaBoundsCheckChildMapKeysWalkValues(t *testing.T) {
	bounds := generousBounds()
	for _, key := range []string{"properties", "patternProperties", "dependentSchemas"} {
		t.Run(key, func(t *testing.T) {
			schema := map[string]any{key: map[string]any{"a": map[string]any{"$ref": "#/x"}}}
			err := bounds.Check(schema)
			want := `schema reference keyword is forbidden: "$ref" is not allowed`
			if err == nil || err.Error() != want {
				t.Fatalf("Check() = %v, want %q (child map key %q must walk its values)", err, want, key)
			}
		})
	}
}

func TestObjectBoundsCheckDepth(t *testing.T) {
	bounds := ObjectBounds{MaxDepth: 3, MaxNodes: 100, MaxSizeBytes: 1 << 20}
	if err := bounds.Check(nestedObjectMap(3)); err != nil {
		t.Fatalf("Check() at limit = %v, want nil", err)
	}
	err := bounds.Check(nestedObjectMap(4))
	want := "nesting depth exceeded: limit 3"
	if err == nil || err.Error() != want {
		t.Fatalf("Check() over limit = %v, want %q", err, want)
	}
}

func TestObjectBoundsCheckNodes(t *testing.T) {
	bounds := ObjectBounds{MaxDepth: 100, MaxNodes: 3, MaxSizeBytes: 1 << 20}
	if err := bounds.Check(flatObjectMap(2)); err != nil { // 1 root + 2 children = 3
		t.Fatalf("Check() at limit = %v, want nil", err)
	}
	err := bounds.Check(flatObjectMap(3)) // 1 root + 3 children = 4
	want := "node count exceeded: limit 3"
	if err == nil || err.Error() != want {
		t.Fatalf("Check() over limit = %v, want %q", err, want)
	}
}

func TestObjectBoundsCheckSize(t *testing.T) {
	obj := map[string]any{"thinking": true, "note": "abcdef"}
	size, err := jsonMarshaledSize(obj)
	if err != nil {
		t.Fatalf("jsonMarshaledSize: %v", err)
	}
	bounds := ObjectBounds{MaxDepth: 10, MaxNodes: 10, MaxSizeBytes: size}
	if err := bounds.Check(obj); err != nil {
		t.Fatalf("Check() at limit = %v, want nil", err)
	}
	bounds.MaxSizeBytes = size - 1
	got := bounds.Check(obj)
	want := "serialized size exceeded: limit " + strconv.Itoa(size-1) + " bytes"
	if got == nil || got.Error() != want {
		t.Fatalf("Check() over limit = %v, want %q", got, want)
	}
}

// Guards against a future SchemaBounds/ObjectBounds merge silently adding schema
// rejections ($ref/type/pattern/enum) to chat_template_kwargs.
func TestObjectBoundsCheckHasNoSchemaSemantics(t *testing.T) {
	bounds := ObjectBounds{MaxDepth: 10, MaxNodes: 10, MaxSizeBytes: 1 << 20}
	obj := map[string]any{
		"$ref":    "not a schema reference here",
		"type":    "not-a-json-schema-primitive",
		"pattern": "(",
		"enum":    []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	if err := bounds.Check(obj); err != nil {
		t.Fatalf("Check() = %v, want nil (ObjectBounds must not apply schema semantics)", err)
	}
}
