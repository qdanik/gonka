package filters

import (
	"bytes"
	"encoding/json"

	"devshard"
)

// MaxNestingDepth bounds JSON nesting before the decode.
const MaxNestingDepth = 32

// Document wraps a decoded request body. No mutex: the pipeline runs single-goroutine per request.
type Document struct {
	raw map[string]any
}

// ParseDocument bounds body's nesting depth, then decodes it into a Document.
func ParseDocument(body []byte) (*Document, error) {
	if err := ensureNestingDepth(body, MaxNestingDepth); err != nil {
		return nil, err
	}
	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, Reject("parse request: %v", err)
	}
	return &Document{raw: raw}, nil
}

// Get returns the raw value stored at key.
func (d *Document) Get(key string) (any, bool) {
	value, ok := d.raw[key]
	return value, ok
}

// Set writes value at key, overwriting any existing value.
func (d *Document) Set(key string, value any) {
	d.raw[key] = value
}

// Delete removes key, if present.
func (d *Document) Delete(key string) {
	delete(d.raw, key)
}

// Has reports whether key is present.
func (d *Document) Has(key string) bool {
	_, ok := d.raw[key]
	return ok
}

// Object returns the value at key as a JSON object, if it is one.
func (d *Document) Object(key string) (map[string]any, bool) {
	value, ok := d.raw[key].(map[string]any)
	return value, ok
}

// Array returns the value at key as a JSON array, if it is one.
func (d *Document) Array(key string) ([]any, bool) {
	value, ok := d.raw[key].([]any)
	return value, ok
}

// Uint returns the value at key coerced to uint64, if present and integer-valued.
func (d *Document) Uint(key string) (uint64, bool) {
	value, ok := d.raw[key]
	if !ok {
		return 0, false
	}
	return devshard.JSONNumericUint64(value)
}

// ObjectField reports absent (present=false), wrong-type (present && !isObject), or the object.
func (d *Document) ObjectField(key string) (obj map[string]any, present, isObject bool) {
	raw, exists := d.raw[key]
	if !exists {
		return nil, false, false
	}
	obj, ok := raw.(map[string]any)
	return obj, true, ok
}

// Raw exposes the underlying map for rule funcs that need direct access.
func (d *Document) Raw() map[string]any {
	return d.raw
}

// Marshal encodes the document; encoding/json sorts map keys, so output is deterministic.
func (d *Document) Marshal() ([]byte, error) {
	body, err := json.Marshal(d.raw)
	if err != nil {
		return nil, Reject("marshal request: %v", err)
	}
	return body, nil
}

// ensureNestingDepth scans body byte-by-byte, tracking brace/bracket depth outside string
// literals, and rejects it once depth exceeds maxDepth -- cheaper than a full decode upfront.
func ensureNestingDepth(body []byte, maxDepth int) error {
	depth := 0
	inString := false
	escaped := false
	for _, b := range body {
		if escaped {
			escaped = false
			continue
		}
		if inString {
			switch b {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				return Reject("request nesting depth exceeds limit %d", maxDepth)
			}
		case '}', ']':
			depth--
			if depth < 0 {
				// Rebase an imbalanced-closer body to 0; the decoder rejects it later.
				depth = 0
			}
		}
	}
	return nil
}
