package filters

import (
	"bytes"
	"encoding/json"

	"devshard"
)

// MaxNestingDepth bounds JSON nesting before the decode.
const MaxNestingDepth = 32

// MaxBodyBytes bounds the raw request body before the decode. A million-token prompt is already
// ~4-6 MiB of text before JSON overhead, so this is sized to reject the absurd rather than to
// throttle load; concurrent memory is bounded by the in-flight input-token budget instead.
const MaxBodyBytes = 32 << 20

// MaxStructuralNodes bounds the containers and elements the decode allocates; a body inside
// MaxBodyBytes can still decode into an order of magnitude more heap than its own bytes.
const MaxStructuralNodes = 250_000

// Document wraps a decoded request body. No mutex: the pipeline runs single-goroutine per request.
type Document struct {
	raw map[string]any
}

// ParseDocument bounds body's size, nesting depth, and node count, then decodes it into a Document.
func ParseDocument(body []byte) (*Document, error) {
	if len(body) > MaxBodyBytes {
		return nil, Reject("request body size %d exceeds limit %d", len(body), MaxBodyBytes)
	}
	if err := ensureStructuralBounds(body, MaxNestingDepth, MaxStructuralNodes); err != nil {
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

// ensureStructuralBounds scans body byte-by-byte outside string literals, rejecting it once
// nesting exceeds maxDepth or the container/element count exceeds maxNodes -- one pass,
// cheaper than a full decode upfront.
func ensureStructuralBounds(body []byte, maxDepth, maxNodes int) error {
	depth := 0
	nodes := 0
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
			continue
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
			continue
		case ',':
		default:
			continue
		}
		nodes++
		if nodes > maxNodes {
			return Reject("request node count exceeds limit %d", maxNodes)
		}
	}
	return nil
}
