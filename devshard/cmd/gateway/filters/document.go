package filters

import (
	"bytes"

	"devshard"

	stdjson "encoding/json"

	json "github.com/goccy/go-json"
)

const (
	MaxNestingDepth = 32

	// MaxBodyBytes bounds the raw request body, both at ingest and before the decode. See
	// gateway-request-filtering.md, "Structural bounds before anything is decoded".
	MaxBodyBytes = 10 << 20

	// MaxStructuralNodes bounds the containers and elements the decode allocates. See
	// gateway-request-filtering.md, "Structural bounds before anything is decoded".
	MaxStructuralNodes = 250_000
)

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
	// The decoder here is the standard library's on purpose, alone in this package. Its error text goes
	// to the client verbatim and the golden corpus pins it against the gateway this replaces, so a
	// faster library with different wording would change what every malformed request sees.
	var raw map[string]any
	decoder := stdjson.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, Reject("parse request: %v", err)
	}
	return &Document{raw: raw}, nil
}

func (d *Document) Get(key string) (any, bool) {
	value, ok := d.raw[key]
	return value, ok
}

func (d *Document) Set(key string, value any) {
	d.raw[key] = value
}

func (d *Document) Delete(key string) {
	delete(d.raw, key)
}

func (d *Document) Has(key string) bool {
	_, ok := d.raw[key]
	return ok
}

func (d *Document) Object(key string) (map[string]any, bool) {
	value, ok := d.raw[key].(map[string]any)
	return value, ok
}

func (d *Document) Array(key string) ([]any, bool) {
	value, ok := d.raw[key].([]any)
	return value, ok
}

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

// ensureStructuralBounds scans body byte-by-byte outside string literals in one pass, rejecting it
// once nesting exceeds maxDepth or the container/element count exceeds maxNodes. See
// gateway-request-filtering.md, "Structural bounds before anything is decoded".
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
