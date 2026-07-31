package filters

import (
	"fmt"
	"strings"
	"testing"
)

// nestedObjectBody builds `{"a":{"a":...1...}}` with exactly depth opening braces.
func nestedObjectBody(depth int) []byte {
	var builder strings.Builder
	for i := 0; i < depth; i++ {
		builder.WriteString(`{"a":`)
	}
	builder.WriteString("1")
	for i := 0; i < depth; i++ {
		builder.WriteString("}")
	}
	return []byte(builder.String())
}

func TestDocumentParseRejectsNestingDepthAboveLimit(t *testing.T) {
	_, err := ParseDocument(nestedObjectBody(33))
	if err == nil {
		t.Fatal("ParseDocument() at depth 33: want error, got nil")
	}
	want := "request nesting depth exceeds limit 32"
	if err.Error() != want {
		t.Errorf("ParseDocument() error = %q, want %q", err.Error(), want)
	}
	if got := ErrorStatus(err, 0); got != 400 {
		t.Errorf("ErrorStatus() = %d, want 400", got)
	}
}

func TestDocumentParseAcceptsNestingDepthAtLimit(t *testing.T) {
	document, err := ParseDocument(nestedObjectBody(32))
	if err != nil {
		t.Fatalf("ParseDocument() at depth 32: want nil error, got %v", err)
	}
	if document == nil {
		t.Fatal("ParseDocument() returned nil document with nil error")
	}
}

// paddedBody builds `{"user":"aaa…"}` whose total length is exactly totalBytes.
func paddedBody(totalBytes int) []byte {
	const envelope = `{"user":""}`
	return []byte(`{"user":"` + strings.Repeat("a", totalBytes-len(envelope)) + `"}`)
}

// structuralNodeBody builds `{"stop":[0,0,…]}` holding exactly nodes structural tokens.
func structuralNodeBody(nodes int) []byte {
	var builder strings.Builder
	builder.WriteString(`{"stop":[0`)
	for i := 2; i < nodes; i++ {
		builder.WriteString(",0")
	}
	builder.WriteString("]}")
	return []byte(builder.String())
}

func TestDocumentParseRejectsBodySizeAboveLimit(t *testing.T) {
	oversize := MaxBodyBytes + 1
	_, err := ParseDocument(paddedBody(oversize))
	if err == nil {
		t.Fatalf("ParseDocument() at %d bytes: want error, got nil", oversize)
	}
	want := fmt.Sprintf("request body size %d exceeds limit %d", oversize, MaxBodyBytes)
	if err.Error() != want {
		t.Errorf("ParseDocument() error = %q, want %q", err.Error(), want)
	}
	if got := ErrorStatus(err, 0); got != 400 {
		t.Errorf("ErrorStatus() = %d, want 400", got)
	}
}

func TestDocumentParseAcceptsBodySizeAtLimit(t *testing.T) {
	if _, err := ParseDocument(paddedBody(MaxBodyBytes)); err != nil {
		t.Fatalf("ParseDocument() at %d bytes: want nil error, got %v", MaxBodyBytes, err)
	}
}

func TestDocumentParseRejectsNodeCountAboveLimit(t *testing.T) {
	_, err := ParseDocument(structuralNodeBody(250001))
	if err == nil {
		t.Fatal("ParseDocument() at 250001 nodes: want error, got nil")
	}
	want := "request node count exceeds limit 250000"
	if err.Error() != want {
		t.Errorf("ParseDocument() error = %q, want %q", err.Error(), want)
	}
	if got := ErrorStatus(err, 0); got != 400 {
		t.Errorf("ErrorStatus() = %d, want 400", got)
	}
}

func TestDocumentParseAcceptsNodeCountAtLimit(t *testing.T) {
	if _, err := ParseDocument(structuralNodeBody(250000)); err != nil {
		t.Fatalf("ParseDocument() at 250000 nodes: want nil error, got %v", err)
	}
}

// Structural characters inside string literals are content, not nodes.
func TestDocumentParseIgnoresStructuralCharactersInsideStrings(t *testing.T) {
	body := []byte(`{"user":"` + strings.Repeat(`{[,`, 250000) + `"}`)
	if _, err := ParseDocument(body); err != nil {
		t.Fatalf("ParseDocument() with structural characters inside a string: want nil error, got %v", err)
	}
}

func TestDocumentParseEmptyBodyRejectsWithEOF(t *testing.T) {
	_, err := ParseDocument(nil)
	if err == nil {
		t.Fatal("ParseDocument(nil): want error, got nil")
	}
	want := "parse request: EOF"
	if err.Error() != want {
		t.Errorf("ParseDocument(nil) error = %q, want %q", err.Error(), want)
	}
}

func TestDocumentParseNonObjectBodyRejects(t *testing.T) {
	_, err := ParseDocument([]byte(`"not a json object"`))
	if err == nil {
		t.Fatal("ParseDocument() on a JSON string: want error, got nil")
	}
	want := "parse request: json: cannot unmarshal string into Go value of type map[string]interface {}"
	if err.Error() != want {
		t.Errorf("ParseDocument() error = %q, want %q", err.Error(), want)
	}
}

func TestDocumentGetSetDeleteRoundTrip(t *testing.T) {
	document, err := ParseDocument([]byte(`{"model":"qwen"}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	value, ok := document.Get("model")
	if !ok || value != "qwen" {
		t.Fatalf("Get(model) = (%v, %v), want (qwen, true)", value, ok)
	}

	document.Set("stream", true)
	value, ok = document.Get("stream")
	if !ok || value != true {
		t.Fatalf("Get(stream) after Set = (%v, %v), want (true, true)", value, ok)
	}

	if !document.Has("model") {
		t.Fatal("Has(model) = false before Delete, want true")
	}
	document.Delete("model")
	if document.Has("model") {
		t.Fatal("Has(model) = true after Delete, want false")
	}
	if _, ok := document.Get("model"); ok {
		t.Fatal("Get(model) after Delete: ok = true, want false")
	}
}

func TestDocumentObject(t *testing.T) {
	document, err := ParseDocument([]byte(`{"metadata":{"key":"value"},"model":"qwen"}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	object, ok := document.Object("metadata")
	if !ok || object["key"] != "value" {
		t.Fatalf("Object(metadata) = (%v, %v), want a map with key=value", object, ok)
	}
	if _, ok := document.Object("missing"); ok {
		t.Error("Object(missing) = ok true, want false")
	}
	if _, ok := document.Object("model"); ok {
		t.Error("Object(model) on a string field = ok true, want false")
	}
}

func TestDocumentArray(t *testing.T) {
	document, err := ParseDocument([]byte(`{"messages":["a","b"],"model":"qwen"}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	array, ok := document.Array("messages")
	if !ok || len(array) != 2 || array[0] != "a" {
		t.Fatalf("Array(messages) = (%v, %v), want ([a b], true)", array, ok)
	}
	if _, ok := document.Array("missing"); ok {
		t.Error("Array(missing) = ok true, want false")
	}
	if _, ok := document.Array("model"); ok {
		t.Error("Array(model) on a string field = ok true, want false")
	}
}

func TestDocumentUint(t *testing.T) {
	document, err := ParseDocument([]byte(`{"seed":42,"model":"qwen","negative":-5}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	value, ok := document.Uint("seed")
	if !ok || value != 42 {
		t.Fatalf("Uint(seed) = (%v, %v), want (42, true)", value, ok)
	}
	if _, ok := document.Uint("missing"); ok {
		t.Error("Uint(missing) = ok true, want false")
	}
	if _, ok := document.Uint("model"); ok {
		t.Error("Uint(model) on a string field = ok true, want false")
	}
	if _, ok := document.Uint("negative"); ok {
		t.Error("Uint(negative) on a negative number = ok true, want false")
	}
}

func TestDocumentObjectField(t *testing.T) {
	document, err := ParseDocument([]byte(`{"metadata":{"key":"value"},"model":"qwen"}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	object, present, isObject := document.ObjectField("metadata")
	if !present || !isObject || object["key"] != "value" {
		t.Fatalf("ObjectField(metadata) = (%v, %v, %v), want a map with key=value, true, true", object, present, isObject)
	}
	if _, present, isObject := document.ObjectField("missing"); present || isObject {
		t.Errorf("ObjectField(missing) = (present=%v, isObject=%v), want (false, false)", present, isObject)
	}
	if _, present, isObject := document.ObjectField("model"); !present || isObject {
		t.Errorf("ObjectField(model) on a string field = (present=%v, isObject=%v), want (true, false)", present, isObject)
	}
}

func TestDocumentRawReflectsMutations(t *testing.T) {
	document, err := ParseDocument([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	document.Set("model", "qwen")
	if document.Raw()["model"] != "qwen" {
		t.Errorf("Raw()[model] = %v, want qwen", document.Raw()["model"])
	}
}

// Pins Marshal's exact bytes for a 3-key document (key sort order).
func TestDocumentMarshalSortsKeys(t *testing.T) {
	document, err := ParseDocument([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	document.Set("zebra", "last")
	document.Set("apple", "first")
	document.Set("middle", "second")

	got, err := document.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"apple":"first","middle":"second","zebra":"last"}`
	if string(got) != want {
		t.Errorf("Marshal() = %s, want %s", got, want)
	}
}
