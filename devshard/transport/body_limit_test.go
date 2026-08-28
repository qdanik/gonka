package transport

import (
	"errors"
	"strings"
	"testing"
)

// A host answering the inference route with a non-stream content type bypasses the SSE line cap, so
// the body read needs a bound of its own.
func TestReadBoundedBody(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		limit int
		want  error
	}{
		{name: "under the limit", body: "abcd", limit: 8},
		{name: "exactly at the limit", body: "abcdefgh", limit: 8},
		{name: "past the limit", body: "abcdefghi", limit: 8, want: ErrResponseBodyTooLarge},
		{name: "an unset limit falls back to the default", body: "abcd", limit: 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			read, err := readBoundedBody(strings.NewReader(testCase.body), testCase.limit)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("readBoundedBody() error = %v, want %v", err, testCase.want)
			}
			if testCase.want == nil && string(read) != testCase.body {
				t.Errorf("readBoundedBody() = %q, want %q", read, testCase.body)
			}
		})
	}
}
