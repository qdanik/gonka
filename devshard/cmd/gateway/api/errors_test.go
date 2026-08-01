package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/limits"
)

// Two documents promise a 429 carries Retry-After, and RateLimitError computes the wait, but nothing
// wrote it: a client told only "too many requests" retries on its own schedule, which is what the
// queue timeout exists to avoid.
func TestARateLimitRejectionCarriesRetryAfter(t *testing.T) {
	recorder := httptest.NewRecorder()

	writeErrorFor(recorder, &limits.RateLimitError{Reason: "queue timeout", RetryAfter: 1500 * time.Millisecond})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
	if got := recorder.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want %q (1.5s rounded up)", got, "2")
	}
}

func TestOtherRejectionsCarryNoRetryAfter(t *testing.T) {
	recorder := httptest.NewRecorder()

	writeErrorFor(recorder, ErrPrivateKeyEnvRequired)

	if got := recorder.Header().Get("Retry-After"); got != "" {
		t.Fatalf("Retry-After = %q on a client-fixable rejection, want none", got)
	}
}

// A host that answers with an error event carrying no message renders its whole raw payload as the
// error text, and that payload is generated content of no fixed size. One line per failed request is
// enough to fill a disk with the model's own output.
func TestLoggedErrorBoundsHostControlledText(t *testing.T) {
	huge := &engine.HostApplicationError{Payload: strings.Repeat("A", 100_000)}

	logged := loggedError(huge)

	if len(logged) > maxLoggedErrorBytes+len("…(truncated)") {
		t.Fatalf("logged error is %d bytes, want it bounded near %d", len(logged), maxLoggedErrorBytes)
	}
	if !strings.HasSuffix(logged, "…(truncated)") {
		t.Fatalf("logged = %q, want it to say it was cut", logged)
	}
}

func TestLoggedErrorLeavesAShortErrorAlone(t *testing.T) {
	if got := loggedError(errors.New("no host available")); got != "no host available" {
		t.Fatalf("loggedError() = %q, want the error unchanged", got)
	}
}

// Cutting a fixed number of bytes lands mid-rune on multi-byte text, which turns a log line into
// invalid UTF-8 that a collector may drop whole. The rune here is three bytes wide because the cap is
// even: a two-byte rune divides into it exactly and would never exercise the boundary at all.
func TestLoggedErrorCutsOnARuneBoundary(t *testing.T) {
	if maxLoggedErrorBytes%3 == 0 {
		t.Fatalf("maxLoggedErrorBytes = %d divides by the test rune width, so this asserts nothing", maxLoggedErrorBytes)
	}
	logged := loggedError(&engine.HostApplicationError{Payload: strings.Repeat("日", 1000)})

	if !utf8.ValidString(logged) {
		t.Fatalf("logged error is not valid UTF-8: %q", logged)
	}
}
