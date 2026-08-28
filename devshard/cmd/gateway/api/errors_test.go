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
	"devshard/cmd/gateway/scheduler"
)

// Two documents promise a 429 carries Retry-After, and RateLimitError computes the wait, but nothing
// wrote it: a client told only "too many requests" retries on its own schedule, which is what the
// queue timeout exists to avoid.
func TestARateLimitRejectionCarriesRetryAfter(t *testing.T) {
	recorder := httptest.NewRecorder()

	writeErrorFor(recorder, &limits.RateLimitError{Reason: "queue timeout", RetryAfter: 1500 * time.Millisecond})

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
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

// A request refused because every host is at capacity is the gateway's own admission refusing, not an upstream answering badly. It used
// to render as 502 with no Retry-After, so a client read it as a broken node and retried at once --
// which is what a load test measured as 35 of 100 requests failing.
func TestAHostlessRequestIsOurRefusalNotAnUpstreamFailure(t *testing.T) {
	recorder := httptest.NewRecorder()

	writeErrorFor(recorder, scheduler.ErrHostsBusy)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want a wait the client can act on", got)
	}
}

// The chat path must not answer 429: it means "you exceeded a quota", and a client that ran into the
// shard's own capacity exceeded nothing. Every capacity refusal is 503 with a hint of when to return.
//
// This contradicts gateway-request-lifecycle.md, which promises 429 with Retry-After for the same
// three rejections, and the old gateway, which answered 429. Whichever wins, both must say it.
func TestEveryCapacityRefusalAnswersUnavailableWithAWait(t *testing.T) {
	refusals := []error{
		&limits.RateLimitError{Reason: "too many concurrent requests"},
		scheduler.ErrHostsBusy,
		scheduler.ErrNoEscrowCapacity,
		scheduler.ErrEscrowBusy,
	}
	for _, refusal := range refusals {
		recorder := httptest.NewRecorder()
		writeErrorFor(recorder, refusal)
		if recorder.Code == http.StatusTooManyRequests {
			t.Fatalf("%v answered 429", refusal)
		}
		if recorder.Header().Get("Retry-After") == "" {
			t.Fatalf("%v carried no Retry-After: a client cannot tell when to come back", refusal)
		}
	}
}
