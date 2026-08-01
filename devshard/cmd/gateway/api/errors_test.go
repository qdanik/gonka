package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
