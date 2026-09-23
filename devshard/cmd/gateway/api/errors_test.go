package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/scheduler"
	"devshard/types"
)

func newErrorsTestServer() *Server {
	defaults := config.Defaults()
	return &Server{config: config.NewHolder(&defaults)}
}

// Two documents promise a 429 carries Retry-After, and RateLimitError computes the wait, but nothing
// wrote it: a client told only "too many requests" retries on its own schedule, which is what the
// queue timeout exists to avoid.
func TestARateLimitRejectionCarriesRetryAfter(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, &limits.RateLimitError{Reason: "queue timeout", RetryAfter: 1500 * time.Millisecond})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
	if got := recorder.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want %q (1.5s rounded up)", got, "2")
	}
}

func TestOtherRejectionsCarryNoRetryAfter(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, ErrPrivateKeyEnvRequired)

	if got := recorder.Header().Get("Retry-After"); got != "" {
		t.Fatalf("Retry-After = %q on a client-fixable rejection, want none", got)
	}
}

// A request refused because every host is at capacity is the gateway's own admission refusing, not an upstream answering badly. It used
// to render as 502 with no Retry-After, so a client read it as a broken node and retried at once --
// which is what a load test measured as 35 of 100 requests failing.
func TestAHostlessRequestIsOurRefusalNotAnUpstreamFailure(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, scheduler.ErrHostsBusy)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want a wait the client can act on", got)
	}
}

// A client that ran into the shard's own capacity exceeded no quota, so 429 would misname it. The old
// gateway drew the same line: its limiter answered 429, its admission control 503.
func TestACapacityRefusalAnswersUnavailableWithAWait(t *testing.T) {
	refusals := []error{scheduler.ErrHostsBusy, scheduler.ErrNoEscrowCapacity, scheduler.ErrEscrowBusy}
	for _, refusal := range refusals {
		recorder := httptest.NewRecorder()
		newErrorsTestServer().writeErrorFor(recorder, refusal)
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("%v answered %d, want 503", refusal, recorder.Code)
		}
		if recorder.Header().Get("Retry-After") == "" {
			t.Fatalf("%v carried no Retry-After: a client cannot tell when to come back", refusal)
		}
	}
}

// ModelUnavailableError takes the escrow tick as Retry-After, as ChainStaleError takes the chain poll interval.
func TestAModelUnavailableRejectionCarriesTheEscrowTickInterval(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, &ModelUnavailableError{Model: "qwen"})

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	want := strconv.Itoa(int(escrow.TickInterval.Seconds()))
	if got := recorder.Header().Get("Retry-After"); got != want {
		t.Fatalf("Retry-After = %q, want %q (the escrow tick interval)", got, want)
	}
}

// A balance is restored by a replacement escrow or a settling inference, so a second is a retry that
// cannot succeed; the refusal takes the same tick a model with no runtime does.
func TestAFundingRefusalCarriesTheEscrowTickInterval(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, fmt.Errorf("%w: %w", scheduler.ErrNoEscrowCapacity, types.ErrInsufficientBalance))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	want := strconv.Itoa(int(escrow.TickInterval.Seconds()))
	if got := recorder.Header().Get("Retry-After"); got != want {
		t.Fatalf("Retry-After = %q, want %q (the escrow tick interval)", got, want)
	}
}

// A shard that is merely full passes on its own, so it keeps the short retry.
func TestABusyShardKeepsTheShortRetry(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, scheduler.ErrNoEscrowCapacity)

	want := strconv.Itoa(int(noHostRetryAfter.Seconds()))
	if got := recorder.Header().Get("Retry-After"); got != want {
		t.Fatalf("Retry-After = %q, want %q", got, want)
	}
}

// The gateway's own limiter is a quota, and an OpenAI client reads 429 as backpressure to retry with
// backoff where 503 reads as an outage worth failing over.
func TestTheGatewaysOwnLimitAnswersTooManyRequests(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, &limits.RateLimitError{Reason: "too many concurrent requests"})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") == "" {
		t.Fatal("a limiter rejection carried no Retry-After")
	}
}

func TestAHostsUnavailableRejectionCarriesTheHostCutoffBase(t *testing.T) {
	recorder := httptest.NewRecorder()

	newErrorsTestServer().writeErrorFor(recorder, engine.ErrHostsUnavailable)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if got := recorder.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q (the default host cutoff base, rounded up to seconds)", got, "5")
	}
}

func TestAHostsUnavailableRejectionRoundsUpAConfiguredBase(t *testing.T) {
	recorder := httptest.NewRecorder()
	configuration := config.Defaults()
	configuration.Limits.HostCutoff.BaseMS = 2_500
	server := &Server{config: config.NewHolder(&configuration)}

	server.writeErrorFor(recorder, engine.ErrHostsUnavailable)

	if got := recorder.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("Retry-After = %q, want %q (2.5s rounded up)", got, "3")
	}
}
