package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/scheduler"
)

// ---- P0: an unroutable model must not spend the pipeline ----

func TestAnUnroutableModelIsRejectedBeforeTheLimiterAndTheRace(t *testing.T) {
	live := newHarness(t)
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"not-served","messages":[{"role":"user","content":"hi"}]}`, nil)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d (%s), want 400", recorder.Code, recorder.Body.String())
	}
	if got := live.limiter.acquires.Load(); got != 0 {
		t.Fatalf("limiter slots taken: got %d, want 0", got)
	}
	if got := live.limiter.tokens.Load(); got != 0 {
		t.Fatalf("input-token budget consumed: got %d, want 0", got)
	}
	if got := live.inference.runs.Load(); got != 0 {
		t.Fatalf("races started: got %d, want 0", got)
	}
	if !strings.Contains(recorder.Body.String(), `unsupported model \"not-served\"`) {
		t.Fatalf("the rejection must name the model and the routable set: %s", recorder.Body.String())
	}
}

func TestAnEmptyRegistryFailsClosed(t *testing.T) {
	live := newHarness(t)
	live.escrows.models = nil
	live.escrows.escrows = nil

	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d (%s), want 503 — an empty registry is not ready, not open", recorder.Code, recorder.Body.String())
	}
	if got := live.limiter.acquires.Load(); got != 0 {
		t.Fatalf("limiter slots taken on an unready gateway: got %d, want 0", got)
	}
	if got := live.inference.runs.Load(); got != 0 {
		t.Fatalf("races started on an unready gateway: got %d, want 0", got)
	}
}

func TestAChatRequestWithoutAModelIsAClientError(t *testing.T) {
	live := newHarness(t)
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hi"}]}`, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d (%s), want 400", recorder.Code, recorder.Body.String())
	}
	if got := live.limiter.acquires.Load(); got != 0 {
		t.Fatalf("limiter slots taken: got %d, want 0", got)
	}
}

// ---- the request path's remaining statuses ----

func TestTheLimiterRejectionReachesTheClientAs429(t *testing.T) {
	live := newHarness(t)
	live.limiter.err = &limits.RateLimitError{Reason: "too many concurrent requests"}
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status: got %d, want 429", recorder.Code)
	}
	if got := live.inference.runs.Load(); got != 0 {
		t.Fatalf("races started after a limiter rejection: got %d", got)
	}
}

func TestAFailedRaceIsA502AndReturnsItsSlot(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = ""
	live.inference.err = errors.New("every attempt failed")
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d, want 502", recorder.Code)
	}
	if got := live.limiter.releases.Load(); got != 1 {
		t.Fatalf("limiter releases: got %d, want 1", got)
	}
	if got := live.limiter.tokens.Load(); got != 0 {
		t.Fatalf("input-token budget left held: got %d, want 0", got)
	}
	if got := recorder.Header().Get("X-Request-Id"); got != "request-1" {
		t.Fatalf("X-Request-Id on the error path: got %q", got)
	}
}

func TestASuccessfulNonStreamingReplyIsJSONAndCarriesItsIdentifiers(t *testing.T) {
	live := newHarness(t)
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type: got %q", got)
	}
	if got := recorder.Header().Get("X-Request-Id"); got != "request-1" {
		t.Fatalf("X-Request-Id: got %q", got)
	}
	if got := recorder.Header().Get("X-Devshard-ID"); got != "7" {
		t.Fatalf("X-Devshard-ID: got %q", got)
	}
}

func TestAStreamedReplyIsFlushedAsEventStream(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"qwen","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type: got %q", got)
	}
	if got := recorder.Header().Get("X-Request-Id"); got != "request-1" {
		t.Fatalf("X-Request-Id must be set before the first byte: got %q", got)
	}
	if !recorder.Flushed {
		t.Fatal("the stream was never flushed; a buffered SSE response reaches the client as nothing")
	}
	if !strings.Contains(recorder.Body.String(), "data: ") {
		t.Fatalf("body: %q", recorder.Body.String())
	}
}

func TestAFailureAfterTheFirstByteKeepsTheStatusTheClientAlreadySaw(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	live.inference.err = errors.New("winner failed after streaming started")
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"qwen","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d, want the 200 the client already received", recorder.Code)
	}
}

// ---- models and status ----

func TestTheModelListNamesEveryRoutableModel(t *testing.T) {
	live := newHarness(t)
	live.escrows.models = []string{"qwen", "kimi"}
	recorder := live.request(t, http.MethodGet, "/v1/models", "", nil)

	var listed modelListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decoding: %v (%s)", err, recorder.Body.String())
	}
	if listed.Object != "list" || len(listed.Data) != 2 {
		t.Fatalf("got %+v", listed)
	}
	if listed.Data[0].ID != "qwen" || listed.Data[1].ID != "kimi" {
		t.Fatalf("model ids: got %q and %q", listed.Data[0].ID, listed.Data[1].ID)
	}
	if listed.Data[0].Created != 1700000000 {
		t.Fatalf("created must come from the injected clock: got %d", listed.Data[0].Created)
	}
}

func TestThePerEscrowModelListIsScopedToThatEscrow(t *testing.T) {
	live := newHarness(t)
	live.escrows.models = []string{"qwen", "kimi"}
	live.escrows.escrows = append(live.escrows.escrows, scheduler.Escrow{ID: "8", Model: "kimi"})
	recorder := live.request(t, http.MethodGet, "/devshard/8/v1/models", "", nil)

	var listed modelListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(listed.Data) != 1 || listed.Data[0].ID != "kimi" {
		t.Fatalf("got %+v", listed.Data)
	}
}

func TestStatusReportsTheAdmissionDecisionNotTheRawChainFlag(t *testing.T) {
	live := newHarness(t)
	live.snapshots.snapshot.RequestsBlocked = true
	live.swapConfig(func(next *config.Config) { next.Modes.PoCMode = config.PoCModeRelaxed })

	recorder := live.request(t, http.MethodGet, "/v1/status", "", nil)
	var reported statusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &reported); err != nil {
		t.Fatalf("decoding: %v (%s)", err, recorder.Body.String())
	}
	if reported.RequestsBlocked {
		t.Fatal("status reported blocked while relaxed mode was serving traffic")
	}
	if len(reported.Devshards) != 1 || reported.Devshards[0].EscrowID != "7" {
		t.Fatalf("devshards: got %+v", reported.Devshards)
	}
}

func TestEstimatePromptTokensHasAFloorOfOne(t *testing.T) {
	testCases := []struct {
		name string
		body string
		want uint64
	}{
		{name: "empty", body: "", want: 1},
		{name: "one byte", body: "a", want: 1},
		{name: "four bytes", body: "abcd", want: 1},
		{name: "five bytes", body: "abcde", want: 2},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := estimatePromptTokens([]byte(testCase.body)); got != testCase.want {
				t.Fatalf("got %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestRequiresToolsReadsTheRequestOnce(t *testing.T) {
	testCases := []struct {
		name string
		body string
		want bool
	}{
		{name: "no tools", body: `{"model":"qwen"}`},
		{name: "empty tools", body: `{"tools":[]}`},
		{name: "tools present", body: `{"tools":[{"type":"function"}]}`, want: true},
		{name: "tool_choice none", body: `{"tool_choice":"none"}`},
		{name: "tool_choice auto", body: `{"tool_choice":"auto"}`, want: true},
		{name: "tool_choice object", body: `{"tool_choice":{"type":"function"}}`, want: true},
		{name: "unparsable", body: `{`},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := requiresTools([]byte(testCase.body)); got != testCase.want {
				t.Fatalf("got %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestALiveStreamCarriesTheEscrowHeader(t *testing.T) {
	live := newHarness(t)
	live.inference.chunks = []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n",
		"data: [DONE]\n\n",
	}
	upstream := httptest.NewServer(live.server.Handler())
	t.Cleanup(upstream.Close)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, upstream.URL+"/v1/chat/completions", strings.NewReader(string(streamChatBody)))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+clientKey)
	response, err := upstream.Client().Do(request)
	if err != nil {
		t.Fatalf("sending request: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", response.StatusCode)
	}
	// Read over the wire, not from a recorder: a recorder keeps a live header map, so a header set
	// after the first byte still shows up there and the assertion would pass on a broken stream.
	if got := response.Header.Get("X-Devshard-ID"); got != "7" {
		t.Fatalf("X-Devshard-ID on a live stream: got %q, want the escrow that served it", got)
	}
}
