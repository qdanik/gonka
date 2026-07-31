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
	"devshard/user"
)

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

// A devshard host answers in SSE whether or not the caller asked to stream, so a non-streaming reply
// is only JSON if the gateway assembles it. Asserting the header alone passes on the raw envelope.
func TestASuccessfulNonStreamingReplyIsJSONAndCarriesItsIdentifiers(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = "data: {\"id\":\"first\",\"choices\":[]}\n\ndata: {\"id\":\"resp\",\"choices\":[{\"message\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"

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
	var assembled map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &assembled); err != nil {
		t.Fatalf("the body labelled application/json does not parse as JSON: %v (%s)", err, recorder.Body.String())
	}
	if assembled["id"] != "resp" {
		t.Fatalf("assembled body: got %s, want the last data event of the stream", recorder.Body.String())
	}
}

// The strip is a privacy filter, and an unassembled body silently bypasses it: StripResponseBody
// returns an unparseable payload unchanged.
func TestANonStreamingReplyIsStrippedOfItsInternalFields(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = "data: {\"id\":\"resp\",\"choices\":[{\"logprobs\":{\"content\":[]},\"message\":{\"content\":\"hi\"}}],\"prompt_token_ids\":[1,2]}\n\ndata: [DONE]\n\n"

	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)

	body := recorder.Body.String()
	for _, stripped := range []string{"logprobs", "prompt_token_ids"} {
		if strings.Contains(body, stripped) {
			t.Fatalf("%s reached the client: %s", stripped, body)
		}
	}
	if !strings.Contains(body, `"content":"hi"`) {
		t.Fatalf("the strip took the reply with it: %s", body)
	}
}

func TestAStreamCarriesTheTerminatorOnEveryExit(t *testing.T) {
	testCases := []struct {
		name     string
		chunks   []string
		failure  error
		want     string
		wantOnce string
	}{
		{
			name:   "a host that never terminated",
			chunks: []string{"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"},
			want:   "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
		},
		{
			name:     "a host that terminated itself",
			chunks:   []string{"data: {\"choices\":[]}\n\n", "data: [DONE]\n\n"},
			want:     "data: {\"choices\":[]}\n\ndata: [DONE]\n\n",
			wantOnce: "data: [DONE]\n\n",
		},
		{
			name:    "a failure after the client saw bytes",
			chunks:  []string{"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"},
			failure: errors.New("winner failed after streaming started"),
			want:    "data: {\"error\":{\"message\":\"winner failed after streaming started\"}}\n\ndata: [DONE]\n\n",
		},
		{
			name: "a winner that produced nothing",
			want: "data: [DONE]\n\n",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			live := newHarness(t)
			live.inference.reply = ""
			live.inference.chunks = testCase.chunks
			live.inference.err = testCase.failure

			recorder := live.request(t, http.MethodPost, "/v1/chat/completions", streamChatBody, nil)

			body := recorder.Body.String()
			if !strings.HasSuffix(body, testCase.want) {
				t.Fatalf("streamed body: got %q, want it to end with %q", body, testCase.want)
			}
			if testCase.wantOnce != "" && strings.Count(body, testCase.wantOnce) != 1 {
				t.Fatalf("streamed body: got %q, want exactly one %q", body, testCase.wantOnce)
			}
		})
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
	want := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	if got := recorder.Body.String(); got != want {
		t.Fatalf("body: got %q, want %q", got, want)
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
	if !strings.Contains(recorder.Body.String(), "winner failed after streaming started") {
		t.Fatalf("body: got %q, want the failure named where the status no longer can", recorder.Body.String())
	}
}

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

func TestReduceMaxTokensHalvesTheCommittedBudgetAndTheBodyCarryingIt(t *testing.T) {
	reduced, ok := reduceMaxTokens(user.InferenceParams{
		Model:       "model-a",
		Prompt:      []byte(`{"max_tokens":800,"model":"model-a"}`),
		InputLength: 36,
		MaxTokens:   800,
	})
	if !ok {
		t.Fatal("reduceMaxTokens refused a budget of 800")
	}
	dispatched, isInference := reduced.(user.InferenceParams)
	if !isInference {
		t.Fatalf("reduceMaxTokens returned %T, want user.InferenceParams", reduced)
	}
	if dispatched.MaxTokens != 400 {
		t.Errorf("MaxTokens = %d, want 400", dispatched.MaxTokens)
	}
	if want := `{"max_tokens":400,"model":"model-a"}`; string(dispatched.Prompt) != want {
		t.Errorf("Prompt = %s, want %s", dispatched.Prompt, want)
	}
	if dispatched.InputLength != 36 {
		t.Errorf("InputLength = %d, want the committed record's own 36", dispatched.InputLength)
	}
}

func TestReduceMaxTokensRefusesParamsItCannotShorten(t *testing.T) {
	testCases := []struct {
		name   string
		params any
	}{
		{"params of another type", struct{ MaxTokens uint64 }{MaxTokens: 800}},
		{"a budget of one token", user.InferenceParams{Prompt: []byte(`{"max_tokens":1}`), MaxTokens: 1}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if reduced, ok := reduceMaxTokens(testCase.params); ok {
				t.Fatalf("reduceMaxTokens = (%+v, true), want a refusal", reduced)
			}
		})
	}
}

func TestAChatRequestHandsTheRaceItsHalvedBudgetHook(t *testing.T) {
	live := newHarness(t)
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"qwen","max_tokens":800,"messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"Authorization": "Bearer " + clientKey})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}

	raced := live.inference.raced.Load()
	if raced == nil || raced.ReduceMaxTokens == nil {
		t.Fatal("the race was started without the halved-budget hook, so a quiet host is never retried")
	}
	reduced, ok := raced.ReduceMaxTokens(raced.Params)
	if !ok {
		t.Fatal("the wired hook refused the params of the request it was wired for")
	}
	dispatched, isInference := reduced.(user.InferenceParams)
	if !isInference {
		t.Fatalf("the wired hook returned %T, want user.InferenceParams", reduced)
	}
	if dispatched.MaxTokens != 400 {
		t.Errorf("MaxTokens = %d, want 400", dispatched.MaxTokens)
	}
	if !strings.Contains(string(dispatched.Prompt), `"max_tokens":400`) {
		t.Errorf("body = %s, want the halved budget", dispatched.Prompt)
	}
}
