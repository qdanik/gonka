package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/types"
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

// Capacity the gateway does not have is not the client exceeding a quota, so the refusal is 503 with a
// hint of when to return -- 429 blamed a caller that had exceeded nothing and carried no such hint.
func TestTheLimiterRejectionReachesTheClientAsUnavailable(t *testing.T) {
	live := newHarness(t)
	live.limiter.err = &limits.RateLimitError{Reason: "too many concurrent requests"}
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", recorder.Code)
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

func TestTheStatusReportsTheSessionVersion(t *testing.T) {
	live := newHarness(t)
	session, machine := newLiveSession(t)
	live.escrows.escrows = []scheduler.Escrow{{ID: liveEscrowID, Model: "qwen"}}
	live.escrows.sessions[liveEscrowID] = registry.NewSessionHandle(session, machine)

	recorder := live.request(t, http.MethodGet, "/v1/status", "", nil)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"session_version":"`+types.EffectiveStateRootAndProtocolVersion+`"`) {
		t.Fatalf("the status carries no session version: %s", recorder.Body.String())
	}
}

func TestHostClockOffsetReadsTheWinnersStamp(t *testing.T) {
	t.Parallel()
	dispatchedAt := time.Unix(1786114580, 0)
	confirmedAt := dispatchedAt.Add(120 * time.Millisecond)
	testCases := []struct {
		name         string
		outcome      engine.RaceOutcome
		wantOffsetMS int64
		wantRoundTri int64
		wantFound    bool
	}{
		{
			name: "a host whose clock agrees with ours",
			outcome: engine.RaceOutcome{WinnerNonce: 7, Attempts: []engine.AttemptOutcome{
				{Nonce: 7, SendTime: dispatchedAt, ReceiptTime: confirmedAt, ConfirmedAt: 1786114584},
			}},
			wantOffsetMS: 4440,
			wantRoundTri: 120,
			wantFound:    true,
		},
		{
			name: "a host stamping before we dispatched, which only a drifted clock can do",
			outcome: engine.RaceOutcome{WinnerNonce: 7, Attempts: []engine.AttemptOutcome{
				{Nonce: 7, SendTime: dispatchedAt, ReceiptTime: confirmedAt, ConfirmedAt: 1786114550},
			}},
			wantOffsetMS: -29560,
			wantRoundTri: 120,
			wantFound:    true,
		},
		{
			// The stamp landed inside the round trip, so half of it is ours, not the host's drift.
			name: "a slow round trip is not charged to the host as drift",
			outcome: engine.RaceOutcome{WinnerNonce: 7, Attempts: []engine.AttemptOutcome{
				{
					Nonce: 7, SendTime: dispatchedAt,
					ReceiptTime: dispatchedAt.Add(4 * time.Second), ConfirmedAt: 1786114584,
				},
			}},
			wantOffsetMS: 2500,
			wantRoundTri: 4000,
			wantFound:    true,
		},
		{
			name: "the loser's stamp is not the winner's",
			outcome: engine.RaceOutcome{WinnerNonce: 7, Attempts: []engine.AttemptOutcome{
				{Nonce: 9, SendTime: dispatchedAt, ReceiptTime: confirmedAt, ConfirmedAt: 1786114999},
				{Nonce: 7, SendTime: dispatchedAt, ReceiptTime: confirmedAt, ConfirmedAt: 1786114584},
			}},
			wantOffsetMS: 4440,
			wantRoundTri: 120,
			wantFound:    true,
		},
		{
			name: "a completion stamp is not a receipt stamp",
			outcome: engine.RaceOutcome{WinnerNonce: 7, Attempts: []engine.AttemptOutcome{
				{Nonce: 7, SendTime: dispatchedAt, ReceiptTime: confirmedAt},
			}},
		},
		{
			name: "a reply that never carried a receipt reports nothing rather than an epoch offset",
			outcome: engine.RaceOutcome{WinnerNonce: 7, Attempts: []engine.AttemptOutcome{
				{Nonce: 7, SendTime: dispatchedAt, ReceiptTime: confirmedAt},
			}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			offsetMS, roundTripMS, found := hostClockOffset(testCase.outcome)

			if found != testCase.wantFound {
				t.Fatalf("found = %v, want %v", found, testCase.wantFound)
			}
			if offsetMS != testCase.wantOffsetMS {
				t.Errorf("offsetMS = %d, want %d", offsetMS, testCase.wantOffsetMS)
			}
			if roundTripMS != testCase.wantRoundTri {
				t.Errorf("roundTripMS = %d, want %d", roundTripMS, testCase.wantRoundTri)
			}
		})
	}
}

// The executor stamps whole seconds by truncation, so a host dispatched to late in a second signs a
// stamp that reads a full second behind however well its clock agrees with ours.
func TestHostClockOffsetDoesNotReadTruncationAsDrift(t *testing.T) {
	t.Parallel()
	dispatchedAt := time.Unix(1786114580, 0).Add(900 * time.Millisecond)

	offsetMS, _, stamped := hostClockOffset(engine.RaceOutcome{
		WinnerNonce: 7,
		Attempts: []engine.AttemptOutcome{{
			Nonce: 7, SendTime: dispatchedAt,
			ReceiptTime: dispatchedAt.Add(200 * time.Millisecond),
			ConfirmedAt: 1786114580,
		}},
	})

	if !stamped {
		t.Fatal("a signed receipt must be readable")
	}
	if offsetMS < -600 || offsetMS > 600 {
		t.Errorf("offsetMS = %d, want a synchronised host inside the truncated second", offsetMS)
	}
}
