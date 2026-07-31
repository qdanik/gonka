// End-to-end suite: the composed gateway, its real engine, scheduler, registry, limits and store, and
// real devshard sessions talking to real in-process hosts — real diffs, real state roots, real
// signatures, a real MsgFinishInference — with no network and no chain.
//
// WHAT GREEN HERE DOES NOT MEAN. These are not gaps to be closed by more of this suite; they are
// outside what any in-process test can reach, and reading green as "verified" would be wrong:
//
//   - That the real chain accepts our transactions. Every chain response here is one we wrote.
//     Protobuf field numbers, unordered-tx TTL semantics, gas, fee denom, sequence handling under
//     contention and the escrow-created event shape are checked against our model of the chain, not
//     the chain. One wrong field number is a green suite and a rejected transaction. This is the
//     largest irreducible risk in the rewrite and nothing below touches it.
//   - That real hosts behave like these. The hosts are real host.Host state machines, but their
//     inference engine is a stub: a vLLM version emitting a new SSE field, a valid receipt followed
//     by a stream truncated at an unscripted byte boundary, or TCP behaviour under packet loss are
//     all unrepresented. The non-streaming reply here is SSE-shaped because the in-process client
//     writes SSE either way, so non-streaming BODY bytes are not evidence about a real host.
//   - Any tuning threshold. Peak-EWMA, the first-token hedging trigger, outlier ejection and the AIMD
//     window take a latency distribution as input. "Given these numbers, this decision" is asserted
//     in the owning packages; "these thresholds are right for the fleet" is unanswerable here.
//   - Real concurrency at real scale. The widest race below is single digits. The two known scale
//     hazards need load: ProcessResponse serialising a wide race on one Session.mu, and the
//     registry's read lock on Candidates.
//   - Settlement money end to end. The suite can assert the payload we build; whether the chain then
//     pays the right participants the right amounts is chain-side.
//   - Byte-for-byte compatibility with real clients. The frames are pinned here; whether an
//     OpenAI-SDK or aiohttp client in the field parses them as it parsed legacy's is an assumption.
//
// The honest boundary in one sentence: this suite verifies every decision the gateway makes given a
// set of inputs, and the wiring that makes those decisions reachable — not that our model of the
// chain or of a real host is correct, and not any tuning.

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"
	"devshard/transport"
	"devshard/user"
)

const operatorKey = "operator-key-at-least-16"

func TestEndToEndBootstrapGivesEveryEscrowWeightAndAServableModel(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{escrows: []string{"1", "2"}})

	for _, escrowID := range []string{"1", "2"} {
		if weight := gateway.capacityWeight(t, escrowID); weight <= 0 {
			t.Errorf("EscrowWeight(%s) = %v, want > 0; membership never reached the capacity model and the gateway serves nothing", escrowID, weight)
		}
	}
	models := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/models"})
	if models.status != http.StatusOK || !strings.Contains(string(models.body), e2eModel) {
		t.Fatalf("GET /v1/models = %d %s, want 200 listing %s", models.status, models.body, e2eModel)
	}
	status := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/status"})
	if status.status != http.StatusOK || !strings.Contains(string(status.body), `"escrow_id":"2"`) {
		t.Fatalf("GET /v1/status = %d %s, want 200 naming both published escrows", status.status, status.body)
	}
}

func TestEndToEndAServedRequestSettlesItsNonceMovesTheMetricsAndIsAccounted(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})

	response := gateway.chat(t, true)

	if response.status != http.StatusOK {
		t.Fatalf("POST /v1/chat/completions = %d %s, want 200", response.status, response.body)
	}
	fixture := gateway.only()
	if got := fixture.session.Nonce(); got != 1 {
		t.Fatalf("committed nonces = %d, want 1: the request path commits nothing", got)
	}
	// Send must apply the reply before it returns, or the engine reads an unfinished nonce on
	// AttemptDone and crowns a paid success as a failure while voting it out.
	if !fixture.session.IsNonceFinished(1) {
		t.Error("nonce 1 is unfinished after a 200: the dispatch returned without applying the host's reply")
	}
	record := gateway.awaitAccounting(t, response.header.Get("X-Request-Id"))
	if record.Outcome != store.RequestSettled || record.Model != e2eModel || record.WinnerNonce != 1 {
		t.Errorf("accounting row = %+v, want the served request's own model, nonce and settled outcome", record)
	}
	if record.EscrowID != fixture.id {
		t.Errorf("accounting row escrow = %q, want %q", record.EscrowID, fixture.id)
	}
	scrape := gateway.scrapeMetrics(t)
	for _, family := range []string{
		`devshard_gateway_requests_total{model="e2e-model",outcome="success",reason="none"} 1`,
		`devshard_http_requests_total{method="POST",path="/v1/chat/completions",status="200"} 1`,
	} {
		if !strings.Contains(scrape, family) {
			t.Errorf("scrape is missing %s", family)
		}
	}
}

func TestEndToEndAStreamedReplyFlushesBeforeItReturnsAndNamesItsEscrow(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})

	response := gateway.chat(t, true)

	if response.status != http.StatusOK {
		t.Fatalf("status = %d %s, want 200", response.status, response.body)
	}
	// The headers are the snapshot taken when the first byte went out, so a header set afterwards is
	// not counted as one the client received.
	if got := response.header.Get("X-Devshard-ID"); got != gateway.only().id {
		t.Errorf("X-Devshard-ID at first byte = %q, want %q", got, gateway.only().id)
	}
	if response.header.Get("X-Request-Id") == "" {
		t.Error("X-Request-Id is absent from the headers the client received")
	}
	if got := response.header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	firstFlush := indexOfEvent(response.events, "flush")
	lastWrite := lastIndexOfEventPrefix(response.events, "write data: [DONE]")
	switch {
	case firstFlush < 0:
		t.Fatalf("no flush reached the client: %q", response.events)
	case lastWrite < 0:
		t.Fatalf("the stream never terminated: %q", response.events)
	case firstFlush > lastWrite:
		t.Fatalf("the first flush came after the last chunk, so the stream was buffered to the end: %q", response.events)
	}
	if !strings.HasSuffix(string(response.body), "data: [DONE]\n\n") {
		t.Errorf("streamed body = %q, want a [DONE]-terminated SSE stream", response.body)
	}
}

func TestEndToEndANonStreamedReplyIsJSONAndNamesItsEscrow(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})

	response := gateway.chat(t, false)

	if response.status != http.StatusOK {
		t.Fatalf("status = %d %s, want 200", response.status, response.body)
	}
	if got := response.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if got := response.header.Get("X-Devshard-ID"); got != gateway.only().id {
		t.Errorf("X-Devshard-ID = %q, want %q", got, gateway.only().id)
	}
	if len(response.body) == 0 {
		t.Error("the client received no body")
	}
}

// A host whose post state root the escrow cannot accept is barred from that escrow for good: each of
// the three hosts serves once, and the fourth request finds nobody left rather than retrying any of them.
func TestEndToEndAStateRootDivergenceBlocksThatHostOnThatEscrowPermanently(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})
	fixture := gateway.only()
	for _, served := range fixture.hosts {
		served.divergent = true
	}

	for request := 0; request < e2eHostCount; request++ {
		gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: distinctChatBody(request)})
	}
	exhausted := gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: distinctChatBody(e2eHostCount)})

	for slot, served := range fixture.hosts {
		if got := served.dispatched(); got != 1 {
			t.Errorf("host %d served %d requests, want 1: a diverged host is still being routed to", slot, got)
		}
	}
	if exhausted.status != http.StatusBadGateway {
		t.Fatalf("the request after every host diverged = %d %s, want 502", exhausted.status, exhausted.body)
	}
	if got := fixture.dispatches(); got != int64(e2eHostCount) {
		t.Errorf("total dispatches = %d, want %d: the blocks are not permanent", got, e2eHostCount)
	}
}

// An escrow retired while a race holds its nonce must not take the reply down with it.
func TestEndToEndAnEscrowRetiredMidRaceStillFinishesTheNonceItCommitted(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})
	fixture := gateway.only()
	fixture.parkHosts(t)

	served := make(chan clientResponse, 1)
	go func() { served <- gateway.chat(t, true) }()
	fixture.awaitDispatch(t)

	deactivated := gateway.send(t, e2eRequest{path: "/v1/admin/devshards/" + fixture.id + "/deactivate", bearer: operatorKey})
	if deactivated.status != http.StatusOK {
		t.Fatalf("deactivate = %d %s, want 200", deactivated.status, deactivated.body)
	}
	fixture.releaseHosts()
	response := <-served

	if response.status != http.StatusOK {
		t.Fatalf("the request whose escrow retired beneath it = %d %s, want 200", response.status, response.body)
	}
	if !fixture.session.IsNonceFinished(1) {
		t.Error("the nonce committed before the retire was never finished")
	}
	if _, routable := gateway.gateway.escrows.RoutableSession(fixture.id); routable {
		t.Fatal("the escrow is still routable, so this test never reached the state it is about")
	}
}

// The money path this suite exists to guard: a nonce whose escrow retired beneath it still gets the
// timeout vote it owes. Without one, the MsgStart it committed is orphaned and settlement can never
// resolve it, so the vote is asserted where the chain would see it and the escrow is held in flight for
// the whole of it -- a settlement that claimed funds mid-race would be claiming against a live nonce.
func TestEndToEndAnEscrowRetiredMidRaceStillPostsTheVoteItsNonceOwes(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})
	fixture := gateway.only()
	fixture.parkHosts(t)
	fixture.failHosts(errors.New("host is gone"))

	served := make(chan clientResponse, 1)
	go func() { served <- gateway.chat(t, true) }()
	fixture.awaitDispatch(t)
	if !gateway.gateway.escrows.IsBusy(fixture.id) {
		t.Fatal("IsBusy while the race holds a committed nonce = false: a settlement could claim funds beneath it")
	}

	deactivated := gateway.send(t, e2eRequest{path: "/v1/admin/devshards/" + fixture.id + "/deactivate", bearer: operatorKey})
	if deactivated.status != http.StatusOK {
		t.Fatalf("deactivate = %d %s, want 200", deactivated.status, deactivated.body)
	}
	if _, routable := gateway.gateway.escrows.RoutableSession(fixture.id); routable {
		t.Fatal("the retired escrow is still routable, so this test never reached the state it is about")
	}
	if !gateway.gateway.escrows.IsBusy(fixture.id) {
		t.Fatal("IsBusy after the retire = false: the escrow drained before the vote its nonce owes")
	}
	fixture.releaseHosts()
	<-served
	gateway.gateway.races.Stop()

	if fixture.session.IsNonceFinished(1) {
		t.Fatal("nonce 1 is finished, so nothing owed a vote and this test asserts nothing")
	}
	posted := timeoutActions(gateway.scrapeMetrics(t), "completed")
	if len(posted) != 1 {
		t.Fatalf("timeout votes posted = %v, want one for the nonce the retired escrow committed", posted)
	}
	if gateway.gateway.escrows.IsBusy(fixture.id) {
		t.Error("IsBusy once the vote is posted = true: the escrow is never released for settlement")
	}
}

// Re-publishing an escrow the previous entry is still draining would hand that entry's committed
// nonces to a session that never saw them and open a second handle on the storage it still holds, so
// the operator is refused until the drain ends rather than served a silently split escrow.
func TestEndToEndActivatingAnEscrowThatIsStillDrainingIsRefused(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})
	fixture := gateway.only()
	fixture.parkHosts(t)

	served := make(chan clientResponse, 1)
	go func() { served <- gateway.chat(t, true) }()
	fixture.awaitDispatch(t)

	deactivated := gateway.send(t, e2eRequest{path: "/v1/admin/devshards/" + fixture.id + "/deactivate", bearer: operatorKey})
	if deactivated.status != http.StatusOK {
		t.Fatalf("deactivate = %d %s, want 200", deactivated.status, deactivated.body)
	}
	reactivated := gateway.send(t, e2eRequest{path: "/v1/admin/devshards/" + fixture.id + "/activate", bearer: operatorKey})

	if reactivated.status != http.StatusConflict {
		t.Fatalf("activate while the escrow drains = %d %s, want 409", reactivated.status, reactivated.body)
	}
	fixture.releaseHosts()
	if response := <-served; response.status != http.StatusOK {
		t.Fatalf("the draining request = %d %s, want 200", response.status, response.body)
	}
}

// timeoutActions returns the exposition lines counting one timeout action, so an assertion names the
// action the chain would see rather than a label ordering.
func timeoutActions(scrape, action string) []string {
	matched := []string{}
	for line := range strings.SplitSeq(scrape, "\n") {
		if strings.HasPrefix(line, "devshard_gateway_timeout_actions_total{") &&
			strings.Contains(line, `action="`+action+`"`) {
			matched = append(matched, line)
		}
	}
	return matched
}

// One outcome and one winner per race, counted across the whole stack rather than inside the engine.
func TestEndToEndConcurrentRacesEachProduceExactlyOneOutcomeAndOneWinner(t *testing.T) {
	const callers = 6
	gateway := bootGateway(t, e2eOptions{})
	fixture := gateway.only()

	responses := make([]clientResponse, callers)
	var calling sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		calling.Add(1)
		go func() {
			defer calling.Done()
			responses[caller] = gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: distinctChatBody(caller)})
		}()
	}
	calling.Wait()

	winners := map[uint64]string{}
	for caller, response := range responses {
		if response.status != http.StatusOK {
			t.Fatalf("caller %d = %d %s, want 200", caller, response.status, response.body)
		}
		record := gateway.awaitAccounting(t, response.header.Get("X-Request-Id"))
		if previous, taken := winners[record.WinnerNonce]; taken {
			t.Fatalf("nonce %d was crowned for both %s and %s", record.WinnerNonce, previous, record.RequestID)
		}
		winners[record.WinnerNonce] = record.RequestID
	}
	if got := fixture.session.Nonce(); got != callers {
		t.Errorf("committed nonces = %d, want %d", got, callers)
	}
	expected := fmt.Sprintf(`devshard_gateway_requests_total{model=%q,outcome="success",reason="none"} %d`, e2eModel, callers)
	if scrape := gateway.scrapeMetrics(t); !strings.Contains(scrape, expected) {
		t.Errorf("scrape does not report exactly %d outcomes; want %s", callers, expected)
	}
}

// The gateway limiter's slot and input-token budget are the DoS surface, so a rejected request must
// never have taken either.
func TestEndToEndARejectedRequestTakesNoLimiterSlot(t *testing.T) {
	testCases := []struct {
		name    string
		options e2eOptions
		body    string
		bearer  string
		want    int
	}{
		{
			name: "a model no escrow serves",
			body: chatBody("nobody-serves-this", false),
			want: http.StatusBadRequest,
		},
		{
			name: "a model the caller may not use",
			options: e2eOptions{tune: func(settings *config.Config) {
				settings.Limits.ModelAccess = map[string]string{e2eModel: config.ModelAccessAdminOnly}
			}},
			body: chatBody(e2eModel, false),
			want: http.StatusUnauthorized,
		},
		{
			name:    "a gateway the operator turned off",
			options: e2eOptions{tune: func(settings *config.Config) { settings.Modes.Disabled = true }},
			body:    chatBody(e2eModel, false),
			want:    http.StatusServiceUnavailable,
		},
		{
			name:    "a chain phase that blocks new requests",
			options: e2eOptions{epochPhase: "PoCGenerate"},
			body:    chatBody(e2eModel, false),
			want:    http.StatusServiceUnavailable,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			gateway := bootGateway(t, testCase.options)

			response := gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: testCase.body, bearer: testCase.bearer})

			if response.status != testCase.want {
				t.Fatalf("status = %d %s, want %d", response.status, response.body, testCase.want)
			}
			if inFlight := gateway.gateway.limiter.Snapshot().Total; inFlight.Requests != 0 || inFlight.InputTokens != 0 {
				t.Errorf("limiter after the rejection = %+v, want nothing held", inFlight)
			}
			if got := gateway.only().dispatches(); got != 0 {
				t.Errorf("dispatches = %d, want 0: a rejected request reached a host", got)
			}
		})
	}
}

func TestEndToEndRelaxedModeAdmitsWhatTheChainPhaseBlocks(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{epochPhase: "PoCGenerate", pocMode: "relaxed"})

	response := gateway.chat(t, true)

	if response.status != http.StatusOK {
		t.Fatalf("status under relaxed mode = %d %s, want 200", response.status, response.body)
	}
	if !gateway.gateway.observer.Snapshot().RequestsBlocked {
		t.Fatal("the chain never reported a blocking phase, so this test never reached the state it is about")
	}
}

func TestEndToEndTheAdminKeyGatesTheOperatorRoutes(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})

	testCases := []struct {
		name   string
		bearer string
		want   int
	}{
		{name: "no key", bearer: "", want: http.StatusUnauthorized},
		{name: "a wrong key of the same length", bearer: "operator-key-at-least-17", want: http.StatusUnauthorized},
		{name: "a prefix of the key", bearer: "operator-key", want: http.StatusUnauthorized},
		{name: "the configured key", bearer: operatorKey, want: http.StatusOK},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/admin/state", bearer: testCase.bearer})
			if response.status != testCase.want {
				t.Fatalf("GET /v1/admin/state = %d %s, want %d", response.status, response.body, testCase.want)
			}
		})
	}
	public := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/models"})
	if public.status != http.StatusOK {
		t.Fatalf("GET /v1/models = %d, want 200: the admin gate reaches a public route", public.status)
	}
}

func TestEndToEndAnOversizedOperatorBodyIsRefusedAtIngest(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})

	response := gateway.send(t, e2eRequest{
		method: http.MethodPut, path: "/v1/admin/settings", bearer: operatorKey,
		body: `{"disabled_message":"` + strings.Repeat("x", 128<<10) + `"}`,
	})

	if response.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("an oversized settings patch = %d %s, want 413", response.status, response.body)
	}
}

func TestEndToEndACacheHitNeverReachesAHostAndAnotherCallerMisses(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{apiKeys: "caller-one-key-at-least-16,caller-two-key-at-least-16"})
	fixture := gateway.only()

	first := gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: chatBody(e2eModel, false), bearer: "caller-one-key-at-least-16"})
	repeated := gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: chatBody(e2eModel, false), bearer: "caller-one-key-at-least-16"})
	other := gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: chatBody(e2eModel, false), bearer: "caller-two-key-at-least-16"})

	if string(repeated.body) != string(first.body) || repeated.status != first.status {
		t.Fatalf("the repeated request = %d %s, want the first reply replayed", repeated.status, repeated.body)
	}
	if got := fixture.dispatches(); got != 2 {
		t.Fatalf("dispatches = %d, want 2: one for the first caller and one for the second, none for the replay", got)
	}
	if other.status != http.StatusOK {
		t.Fatalf("the second caller = %d %s, want 200", other.status, other.body)
	}
	if got := fixture.session.Nonce(); got != 2 {
		t.Errorf("committed nonces = %d, want 2: the replay spent one", got)
	}
}

func TestEndToEndTheScrapeCarriesTheRouteLabelAndNeverCountsItself(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})
	gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/models"})
	gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/no-such-route"})

	gateway.scrapeMetrics(t)
	scrape := gateway.scrapeMetrics(t)

	if !strings.Contains(scrape, `devshard_http_requests_total{method="GET",path="/v1/models",status="200"} 1`) {
		t.Error("the scrape does not carry the /v1/models route label")
	}
	if !strings.Contains(scrape, `path="other"`) {
		t.Error("an unmatched path did not fold into the other label")
	}
	if strings.Contains(scrape, `path="/metrics"`) {
		t.Error("the scrape counts itself, so every request-rate panel is inflated by monitoring traffic")
	}
	if strings.Contains(scrape, `path="/v1/no-such-route"`) {
		t.Error("an unmatched path became its own series: unauthenticated traffic can mint labels")
	}
}

func TestEndToEndTheKillSwitchLeavesMetricsAndOperatorRoutesReachable(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})

	patched := gateway.send(t, e2eRequest{
		method: http.MethodPut, path: "/v1/admin/settings", bearer: operatorKey,
		body: `{"disabled":true,"disabled_message":"under maintenance"}`,
	})
	if patched.status != http.StatusOK {
		t.Fatalf("PUT /v1/admin/settings = %d %s, want 200", patched.status, patched.body)
	}

	chat := gateway.chat(t, false)
	if chat.status != http.StatusServiceUnavailable || !strings.Contains(string(chat.body), "under maintenance") {
		t.Errorf("chat while disabled = %d %s, want 503 carrying the operator's message", chat.status, chat.body)
	}
	if scrape := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/metrics"}); scrape.status != http.StatusOK {
		t.Errorf("/metrics while disabled = %d, want 200: monitoring dies exactly when the gateway is turned off", scrape.status)
	}
	if state := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/admin/state", bearer: operatorKey}); state.status != http.StatusOK {
		t.Errorf("GET /v1/admin/state while disabled = %d, want 200: remediation is what a turned-off gateway still needs", state.status)
	}
}

// The patch is persisted and swapped in one step, so the request after it is judged on the new value.
func TestEndToEndAnAdminSettingsPatchIsAppliedToTheNextRequest(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})

	if before := gateway.chat(t, false); before.status != http.StatusOK {
		t.Fatalf("chat before the patch = %d %s, want 200", before.status, before.body)
	}
	patched := gateway.send(t, e2eRequest{
		method: http.MethodPut, path: "/v1/admin/settings", bearer: operatorKey,
		body: fmt.Sprintf(`{"model_access":{%q:"admin_only"}}`, e2eModel),
	})
	if patched.status != http.StatusOK {
		t.Fatalf("PUT /v1/admin/settings = %d %s, want 200", patched.status, patched.body)
	}

	after := gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: distinctChatBody(1)})

	if after.status != http.StatusUnauthorized {
		t.Fatalf("chat after the patch = %d %s, want 401", after.status, after.body)
	}
	stored := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/v1/admin/settings", bearer: operatorKey})
	if !strings.Contains(string(stored.body), `"admin_only"`) {
		t.Errorf("GET /v1/admin/settings = %s, want the patch persisted", stored.body)
	}
}

// Shutdown lets the request already running finish, and the escrow's nonces survive the process that
// committed them: a reopened session rebuilds them from its own storage.
func TestEndToEndShutdownDrainsAnInFlightRequestAndTheNoncesSurviveIt(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})
	fixture := gateway.only()
	fixture.parkHosts(t)

	served := make(chan clientResponse, 1)
	go func() { served <- gateway.chat(t, true) }()
	fixture.awaitDispatch(t)

	stopped := make(chan error, 1)
	go func() { stopped <- gateway.shutdownNow() }()
	fixture.releaseHosts()

	if response := <-served; response.status != http.StatusOK {
		t.Fatalf("the in-flight request = %d %s, want 200: shutdown dropped it", response.status, response.body)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("shutdown = %v, want nil", err)
	}

	reopened, machine, err := user.NewLocalSession(user.LocalSessionConfig{
		PrivateKeyHex: fixture.privateKeyHex,
		EscrowID:      fixture.id,
		StoragePath:   fixture.storagePath,
	})
	if err != nil {
		t.Fatalf("NewLocalSession after shutdown = %v, want nil", err)
	}
	defer reopened.Close()
	if got := reopened.Nonce(); got != 1 {
		t.Fatalf("reopened nonce = %d, want 1: the committed nonce did not survive the store closing", got)
	}
	if _, committed := machine.SnapshotState().Inferences[1]; !committed {
		t.Error("the reopened escrow has no record of the inference its nonce committed")
	}
}

// The drain protects the vote a committed nonce owes, and that vote waits on a protocol deadline no
// grace period can shorten. Waiting for it costs the store, the queued ledger rows and the escrow
// snapshots to the SIGKILL that follows, so the drain gives up its budget and reports what it left
// running instead of taking the process down with it.
func TestEndToEndShutdownGivesUpOnAVoteThatOutlastsTheGracePeriod(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{refusalTimeoutSeconds: 1})
	fixture := gateway.only()
	fixture.failHosts(errors.New("host is gone"))

	if response := gateway.chat(t, true); response.status != http.StatusBadGateway {
		t.Fatalf("the failed race = %d %s, want 502", response.status, response.body)
	}

	started := time.Now()
	err := gateway.shutdownWithin(200 * time.Millisecond)
	elapsed := time.Since(started)
	t.Cleanup(gateway.gateway.races.Stop)

	if elapsed > 2*time.Second {
		t.Fatalf("shutdown with a 200ms grace period took %v: the drain is unbounded, so SIGTERM ends in a SIGKILL mid-vote", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "stopping races") {
		t.Fatalf("shutdown() = %v, want the abandoned race drain named", err)
	}
}

func TestEndToEndAFailedRaceIsAccountedAndReleasesItsSlot(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})
	fixture := gateway.only()
	for _, served := range fixture.hosts {
		served.failure = errors.New("host went away")
	}

	response := gateway.chat(t, true)

	if response.status != http.StatusBadGateway {
		t.Fatalf("status = %d %s, want 502", response.status, response.body)
	}
	record := gateway.awaitAccounting(t, response.header.Get("X-Request-Id"))
	if record.Outcome != store.RequestFailed || record.Model != e2eModel {
		t.Errorf("accounting row = %+v, want a failed row carrying the request's own model", record)
	}
	if inFlight := gateway.gateway.limiter.Snapshot().Total; inFlight.Requests != 0 {
		t.Errorf("limiter after the failure = %+v, want nothing held", inFlight)
	}
}

func indexOfEvent(events []string, want string) int {
	for index, event := range events {
		if event == want {
			return index
		}
	}
	return -1
}

func lastIndexOfEventPrefix(events []string, prefix string) int {
	found := -1
	for index, event := range events {
		if strings.HasPrefix(event, prefix) {
			found = index
		}
	}
	return found
}

// A burned nonce is spent money. Production had no accounting for it at all: the scheduler's observer
// was implemented only in a test file, so this asserts the counter off the gateway's own exposition.
func TestEndToEndABurnedNonceIsCountedUnderItsOwnReason(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{tune: func(settings *config.Config) {
		settings.Scheduler.HoldGraceMS = 0
	}})
	fixture := gateway.only()
	// nonce%3 binds host 1 on nonces 1 and 4: the first blocks it, the second has to burn past it.
	fixture.hosts[1].divergent = true

	for request := 0; request < 4; request++ {
		gateway.send(t, e2eRequest{path: "/v1/chat/completions", body: distinctChatBody(request)})
	}

	burned := fmt.Sprintf("devshard_gateway_ghost_nonces_burned_total{devshard_id=%q,reason=%q} 1", fixture.id, "participant_capability_no_send")
	if scrape := gateway.scrapeMetrics(t); !strings.Contains(scrape, burned) {
		t.Fatalf("scrape is missing %s: a nonce was burned and nothing in production counted it", burned)
	}
}

// A host reporting "escrow not found" must end with that escrow no longer accepting traffic. The
// confirming chain lookup is the escrow lifecycle's, never the request's.
func TestEndToEndAnEscrowAHostCannotFindIsCheckedAndDeactivated(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{})
	fixture := gateway.only()
	fixture.failHosts(&transport.UpstreamStatusError{
		Path:       "/v1/chat/completions",
		StatusCode: http.StatusInternalServerError,
		Body:       `{"error":"escrow not found"}`,
	})

	response := gateway.chat(t, true)

	record := gateway.awaitAccounting(t, response.header.Get("X-Request-Id"))
	if !record.EscrowMissing {
		t.Fatalf("accounting row = %+v, want escrow_missing: the host's report never reached the outcome", record)
	}
	gateway.gateway.manager.Start(context.Background())
	awaitDevshardInactive(t, gateway, fixture.id)
}

// An escrow that cannot fund another nonce must say so where an operator reads it. The escrow refuses
// to commit the nonce before any host is contacted, so this is the only path that can report it.
func TestEndToEndAnEscrowThatCannotFundANonceIsAccountedAsBalanceExhausted(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{balance: 1})
	fixture := gateway.only()

	response := gateway.chat(t, true)

	if response.status == http.StatusOK {
		t.Fatalf("status = %d, want a failure: the escrow cannot fund this request", response.status)
	}
	if got := fixture.dispatches(); got != 0 {
		t.Fatalf("dispatches = %d, want 0: an exhausted escrow must not reach a host", got)
	}
	record := gateway.awaitAccounting(t, response.header.Get("X-Request-Id"))
	if !record.BalanceExhausted {
		t.Fatalf("accounting row = %+v, want balance_exhausted: the column cannot report an escrow running dry", record)
	}
	scrape := gateway.scrapeMetrics(t)
	if !strings.Contains(scrape, `devshard_gateway_requests_total{model="e2e-model",outcome="failure",reason="balance_exhausted"} 1`) {
		t.Error("scrape has no balance_exhausted failure reason: the metric arm is still unreachable")
	}
}

func awaitDevshardInactive(t *testing.T, gateway *e2eGateway, escrowID string) {
	t.Helper()
	deadline := time.Now().Add(e2eSettleWait)
	for {
		records, err := gateway.gateway.store.ListDevshards(context.Background())
		if err != nil {
			t.Fatalf("ListDevshards = %v, want nil", err)
		}
		for _, record := range records {
			if record.EscrowID == escrowID && !record.Active {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("escrow %s is still active: a host reported it gone from chain and it keeps taking traffic", escrowID)
		}
		time.Sleep(time.Millisecond)
	}
}

// A pinned request is served by the escrow it names, however routing would have scored it: two
// requests that would otherwise rotate across the tie both land on the escrow they name. A pin the
// gateway no longer routes to is refused rather than quietly served by whichever escrow is left.
func TestEndToEndAPinnedRequestReachesItsEscrowAndARefusedPinDoesNotFallBack(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{escrows: []string{"1", "2"}, adminKey: operatorKey})
	pinned, unnamed := gateway.fixtures["1"], gateway.fixtures["2"]

	for caller := range 2 {
		served := gateway.send(t, e2eRequest{path: "/devshard/1/v1/chat/completions", body: distinctChatBody(caller)})
		if served.status != http.StatusOK {
			t.Fatalf("pinned request %d = %d %s, want 200", caller, served.status, served.body)
		}
		if got := served.header.Get("X-Devshard-ID"); got != "1" {
			t.Fatalf("X-Devshard-ID = %q, want the escrow the request pinned", got)
		}
	}
	if got := unnamed.dispatches(); got != 0 {
		t.Fatalf("dispatches on the escrow no request named = %d, want 0: routing overrode the pin", got)
	}

	deactivated := gateway.send(t, e2eRequest{path: "/v1/admin/devshards/2/deactivate", bearer: operatorKey})
	if deactivated.status != http.StatusOK {
		t.Fatalf("deactivate = %d %s, want 200", deactivated.status, deactivated.body)
	}
	refused := gateway.send(t, e2eRequest{path: "/devshard/2/v1/chat/completions", body: distinctChatBody(2)})

	if refused.status == http.StatusOK {
		t.Fatalf("a pin to an escrow that no longer serves the model = 200 %s, want a refusal", refused.body)
	}
	if got := pinned.dispatches(); got != 2 {
		t.Fatalf("dispatches on the live escrow = %d, want 2: the refused pin fell back onto it", got)
	}
}

// The cache is keyed on the escrow as well as on the caller and the body. Without that dimension one
// escrow's reply is replayed for another under an X-Devshard-ID that never served it.
func TestEndToEndACacheEntryOnOneEscrowIsAMissOnAnother(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{escrows: []string{"1", "2"}})
	first, second := gateway.fixtures["1"], gateway.fixtures["2"]
	body := chatBody(e2eModel, false)

	served := gateway.send(t, e2eRequest{path: "/devshard/1/v1/chat/completions", body: body})
	replayed := gateway.send(t, e2eRequest{path: "/devshard/1/v1/chat/completions", body: body})
	elsewhere := gateway.send(t, e2eRequest{path: "/devshard/2/v1/chat/completions", body: body})

	if string(replayed.body) != string(served.body) || replayed.status != served.status {
		t.Fatalf("the repeated pinned request = %d %s, want the first reply replayed", replayed.status, replayed.body)
	}
	if got := first.dispatches(); got != 1 {
		t.Fatalf("dispatches on the pinned escrow = %d, want 1: the replay reached a host", got)
	}
	if elsewhere.status != http.StatusOK {
		t.Fatalf("the same body on another escrow = %d %s, want 200", elsewhere.status, elsewhere.body)
	}
	if got := elsewhere.header.Get("X-Devshard-ID"); got != "2" {
		t.Fatalf("X-Devshard-ID = %q, want 2: the reply came from the entry another escrow cached", got)
	}
	if got := second.dispatches(); got != 1 {
		t.Fatalf("dispatches on the other escrow = %d, want 1: the cache answered for an escrow it never ran on", got)
	}
}

// The stuck-escrow recovery surface. It reads through settlement rather than through routing, so it
// still answers for an escrow already retired from routing — the state an operator reaches for it in.
func TestEndToEndTheRecoveryRoutesReadALiveEscrowAndADrainingOne(t *testing.T) {
	gateway := bootGateway(t, e2eOptions{adminKey: operatorKey})
	fixture := gateway.only()
	if served := gateway.chat(t, false); served.status != http.StatusOK {
		t.Fatalf("chat = %d %s, want 200", served.status, served.body)
	}

	unauthenticated := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/devshard/" + fixture.id + "/v1/state"})
	if unauthenticated.status != http.StatusUnauthorized {
		t.Fatalf("state without the operator key = %d, want 401", unauthenticated.status)
	}
	unknown := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/devshard/404/v1/state", bearer: operatorKey})
	if unknown.status != http.StatusNotFound {
		t.Fatalf("state for an escrow the gateway does not hold = %d, want 404", unknown.status)
	}
	for _, path := range []string{"/v1/state", "/v1/debug/state", "/v1/debug/inferences", "/v1/debug/pending", "/v1/debug/signatures"} {
		read := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/devshard/" + fixture.id + path, bearer: operatorKey})
		if read.status != http.StatusOK {
			t.Fatalf("%s = %d %s, want 200", path, read.status, read.body)
		}
		if !strings.Contains(string(read.body), `"escrow_id":"`+fixture.id+`"`) {
			t.Errorf("%s = %s, want the escrow it names", path, read.body)
		}
	}

	state := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/devshard/" + fixture.id + "/v1/state", bearer: operatorKey})
	if !strings.Contains(string(state.body), `"nonce":1`) || !strings.Contains(string(state.body), `"inferences":1`) {
		t.Errorf("state = %s, want the one nonce the served request committed", state.body)
	}
	inferences := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/devshard/" + fixture.id + "/v1/debug/inferences", bearer: operatorKey})
	if !strings.Contains(string(inferences.body), `"nonce":1`) {
		t.Errorf("inferences = %s, want the record nonce 1 committed", inferences.body)
	}

	fixture.parkHosts(t)
	draining := make(chan clientResponse, 1)
	go func() { draining <- gateway.chat(t, true) }()
	fixture.awaitDispatch(t)
	deactivated := gateway.send(t, e2eRequest{path: "/v1/admin/devshards/" + fixture.id + "/deactivate", bearer: operatorKey})
	if deactivated.status != http.StatusOK {
		t.Fatalf("deactivate = %d %s, want 200", deactivated.status, deactivated.body)
	}
	if _, routable := gateway.gateway.escrows.RoutableSession(fixture.id); routable {
		t.Fatal("the escrow is still routable, so this test never reached the state it is about")
	}
	pending := gateway.send(t, e2eRequest{method: http.MethodGet, path: "/devshard/" + fixture.id + "/v1/debug/pending", bearer: operatorKey})
	if pending.status != http.StatusOK {
		t.Fatalf("pending on a draining escrow = %d %s, want 200", pending.status, pending.body)
	}
	if !strings.Contains(string(pending.body), `"nonce":2`) {
		t.Errorf("pending = %s, want the nonce the draining request is still holding", pending.body)
	}
	fixture.releaseHosts()
	<-draining
}
