package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"

	"devshard/bridge"
	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
	"devshard/types"
	"devshard/user"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// database/sql keeps its connection cleaner alive for the process, and the sqlite driver's
		// own finalizer goroutine outlives every handle we close.
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreTopFunction("modernc.org/sqlite.(*conn).interruptOnDone.func1"),
		// The chain client is dialed once and lives for the process: common/chain exposes no Close, and
		// its grpc connection and desertbit/timer's wheel both outlive every gateway this test composes.
		// The connection now carries every chain read, so its resolver and transport goroutines start
		// where before only the bridge held an idle handle.
		goleak.IgnoreTopFunction("github.com/desertbit/timer.timerRoutine"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/resolver/dns.(*dnsResolver).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransportAndUnlock"),
	)
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// fakeChain answers the observer's polls with an empty epoch, so a composed gateway boots without
// reaching a real node and without waiting on one.
func fakeChain(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func gatewayEnvironment(t *testing.T) {
	t.Helper()
	chainURL := fakeChain(t)
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())
	t.Setenv("GATEWAY_PUBLIC_API", chainURL)
}

// composedGateway builds exactly what run() builds, so an assertion below reaches the wiring the
// process uses rather than a re-declaration of it.
func composedGateway(t *testing.T) *gateway {
	t.Helper()
	values, err := env.Load()
	if err != nil {
		t.Fatalf("loading environment: %v", err)
	}
	storageDir, err := resolveStorageDir(values.StorageDir)
	if err != nil {
		t.Fatalf("resolving storage dir: %v", err)
	}
	gatewayStore, err := store.Open(storageDir)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	composed, err := compose(context.Background(), values, storageDir, gatewayStore, chainBackedSessions(gatewayStore, storageDir))
	if err != nil {
		gatewayStore.Close()
		t.Fatalf("compose(): %v", err)
	}
	t.Cleanup(func() {
		if err := composed.shutdown(shutdownGracePeriod); err != nil {
			t.Errorf("shutdown(): %v", err)
		}
	})
	return composed
}

func TestRunServesMetricsAndShutsDownGracefully(t *testing.T) {
	port := freePort(t)
	gatewayEnvironment(t)
	t.Setenv("GATEWAY_PORT", fmt.Sprintf("%d", port))

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- run(ctx) }()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(baseURL + "/metrics")
		if err == nil {
			raw, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				body = string(raw)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not serve /metrics within 5s (last error: %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// go_goroutines comes from the process collector, so it is present on the very first scrape,
	// unlike the request counter which only appears once a route has been served.
	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("/metrics exposition is missing the process collector:\n%s", body)
	}

	cancel()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("run() after cancel: %v, want nil (graceful shutdown)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return within 10s of cancellation")
	}
}

func TestRunServesTheChatCompletionsRouteFromTheComposedServer(t *testing.T) {
	port := freePort(t)
	gatewayEnvironment(t)
	t.Setenv("GATEWAY_PORT", fmt.Sprintf("%d", port))

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- run(ctx) }()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	var status int
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Post(baseURL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"nobody-serves-this","messages":[{"role":"user","content":"hi"}]}`))
		if err == nil {
			response.Body.Close()
			status = response.StatusCode
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not serve /v1/chat/completions within 5s (last error: %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 404 would mean main serves the Phase-1 metrics mux instead of the api server's routes.
	if status != http.StatusServiceUnavailable {
		t.Fatalf("POST /v1/chat/completions with an empty registry = %d, want %d", status, http.StatusServiceUnavailable)
	}

	cancel()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("run() after cancel: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return within 10s of cancellation")
	}
}

func TestRunFailsFastOnInvalidEnvironment(t *testing.T) {
	gatewayEnvironment(t)
	t.Setenv("GATEWAY_PORT", "not-a-port")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "GATEWAY_PORT") {
		t.Fatalf("run() with bad env = %v, want error naming GATEWAY_PORT", err)
	}
}

func TestRunFailsFastOnInvalidMergedConfig(t *testing.T) {
	gatewayEnvironment(t)
	t.Setenv("GATEWAY_MAX_TOKENS_CAP", "0")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "max_tokens_cap") {
		t.Fatalf("run() with invalid merged config = %v, want max_tokens_cap error", err)
	}
}

func TestARacePutsAnAccountingRowInTheStoreTheGatewayOpened(t *testing.T) {
	gatewayEnvironment(t)
	composed := composedGateway(t)

	if _, err := composed.races.Run(context.Background(), engine.Request{
		RequestID:   "accounted-request",
		Model:       "nobody-serves-this",
		InputTokens: 11,
	}, io.Discard); err == nil {
		t.Fatal("Run() with no escrows = nil, want a failure")
	}

	record := waitForAccountingRow(t, composed.store, "accounted-request")
	if record.Model != "nobody-serves-this" || record.InputTokens != 11 {
		t.Fatalf("accounting row = %+v, want the raced request's own model and input tokens", record)
	}
}

func waitForAccountingRow(t *testing.T, records *store.Store, requestID string) store.RequestRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		record, found, err := records.FindRequest(context.Background(), requestID)
		if err != nil {
			t.Fatalf("FindRequest(%q): %v", requestID, err)
		}
		if found {
			return record
		}
		if time.Now().After(deadline) {
			t.Fatalf("no accounting row for %q within 5s: the engine's ledger is not wired to this store", requestID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestARaceMovesTheMetricsTheGatewayExposes(t *testing.T) {
	gatewayEnvironment(t)
	composed := composedGateway(t)
	exposition := httptest.NewServer(composed.server.Handler)
	defer exposition.Close()

	before := scrape(t, exposition.URL)
	if _, err := composed.races.Run(context.Background(), engine.Request{
		RequestID:   "measured-request",
		Model:       "nobody-serves-this",
		InputTokens: 7,
	}, io.Discard); err == nil {
		t.Fatal("Run() with no escrows = nil, want a failure")
	}
	after := scrape(t, exposition.URL)

	const raceFamily = "devshard_gateway_requests_total"
	if strings.Contains(before, raceFamily) {
		t.Fatalf("%s was already exposed before any race ran", raceFamily)
	}
	if !strings.Contains(after, raceFamily) {
		t.Fatalf("%s is absent after a race ran: the engine's race recorder is not wired\n%s", raceFamily, after)
	}
}

func scrape(t *testing.T, baseURL string) string {
	t.Helper()
	response, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("scraping /metrics: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading /metrics: %v", err)
	}
	return string(body)
}

func TestEveryOwnerCollectorIsRegisteredOnTheGatewaysRegistry(t *testing.T) {
	gatewayEnvironment(t)
	composed := composedGateway(t)

	// A collector describing families the registry already knows is refused, so a refusal is proof
	// compose registered that owner's collector and nothing else is.
	testCases := []struct {
		name      string
		collector prometheus.Collector
	}{
		{name: "limits", collector: metrics.NewLimitsCollector(metrics.LimitsSources{})},
		{name: "perf", collector: metrics.NewPerfCollector(nil)},
		{name: "registry", collector: metrics.NewRegistryCollector(metrics.RegistrySources{})},
		{name: "chain", collector: metrics.NewChainCollector(nil, time.Now)},
		{name: "transport", collector: metrics.NewTransportCollector(nil)},
		{name: "accounting", collector: metrics.NewAccountingCollector(nil)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := composed.telemetry.Registry().Register(testCase.collector); err == nil {
				t.Fatalf("the %s collector was not registered by compose()", testCase.name)
			}
		})
	}
}

func TestBootBudgetSizesTheIdlePoolToTheBuilderLimit(t *testing.T) {
	for _, builders := range []int{1, 4, 16, 64} {
		t.Run(fmt.Sprintf("%d builders", builders), func(t *testing.T) {
			budget := newBootBudget(builders)
			pooled, isTransport := budget.client.Transport.(*http.Transport)
			if !isTransport {
				t.Fatalf("boot client transport is %T, want *http.Transport", budget.client.Transport)
			}
			if budget.builders != builders {
				t.Fatalf("builders = %d, want %d", budget.builders, builders)
			}
			if pooled.MaxIdleConnsPerHost != builders {
				t.Fatalf("MaxIdleConnsPerHost = %d, want %d (the semaphore bound); an unpaired pool churns connections",
					pooled.MaxIdleConnsPerHost, builders)
			}
			if pooled.MaxIdleConns != builders {
				t.Fatalf("MaxIdleConns = %d, want %d", pooled.MaxIdleConns, builders)
			}
		})
	}
}

func TestBootBudgetRefusesANonPositiveBuilderLimit(t *testing.T) {
	if budget := newBootBudget(0); budget.builders != 1 {
		t.Fatalf("newBootBudget(0).builders = %d, want 1", budget.builders)
	}
}

type escrowPublisher struct {
	mu          sync.Mutex
	added       []string
	retired     []string
	deactivated []string

	failures map[string]error

	inFlight atomic.Int64
	peak     atomic.Int64
	hold     chan struct{}
}

func (p *escrowPublisher) add(_ context.Context, escrowID, _ string) error {
	inFlight := p.inFlight.Add(1)
	for {
		peak := p.peak.Load()
		if inFlight <= peak || p.peak.CompareAndSwap(peak, inFlight) {
			break
		}
	}
	if p.hold != nil {
		<-p.hold
	}
	p.inFlight.Add(-1)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.added = append(p.added, escrowID)
	return p.failures[escrowID]
}

func (p *escrowPublisher) retire(escrowID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retired = append(p.retired, escrowID)
	return nil
}

func (p *escrowPublisher) deactivate(escrowID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deactivated = append(p.deactivated, escrowID)
	return nil
}

func devshard(escrowID string, active bool) store.DevshardRecord {
	return store.DevshardRecord{EscrowID: escrowID, Model: "model-a", Active: active}
}

func TestPublishEscrowsWalksTheThreeArmedBuildLadder(t *testing.T) {
	buildFailure := errors.New("chain REST unreachable")
	testCases := []struct {
		name            string
		records         []store.DevshardRecord
		failures        map[string]error
		wantAdded       []string
		wantRetired     []string
		wantDeactivated []string
		wantError       error
	}{
		{
			name:        "an inactive devshard is never built and is retired from routing",
			records:     []store.DevshardRecord{devshard("dormant", false)},
			wantRetired: []string{"dormant"},
		},
		{
			name:            "an escrow the chain does not have is marked inactive and skipped",
			records:         []store.DevshardRecord{devshard("gone", true)},
			failures:        map[string]error{"gone": fmt.Errorf("opening session: %w", bridge.ErrEscrowNotFound)},
			wantAdded:       []string{"gone"},
			wantDeactivated: []string{"gone"},
		},
		{
			name:            "an escrow whose key the environment lacks is marked inactive and skipped",
			records:         []store.DevshardRecord{devshard("keyless", true)},
			failures:        map[string]error{"keyless": fmt.Errorf("opening session: %w", env.ErrPrivateKeyMissing)},
			wantAdded:       []string{"keyless"},
			wantDeactivated: []string{"keyless"},
		},
		{
			name:      "any other failure is fatal and marks nothing inactive",
			records:   []store.DevshardRecord{devshard("unreachable", true)},
			failures:  map[string]error{"unreachable": buildFailure},
			wantAdded: []string{"unreachable"},
			wantError: buildFailure,
		},
		{
			name:      "a healthy devshard is published",
			records:   []store.DevshardRecord{devshard("serving", true)},
			wantAdded: []string{"serving"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			publisher := &escrowPublisher{failures: testCase.failures}

			err := publishEscrows(context.Background(), testCase.records, 4,
				publisher.add, publisher.retire, publisher.deactivate)

			if testCase.wantError == nil && err != nil {
				t.Fatalf("publishEscrows() = %v, want nil", err)
			}
			if testCase.wantError != nil && !errors.Is(err, testCase.wantError) {
				t.Fatalf("publishEscrows() = %v, want %v", err, testCase.wantError)
			}
			assertSame(t, "added", publisher.added, testCase.wantAdded)
			assertSame(t, "retired", publisher.retired, testCase.wantRetired)
			assertSame(t, "deactivated", publisher.deactivated, testCase.wantDeactivated)
		})
	}
}

func assertSame(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for index := range got {
		if got[index] != want[index] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}

func TestPublishEscrowsBuildsNoMoreThanTheBuilderLimitAtOnce(t *testing.T) {
	const builders = 3
	records := make([]store.DevshardRecord, 0, 12)
	for index := range 12 {
		records = append(records, devshard(fmt.Sprintf("escrow-%d", index), true))
	}
	publisher := &escrowPublisher{hold: make(chan struct{})}

	done := make(chan error, 1)
	go func() {
		done <- publishEscrows(context.Background(), records, builders,
			publisher.add, publisher.retire, publisher.deactivate)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for publisher.inFlight.Load() < builders {
		if time.Now().After(deadline) {
			t.Fatalf("only %d builders ever ran at once, want %d", publisher.peak.Load(), builders)
		}
		time.Sleep(time.Millisecond)
	}
	close(publisher.hold)
	if err := <-done; err != nil {
		t.Fatalf("publishEscrows(): %v", err)
	}
	if peak := publisher.peak.Load(); peak > builders {
		t.Fatalf("%d builders ran at once, want at most %d", peak, builders)
	}
}

type shutdownRecorder struct {
	mu       sync.Mutex
	sequence *[]string
	name     string
	err      error
}

func (r *shutdownRecorder) note() {
	r.mu.Lock()
	defer r.mu.Unlock()
	*r.sequence = append(*r.sequence, r.name)
}

func (r *shutdownRecorder) Shutdown(context.Context) error { r.note(); return r.err }
func (r *shutdownRecorder) Stop()                          { r.note() }
func (r *shutdownRecorder) Close() error                   { r.note(); return r.err }
func (r *shutdownRecorder) CloseIdleConnections()          { r.note() }

func TestShutdownStopsAcceptingFirstAndClosesTheStoreLast(t *testing.T) {
	var sequence []string
	recorder := func(name string) *shutdownRecorder {
		return &shutdownRecorder{sequence: &sequence, name: name}
	}

	steps := shutdownOrder(
		recorder("http server"), recorder("races"), recorder("dispatchers"),
		recorder("escrow lifecycle"), recorder("chain observer"),
		recorder("escrow sessions"), recorder("nonce accounting"), recorder("store"),
		recorder("public api connections"))
	if err := stopAll(context.Background(), steps); err != nil {
		t.Fatalf("stopAll(): %v", err)
	}

	want := []string{"http server", "races", "dispatchers", "escrow lifecycle", "chain observer", "escrow sessions", "nonce accounting", "store", "public api connections"}
	assertSame(t, "shutdown sequence", sequence, want)
}

// An escrow session is closed only once the work that reaches it has stopped. See
// gateway-invariants.md, "6. Shutdown order is a contract".
func TestShutdownSkipsEscrowSessionsWhenADrainOverran(t *testing.T) {
	var sequence []string
	record := func(name string) func(context.Context) error {
		return func(context.Context) error { sequence = append(sequence, name); return nil }
	}
	steps := []shutdownStep{
		{name: "races", stop: func(context.Context) error { return errors.New("abandoned with work still running") }},
		{name: "escrow sessions", stop: record("escrow sessions"), needsQuiesced: true},
		{name: "store", stop: record("store")},
	}

	err := stopAll(context.Background(), steps)

	if err == nil {
		t.Fatal("stopAll reported success after abandoning a drain")
	}
	for _, name := range sequence {
		if name == "escrow sessions" {
			t.Fatal("escrow sessions were closed while a drain was still running")
		}
	}
	if len(sequence) != 1 || sequence[0] != "store" {
		t.Fatalf("sequence = %v, want the store still reached", sequence)
	}
}

func TestShutdownReachesTheStoreEvenWhenAnEarlierStepFails(t *testing.T) {
	var sequence []string
	recorder := func(name string) *shutdownRecorder {
		return &shutdownRecorder{sequence: &sequence, name: name}
	}
	failing := &shutdownRecorder{sequence: &sequence, name: "http server", err: errors.New("listener stuck")}

	steps := shutdownOrder(
		failing, recorder("races"), recorder("dispatchers"),
		recorder("escrow lifecycle"), recorder("chain observer"),
		recorder("escrow sessions"), recorder("nonce accounting"), recorder("store"),
		recorder("public api connections"))
	err := stopAll(context.Background(), steps)

	if err == nil || !strings.Contains(err.Error(), "listener stuck") {
		t.Fatalf("stopAll() = %v, want the listener failure reported", err)
	}
	if !slices.Contains(sequence, "store") {
		t.Fatalf("shutdown sequence = %v, want the store closed despite the earlier failure", sequence)
	}
}

// blockingStopper is a component whose drain never finishes on its own.
type blockingStopper struct {
	released chan struct{}
	returned chan struct{}
}

func newBlockingStopper(t *testing.T) *blockingStopper {
	t.Helper()
	blocked := &blockingStopper{released: make(chan struct{}), returned: make(chan struct{})}
	t.Cleanup(func() {
		close(blocked.released)
		<-blocked.returned
	})
	return blocked
}

func (b *blockingStopper) Stop() {
	<-b.released
	close(b.returned)
}

// A drain that cannot finish inside the budget must yield it to the steps below. See
// gateway-architecture.md, "Shutdown".
func TestWaitForYieldsTheBudgetToTheStepsBelowADrainThatOutlastsIt(t *testing.T) {
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelDrain()

	err := waitFor(newBlockingStopper(t))(drainCtx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitFor() over a drain that outlasts the budget = %v, want the exceeded deadline reported", err)
	}
}

func TestWaitForReportsNothingWhenTheDrainFinishesInTime(t *testing.T) {
	var sequence []string
	if err := waitFor(&shutdownRecorder{sequence: &sequence, name: "races"})(context.Background()); err != nil {
		t.Fatalf("waitFor() over a drain that finishes = %v, want nil", err)
	}
	assertSame(t, "drained components", sequence, []string{"races"})
}

// settleObserver reports what routing still offered at the moment the chain settlement began.
type settleObserver struct {
	escrows          *registry.Registry
	failure          error
	routableAtSettle bool
}

func (s *settleObserver) CreateEscrow(context.Context, escrow.ModelConfig) (chain.CreateEscrowResult, error) {
	return chain.CreateEscrowResult{}, nil
}

func (s *settleObserver) Settle(_ context.Context, escrowID string) (chain.SettleEscrowResult, error) {
	_, s.routableAtSettle = s.escrows.RoutableSession(escrowID)
	return chain.SettleEscrowResult{}, s.failure
}

// A nonce committed after the settlement payload has been built is either missing from the state root
// the chain recomputes or rejects the transaction outright, so routing must stop first. A settlement
// that fails leaves the escrow retired: its row is inactive and settlement-pending by then, which is
// where the lifecycle picks it up again.
func TestSettleStopsRoutingBeforeTheChainSettlementAndLeavesItRetiredWhenItFails(t *testing.T) {
	testCases := []struct {
		name    string
		failure error
	}{
		{name: "a settlement that succeeds"},
		{name: "a settlement the chain refused", failure: errors.New("broadcast refused")},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			escrows := registry.New(registry.Deps{
				ServingSessions: func(context.Context, string) (registry.EscrowSession, error) {
					return weightlessSession{participants: []string{"validator-a"}}, nil
				},
				Now: time.Now,
			})
			t.Cleanup(func() { escrows.Close() })
			if err := escrows.Add(context.Background(), "escrow-1", "model-a"); err != nil {
				t.Fatalf("Add(): %v", err)
			}
			settler := &settleObserver{escrows: escrows, failure: testCase.failure}
			operator := &operations{escrows: escrows, manager: settler}

			_, err := operator.Settle(context.Background(), "escrow-1")

			if !errors.Is(err, testCase.failure) {
				t.Fatalf("Settle() = %v, want %v", err, testCase.failure)
			}
			if settler.routableAtSettle {
				t.Error("the escrow was still routable when the settlement began: a nonce can land after the payload was built")
			}
			if _, routable := escrows.RoutableSession("escrow-1"); routable {
				t.Error("the escrow is routable after the settle: it claims funds against nonces routing can still spend")
			}
		})
	}
}

// A key no limiter state is tracked under is a typo, not a cleared quarantine, and answering 200 tells
// the operator a host was reopened that never existed.
func TestUnquarantiningAnUntrackedParticipantIsNotReportedAsDone(t *testing.T) {
	configuration := config.Defaults()
	limiter := limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(configuration.Limits), time.Now)
	limiter.OnResult("validator-a", "model-a", limits.TransportFault)
	operator := &operations{participants: limiter}

	if err := operator.Unquarantine(context.Background(), "validator-typo"); !errors.Is(err, api.ErrUnknownParticipant) {
		t.Fatalf("Unquarantine(unknown) = %v, want ErrUnknownParticipant", err)
	}
	if err := operator.Unquarantine(context.Background(), "validator-a"); err != nil {
		t.Fatalf("Unquarantine(tracked) = %v, want nil", err)
	}
}

// weightlessSession is enough of an escrow for the registry to publish it and push its membership;
// it cannot commit a nonce, because *user.PreparedInference has no constructor outside its package.
type weightlessSession struct{ participants []string }

func (weightlessSession) Balance() uint64    { return 1 << 40 }
func (weightlessSession) TokenPrice() uint64 { return 1 }

func (s weightlessSession) ParticipantKeys() []string        { return s.participants }
func (s weightlessSession) HostParticipantKeyList() []string { return s.participants }
func (s weightlessSession) Nonce() uint64                    { return 1 }
func (s weightlessSession) Phase() types.SessionPhase        { return types.PhaseActive }
func (s weightlessSession) Signatures() map[uint64]map[uint32][]byte {
	return map[uint64]map[uint32][]byte{}
}

func (s weightlessSession) SignatureStatus() ([]user.SignatureStatusEntry, uint64, bool) {
	return nil, 0, false
}

func (s weightlessSession) SignedSlots() map[uint64]types.Bitmap128 { return nil }
func (s weightlessSession) SnapshotState() types.EscrowState        { return types.EscrowState{} }
func (s weightlessSession) SealedInferences() int                   { return 0 }
func (s weightlessSession) Finalize(context.Context) error          { return nil }
func (s weightlessSession) FlushSnapshot() error                    { return nil }
func (s weightlessSession) Close() error                            { return nil }
func (s weightlessSession) UserSession() *user.Session              { return nil }

func (s weightlessSession) PrepareInferenceFn(user.ParamsForHost) (*user.PreparedInference, error) {
	return nil, nil
}

func TestPublishingAnEscrowGivesItWeightAndMakesItPickable(t *testing.T) {
	participants := []string{"validator-a", "validator-b"}
	capacity := limits.NewCapacity(func(string, string) bool { return true })
	capacity.Update(chain.PhaseSnapshot{
		CurrentWeights: map[string]float64{"validator-a": 1000, "validator-b": 1000},
		FullWeights:    map[string]float64{"validator-a": 1000, "validator-b": 1000},
	})
	configuration := config.Defaults()
	configHolder := config.NewHolder(&configuration)

	observer, err := chain.NewPhaseObserver(chain.ObserverConfig{PublicAPIBaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	escrows, router, _ := newRouting(routingDeps{
		Sessions: func(context.Context, string) (registry.EscrowSession, error) {
			return weightlessSession{participants: participants}, nil
		},
		Capacity:     capacity,
		Participants: limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(configuration.Limits), time.Now),
		Hosts:        perf.NewTracker(configHolder, time.Now),
		Snapshots:    observer,
		Config:       configHolder,
		Depletion:    &depletionNotice{},
		Now:          time.Now,
	})
	t.Cleanup(func() { escrows.Close() })
	t.Cleanup(router.Stop)

	if err := escrows.Add(context.Background(), "escrow-1", "model-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}

	if weight := capacity.EscrowWeight("escrow-1", "model-a"); weight <= 0 {
		t.Fatalf("EscrowWeight() = %v, want > 0; membership never reached the capacity model, so every candidate scores as weightless", weight)
	}
	// The fake session cannot hand back a committed nonce, so the pick is bounded and judged on which
	// failure it reaches: a weightless escrow is never selected at all.
	pickCtx, cancelPick := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelPick()
	_, err = router.Pick(pickCtx, scheduler.RequestProfile{Model: "model-a", InputTokens: 4})
	if errors.Is(err, scheduler.ErrNoEscrowCapacity) {
		t.Fatalf("Pick() = %v, want the escrow to be selected; a weightless escrow is skipped and the gateway serves nothing", err)
	}
}

// routingFor builds the escrow set, the capacity model and the picker the way compose does, so a pick
// is judged on the wiring the gateway actually runs rather than on a hand-built scheduler.
func routingFor(t *testing.T, capacity *limits.Capacity, participants []string) *scheduler.Scheduler {
	t.Helper()
	configuration := config.Defaults()
	configHolder := config.NewHolder(&configuration)
	observer, err := chain.NewPhaseObserver(chain.ObserverConfig{PublicAPIBaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}
	escrows, router, _ := newRouting(routingDeps{
		Sessions: func(context.Context, string) (registry.EscrowSession, error) {
			return weightlessSession{participants: participants}, nil
		},
		Capacity:     capacity,
		Participants: limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(configuration.Limits), time.Now),
		Hosts:        perf.NewTracker(configHolder, time.Now),
		Snapshots:    observer,
		Config:       configHolder,
		Depletion:    &depletionNotice{},
		Now:          time.Now,
	})
	t.Cleanup(func() { escrows.Close() })
	t.Cleanup(router.Stop)
	if err := escrows.Add(context.Background(), "escrow-1", "model-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}
	return router
}

// The chain reporting nothing is missing data, not zero capacity: routing on it must still reach a
// host, because ErrNoEscrowCapacity is what the client is served as a 429.
func TestChainWeightsTheGatewayHasNotObservedStillRouteARequest(t *testing.T) {
	cases := []struct {
		name       string
		snapshot   chain.PhaseSnapshot
		wantRouted bool
	}{
		{
			name:       "a fresh poll that named no participants routes on membership alone",
			snapshot:   chain.PhaseSnapshot{LastUpdatedAt: time.Now()},
			wantRouted: true,
		},
		{
			name: "a stale poll routes on the weights it last observed",
			snapshot: chain.PhaseSnapshot{
				CurrentWeights: map[string]float64{"validator-a": 1000, "validator-b": 1000},
				FullWeights:    map[string]float64{"validator-a": 1000, "validator-b": 1000},
				LastError:      "fetch participants: connection refused",
			},
			wantRouted: true,
		},
		{
			name: "a fresh poll that named every participant at zero weight is honoured",
			snapshot: chain.PhaseSnapshot{
				CurrentWeights: map[string]float64{"validator-a": 0, "validator-b": 0},
				FullWeights:    map[string]float64{"validator-a": 0, "validator-b": 0},
				LastUpdatedAt:  time.Now(),
			},
			wantRouted: false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			capacity := limits.NewCapacity(func(string, string) bool { return true })
			capacity.Update(testCase.snapshot)
			router := routingFor(t, capacity, []string{"validator-a", "validator-b"})

			// The fake session cannot hand back a committed nonce, so the pick is bounded and judged on
			// which failure it reaches: a weightless escrow is never selected at all.
			pickCtx, cancelPick := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancelPick()
			_, err := router.Pick(pickCtx, scheduler.RequestProfile{Model: "model-a", InputTokens: 4})

			if routed := !errors.Is(err, scheduler.ErrNoEscrowCapacity); routed != testCase.wantRouted {
				t.Fatalf("Pick() = %v, routed = %v, want routed = %v", err, routed, testCase.wantRouted)
			}
		})
	}
}

func TestReconfiguringTheGatewayChangesTheCapTheNextRequestIsJudgedAgainst(t *testing.T) {
	gatewayEnvironment(t)
	composed := composedGateway(t)

	widened := config.Defaults()
	widened.Limits.Concurrency.MaxRequests = 1
	widened.Limits.AdmissionQueueWaitMS = 0
	composed.config.Swap(&widened)

	if err := composed.limiter.AcquireForModel(context.Background(), "model-a", 1, limits.ModelCapacity{ScaleFactor: 1}); err != nil {
		t.Fatalf("first AcquireForModel() under a cap of 1 = %v, want admitted", err)
	}
	var rateLimited *limits.RateLimitError
	if err := composed.limiter.AcquireForModel(context.Background(), "model-a", 1, limits.ModelCapacity{ScaleFactor: 1}); !errors.As(err, &rateLimited) {
		t.Fatalf("second AcquireForModel() under a cap of 1 = %v, want a rate-limit error; the limiter never saw the new configuration", err)
	}
	composed.limiter.ReleaseForModel("model-a", 1)
}

func TestSuspiciousHostsAreWrittenThroughToTheStoreTheEngineReadsFrom(t *testing.T) {
	records, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := records.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	ctx := context.Background()

	pins, err := newSuspiciousHosts(ctx, records)
	if err != nil {
		t.Fatalf("newSuspiciousHosts(): %v", err)
	}
	if err := pins.Add(ctx, "validator-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}
	if !pins.Suspicious("validator-a") {
		t.Error("the predicate the engine is given does not see a host the admin route just pinned")
	}

	restarted, err := newSuspiciousHosts(ctx, records)
	if err != nil {
		t.Fatalf("newSuspiciousHosts() after a restart: %v", err)
	}
	if !restarted.Suspicious("validator-a") {
		t.Error("the pin did not reach the store: a restart forgets it")
	}

	if err := restarted.Remove(ctx, "validator-a"); err != nil {
		t.Fatalf("Remove(): %v", err)
	}
	reloaded, err := newSuspiciousHosts(ctx, records)
	if err != nil {
		t.Fatalf("newSuspiciousHosts() after an unpin: %v", err)
	}
	if reloaded.Suspicious("validator-a") {
		t.Error("the unpin did not reach the store")
	}
}
