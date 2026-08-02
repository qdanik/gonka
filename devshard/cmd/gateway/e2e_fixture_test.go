package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/store"
	"devshard/host"
	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/state"
	"devshard/storage"
	"devshard/stub"
	"devshard/user"
)

const (
	e2eModel      = "e2e-model"
	e2eHostCount  = 3
	e2eBalance    = 5_000_000
	e2eGrace      = 1
	e2eSettleWait = 5 * time.Second
)

// e2eCreatorKeys are fixed so an escrow's creator address, and therefore every state root derived from
// it, is the same on every run.
var e2eCreatorKeys = []string{
	"1111111111111111111111111111111111111111111111111111111111111111",
	"2222222222222222222222222222222222222222222222222222222222222222",
	"3333333333333333333333333333333333333333333333333333333333333333",
}

// eventFlusher mirrors what the HTTP transport does with a host's SSE lines: write one event, flush it.
// Without the flush a crowned winner's bytes sit in the server's buffer until the response ends.
type eventFlusher struct{ inner io.Writer }

func (e eventFlusher) Write(chunk []byte) (int, error) {
	written, err := e.inner.Write(chunk)
	if flusher, isFlusher := e.inner.(http.Flusher); isFlusher {
		flusher.Flush()
	}
	return written, err
}

// scriptedHost drives a real in-process host.Host through user.InProcessClient — real diffs, real state
// roots, real signatures, a real MsgFinishInference — and adds only the failure modes a test scripts.
type scriptedHost struct {
	inner *user.InProcessClient

	entered chan struct{}
	release chan struct{}

	divergent bool
	failure   error

	dispatches atomic.Int64
}

func (h *scriptedHost) Send(ctx context.Context, request host.HostRequest, stream io.Writer, onReceipt func()) (*host.HostResponse, error) {
	h.dispatches.Add(1)
	if h.entered != nil {
		h.entered <- struct{}{}
	}
	if h.release != nil {
		<-h.release
	}
	if h.failure != nil {
		return nil, h.failure
	}
	if stream != nil {
		stream = eventFlusher{inner: stream}
	}
	reply, err := h.inner.Send(ctx, request, stream, onReceipt)
	if reply != nil && h.divergent && len(reply.StateHash) > 0 {
		corrupted := append([]byte(nil), reply.StateHash...)
		corrupted[0] ^= 0xFF
		reply.StateHash = corrupted
	}
	return reply, err
}

func (h *scriptedHost) dispatched() int64 { return h.dispatches.Load() }

// escrowFixture is one escrow served by real hosts over the same on-disk storage production opens.
type escrowFixture struct {
	id            string
	model         string
	storagePath   string
	privateKeyHex string
	hosts         []*scriptedHost
	participants  []string
	session       *user.Session
	machine       *state.StateMachine
	opening       func()
}

func newEscrowFixture(t *testing.T, storageDir, escrowID, model string, index int, refusalTimeoutSeconds int64, balance uint64) *escrowFixture {
	t.Helper()
	if balance == 0 {
		balance = e2eBalance
	}
	creator, err := signing.SignerFromHex(e2eCreatorKeys[index])
	if err != nil {
		t.Fatalf("SignerFromHex = %v, want nil", err)
	}
	signers := make([]*signing.Secp256k1Signer, e2eHostCount)
	for slot := range signers {
		signers[slot] = testutil.MustGenerateKey(t)
	}
	group := testutil.MakeGroup(signers)
	sessionConfig := testutil.DefaultConfig(e2eHostCount)
	// The protocol's timeout deadlines are wall-clock and the gateway's clock is the process clock, so
	// a nonce this suite leaves unsettled is due as soon as the protocol's own buffer has passed, unless
	// a test asks for a refusal deadline the drain it is about has to outlive.
	sessionConfig.RefusalTimeout = refusalTimeoutSeconds
	sessionConfig.ExecutionTimeout = 0
	verifier := signing.NewSecp256k1Verifier()

	storagePath := api.DevshardStoragePath(storageDir, escrowID)
	sessionStore, err := storage.NewSQLite(storagePath)
	if err != nil {
		t.Fatalf("NewSQLite(%s) = %v, want nil", storagePath, err)
	}
	if err := sessionStore.CreateSession(storage.CreateSessionParams{
		EscrowID:       escrowID,
		EpochID:        1,
		Version:        testutil.RuntimeTestVersion,
		CreatorAddr:    creator.Address(),
		Config:         sessionConfig,
		Group:          group,
		InitialBalance: balance,
	}); err != nil {
		t.Fatalf("CreateSession = %v, want nil", err)
	}

	hosts := make([]*scriptedHost, e2eHostCount)
	clients := make([]user.HostClient, e2eHostCount)
	participants := make([]string, e2eHostCount)
	for slot := range signers {
		hostStore := testutil.MustMemoryStore(t, escrowID, creator.Address(), sessionConfig, group, balance)
		hostMachine, err := state.NewStateMachine(escrowID, sessionConfig, group, balance, creator.Address(), verifier, hostStore)
		if err != nil {
			t.Fatalf("NewStateMachine(host %d) = %v, want nil", slot, err)
		}
		served, err := host.NewHost(hostMachine, signers[slot], stub.NewInferenceEngine(), escrowID, group, nil, host.WithGrace(e2eGrace))
		if err != nil {
			t.Fatalf("NewHost(%d) = %v, want nil", slot, err)
		}
		t.Cleanup(served.Close)
		hosts[slot] = &scriptedHost{inner: &user.InProcessClient{Host: served}}
		clients[slot] = hosts[slot]
		participants[slot] = signers[slot].Address()
	}

	machine, err := state.NewStateMachine(escrowID, sessionConfig, group, balance, creator.Address(), verifier, sessionStore)
	if err != nil {
		t.Fatalf("NewStateMachine(session) = %v, want nil", err)
	}
	session, err := user.NewSession(machine, creator, escrowID, group, clients, verifier, user.WithStorage(sessionStore))
	if err != nil {
		t.Fatalf("NewSession = %v, want nil", err)
	}
	return &escrowFixture{
		id:            escrowID,
		model:         model,
		storagePath:   storagePath,
		privateKeyHex: e2eCreatorKeys[index],
		hosts:         hosts,
		participants:  participants,
		session:       session,
		machine:       machine,
	}
}

func (f *escrowFixture) dispatches() int64 {
	var total int64
	for _, served := range f.hosts {
		total += served.dispatched()
	}
	return total
}

// parkHosts holds every dispatch inside Send until releaseHosts: the window a retire or a shutdown
// needs in order to overlap a race that already holds a committed nonce. The gate is opened again on
// cleanup because shutdown is a barrier over the race a parked host holds, so a failed assertion that
// left one parked would hang the package rather than fail it.
func (f *escrowFixture) parkHosts(t *testing.T) {
	t.Helper()
	for _, served := range f.hosts {
		served.entered = make(chan struct{}, 1)
		served.release = make(chan struct{})
	}
	f.opening = sync.OnceFunc(func() {
		for _, served := range f.hosts {
			close(served.release)
		}
	})
	t.Cleanup(f.releaseHosts)
}

func (f *escrowFixture) releaseHosts() {
	if f.opening != nil {
		f.opening()
	}
}

// failHosts leaves every dispatch unanswered, so the nonce the race committed stays unfinished and owes
// the timeout vote that is the only thing able to resolve its MsgStart.
func (f *escrowFixture) failHosts(failure error) {
	for _, served := range f.hosts {
		served.failure = failure
	}
}

// awaitDispatch blocks until some host has been reached, so the overlap a test arranges is real
// rather than assumed.
func (f *escrowFixture) awaitDispatch(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(e2eSettleWait)
	for {
		for _, served := range f.hosts {
			select {
			case <-served.entered:
				return
			default:
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no host was reached: the request never dispatched")
		}
		time.Sleep(time.Millisecond)
	}
}

type e2eGateway struct {
	*gateway
	fixtures   map[string]*escrowFixture
	storageDir string

	mu      sync.Mutex
	stopped bool
}

// e2eOptions tunes one fixture. A set epochPhase starts the observer against a fake chain reporting it, so
// admission is judged on a polled snapshot rather than one a test wrote in; a 0 balance takes e2eBalance.
type e2eOptions struct {
	escrows               []string
	adminKey              string
	apiKeys               string
	pocMode               string
	epochPhase            string
	refusalTimeoutSeconds int64
	balance               uint64
	tune                  func(*config.Config)
}

// fakeChainInPhase answers the observer's epoch poll with one phase and names the fixtures' own hosts
// as the epoch's participants, so the weights the capacity model scales on are polled, not injected.
func fakeChainInPhase(t *testing.T, phase string, participants []string) string {
	t.Helper()
	entries := make([]string, 0, len(participants))
	for _, participant := range participants {
		entries = append(entries, fmt.Sprintf(
			`{"index":%q,"inference_url":"http://in-process","models":[%q],"ml_nodes":[{"ml_nodes":[{"node_id":%q,"timeslot_allocation":[true,true],"poc_weight":"1000"}]}]}`,
			participant, e2eModel, participant))
	}
	body := fmt.Sprintf(`{"active_participants":{"participants":[%s]}}`, strings.Join(entries, ","))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/epochs/latest"):
			fmt.Fprintf(w, `{"block_height":"100","phase":%q,"latest_epoch":{"index":"7","poc_start_block_height":"90"}}`, phase)
		case strings.HasSuffix(r.URL.Path, "/participants"):
			fmt.Fprint(w, body)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// bootGateway builds the process through compose, so every assertion below reaches the wiring the
// gateway runs rather than a re-declaration of it, and publishes the escrows the way boot does.
func bootGateway(t *testing.T, options e2eOptions) *e2eGateway {
	t.Helper()
	if len(options.escrows) == 0 {
		options.escrows = []string{"1"}
	}
	storageDir := t.TempDir()
	fixtures := map[string]*escrowFixture{}
	var participants []string
	for index, escrowID := range options.escrows {
		fixtures[escrowID] = newEscrowFixture(t, storageDir, escrowID, e2eModel, index, options.refusalTimeoutSeconds, options.balance)
		participants = append(participants, fixtures[escrowID].participants...)
	}

	chainURL := fakeChain(t)
	if options.epochPhase != "" {
		chainURL = fakeChainInPhase(t, options.epochPhase, participants)
	}
	t.Setenv("GATEWAY_STORAGE_DIR", storageDir)
	t.Setenv("GATEWAY_PUBLIC_API", chainURL)
	if options.adminKey != "" {
		t.Setenv("GATEWAY_ADMIN_API_KEY", options.adminKey)
	}
	if options.apiKeys != "" {
		t.Setenv("GATEWAY_API_KEYS", options.apiKeys)
	}
	if options.pocMode != "" {
		t.Setenv("GATEWAY_POC_MODE", options.pocMode)
	}

	values, err := env.Load()
	if err != nil {
		t.Fatalf("env.Load = %v, want nil", err)
	}
	gatewayStore, err := store.Open(storageDir)
	if err != nil {
		t.Fatalf("store.Open = %v, want nil", err)
	}
	served := &e2eGateway{fixtures: fixtures, storageDir: storageDir}
	for _, escrowID := range options.escrows {
		if err := gatewayStore.UpsertDevshard(context.Background(), store.DevshardRecord{
			EscrowID: escrowID, Model: e2eModel, PrivateKeyEnv: "GATEWAY_E2E_KEY", Active: true,
		}); err != nil {
			t.Fatalf("UpsertDevshard(%s) = %v, want nil", escrowID, err)
		}
	}

	composed, err := compose(context.Background(), values, storageDir, gatewayStore, served.sessions)
	if err != nil {
		gatewayStore.Close()
		t.Fatalf("compose = %v, want nil", err)
	}
	served.gateway = composed

	// One attempt per race unless a test asks otherwise: escalation onto a second nonce is what makes
	// the nonces, votes and ledger rows a test counts stop being the ones it arranged.
	settings := *composed.config.Load()
	settings.Engine.MaxSpeculativeAttempts = 1
	if options.tune != nil {
		options.tune(&settings)
	}
	composed.config.Swap(&settings)

	if err := composed.publishEscrows(context.Background()); err != nil {
		t.Fatalf("publishEscrows = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := served.shutdownNow(); err != nil {
			t.Errorf("shutdown = %v, want nil", err)
		}
	})
	if options.epochPhase != "" {
		observerCtx, stopObserver := context.WithCancel(context.Background())
		t.Cleanup(stopObserver)
		composed.observer.Start(observerCtx)
		served.awaitChainPhase(t, options.epochPhase)
	}
	return served
}

// awaitChainPhase waits on the poll the observer runs, not on a duration.
func (g *e2eGateway) awaitChainPhase(t *testing.T, phase string) {
	t.Helper()
	deadline := time.Now().Add(e2eSettleWait)
	for {
		if string(g.gateway.observer.Snapshot().EpochPhase) == phase {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the observer never reported phase %q; admission is judged on a snapshot it never polled", phase)
		}
		time.Sleep(time.Millisecond)
	}
}

// sessions is the composition seam: the gateway opens each escrow over in-process hosts instead of over
// the chain and the hosts' HTTP endpoints, and over the same on-disk storage either way.
// inProcessChain answers the chain reads without dialing, the way the fake chain server's empty bodies
// used to: no params, no preserved snapshot, no escrow on chain. A broadcast is refused outright --
// an e2e fixture that reached one would be signing against a chain that is not there.
type inProcessChain struct{}

func (inProcessChain) MaxNonce(context.Context) (uint64, bool, error) { return 0, false, nil }

func (inProcessChain) PreservedNodes(context.Context) (*chain.PreservedNodes, bool, error) {
	return nil, false, nil
}

func (inProcessChain) ChainID(context.Context) (string, error) { return "gonka-e2e", nil }

func (inProcessChain) Account(context.Context, string) (chain.Account, error) {
	return chain.Account{}, nil
}

func (inProcessChain) Broadcast(context.Context, []byte) (string, error) {
	return "", fmt.Errorf("in-process chain broadcasts nothing")
}

func (inProcessChain) Tx(context.Context, string) (chain.TxResult, bool, error) {
	return chain.TxResult{}, false, nil
}

func (inProcessChain) Escrow(context.Context, uint64) (chain.EscrowInfo, bool, error) {
	return chain.EscrowInfo{}, false, nil
}

func (g *e2eGateway) sessions(config.Chain, string) (chainSources, error) {
	serving := func(_ context.Context, escrowID string) (registry.EscrowSession, error) {
		fixture, known := g.fixtures[escrowID]
		if !known {
			return nil, fmt.Errorf("escrow %s has no in-process fixture", escrowID)
		}
		return registry.NewSessionHandle(fixture.session, fixture.machine), nil
	}
	readOnly := func(_ context.Context, escrowID string) (registry.EscrowSession, error) {
		fixture, known := g.fixtures[escrowID]
		if !known {
			return nil, fmt.Errorf("escrow %s has no in-process fixture: %w", escrowID, escrow.ErrUnknownEscrow)
		}
		session, machine, err := user.NewLocalSession(user.LocalSessionConfig{
			PrivateKeyHex: fixture.privateKeyHex,
			EscrowID:      escrowID,
			StoragePath:   fixture.storagePath,
		})
		if err != nil {
			return nil, err
		}
		return registry.NewSessionHandle(session, machine), nil
	}
	return chainSources{Serving: serving, ReadOnly: readOnly, Reader: inProcessChain{}, Transport: inProcessChain{}}, nil
}

func (g *e2eGateway) only() *escrowFixture {
	for _, fixture := range g.fixtures {
		return fixture
	}
	return nil
}

// shutdownNow runs the process's own shutdown once, so a test that drives it and the cleanup that
// guarantees it do not close the store twice.
func (g *e2eGateway) shutdownNow() error { return g.shutdownWithin(shutdownGracePeriod) }

func (g *e2eGateway) shutdownWithin(grace time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return nil
	}
	g.stopped = true
	return g.gateway.shutdown(grace)
}

// capacityWeight reads the escrow's effective weight off the gateway's own exposition, which is the
// only place outside the composition root that holds it. A weightless escrow is skipped by every pick.
func (g *e2eGateway) capacityWeight(t *testing.T, escrowID string) float64 {
	t.Helper()
	prefix := fmt.Sprintf("devshard_gateway_escrow_weight{devshard_id=%q}", escrowID)
	for line := range strings.SplitSeq(g.scrapeMetrics(t), "\n") {
		value, found := strings.CutPrefix(line, prefix)
		if !found {
			continue
		}
		weight, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			t.Fatalf("parsing %q: %v", line, err)
		}
		return weight
	}
	t.Fatalf("no escrow weight is exposed for %s", escrowID)
	return 0
}

// clientResponse is what a caller saw: the status and headers as they were when the first byte went
// out, and the ordered write/flush events beneath them. httptest.ResponseRecorder hands back a live
// header map, so a header set after the body started would read as if the client had received it.
type clientResponse struct {
	status  int
	header  http.Header
	body    []byte
	events  []string
	flushes int
}

type recordingWriter struct {
	mu       sync.Mutex
	header   http.Header
	snapshot http.Header
	status   int
	body     []byte
	events   []string
	flushes  int
	written  bool
}

func newRecordingWriter() *recordingWriter {
	return &recordingWriter{header: http.Header{}, status: http.StatusOK}
}

func (w *recordingWriter) Header() http.Header { return w.header }

func (w *recordingWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.written {
		return
	}
	w.written = true
	w.status = status
	w.snapshot = w.header.Clone()
	w.events = append(w.events, fmt.Sprintf("status %d", status))
}

func (w *recordingWriter) Write(chunk []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.body = append(w.body, chunk...)
	w.events = append(w.events, "write "+string(chunk))
	return len(chunk), nil
}

func (w *recordingWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushes++
	w.events = append(w.events, "flush")
}

func (w *recordingWriter) response() clientResponse {
	w.mu.Lock()
	defer w.mu.Unlock()
	header := w.snapshot
	if header == nil {
		header = w.header.Clone()
	}
	return clientResponse{
		status:  w.status,
		header:  header,
		body:    append([]byte(nil), w.body...),
		events:  append([]string(nil), w.events...),
		flushes: w.flushes,
	}
}

type e2eRequest struct {
	method  string
	path    string
	body    string
	bearer  string
	context context.Context
}

func (g *e2eGateway) send(t *testing.T, request e2eRequest) clientResponse {
	t.Helper()
	return g.response(t, g.serve(t, request))
}

func (g *e2eGateway) serve(t *testing.T, request e2eRequest) *recordingWriter {
	t.Helper()
	writer := newRecordingWriter()
	g.gateway.server.Handler.ServeHTTP(writer, g.httpRequest(t, request))
	return writer
}

func (g *e2eGateway) response(t *testing.T, writer *recordingWriter) clientResponse {
	t.Helper()
	return writer.response()
}

func (g *e2eGateway) httpRequest(t *testing.T, request e2eRequest) *http.Request {
	t.Helper()
	method := request.method
	if method == "" {
		method = http.MethodPost
	}
	ctx := request.context
	if ctx == nil {
		ctx = context.Background()
	}
	built := httptest.NewRequestWithContext(ctx, method, request.path, strings.NewReader(request.body))
	if request.bearer != "" {
		built.Header.Set("Authorization", "Bearer "+request.bearer)
	}
	if request.body != "" {
		built.Header.Set("Content-Type", "application/json")
	}
	return built
}

func chatBody(model string, stream bool) string {
	return fmt.Sprintf(`{"model":%q,"stream":%t,"messages":[{"role":"user","content":"hello"}]}`, model, stream)
}

// distinctChatBody varies the prompt so a request is a genuine cache miss and reaches a host.
func distinctChatBody(caller int) string {
	return fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hello %d"}]}`, e2eModel, caller)
}

func (g *e2eGateway) chat(t *testing.T, stream bool) clientResponse {
	t.Helper()
	return g.send(t, e2eRequest{path: "/v1/chat/completions", body: chatBody(e2eModel, stream)})
}

func (g *e2eGateway) scrapeMetrics(t *testing.T) string {
	t.Helper()
	return string(g.send(t, e2eRequest{method: http.MethodGet, path: "/metrics"}).body)
}

func (g *e2eGateway) awaitAccounting(t *testing.T, requestID string) store.RequestRecord {
	t.Helper()
	return waitForAccountingRow(t, g.gateway.store, requestID)
}
