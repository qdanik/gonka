// The cross-package wiring tests: a real engine over a real scheduler over a real registry, holding a
// real *user.Session. Both halves of this path pass their own tests while the seam between them serves
// nothing, so the assertions here are that a nonce reaches the chain and that its vote does too.
package api

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"devshard"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/host"
	"devshard/types"
	"devshard/user"
)

const contentEvent = `data: {"choices":[{"delta":{"content":"hello"}}]}` + "\n\n"

type fixedSnapshots struct{ snapshot chain.PhaseSnapshot }

func (f fixedSnapshots) Snapshot() chain.PhaseSnapshot { return f.snapshot }

// gatedHost answers one dispatch. entered/release park it inside Send, which is the window a rotation
// needs to retire the escrow while the race still holds its nonce.
type gatedHost struct {
	entered  chan struct{}
	release  chan struct{}
	opening  sync.Once
	events   []string
	finishes bool
	err      error
}

// open lets every parked dispatch through and tolerates being called twice: shutdown is a barrier over
// the race a parked host holds, so a failed assertion must never leave one parked.
func (h *gatedHost) open() {
	h.opening.Do(func() {
		if h.release != nil {
			close(h.release)
		}
	})
}

func (h *gatedHost) Send(_ context.Context, request host.HostRequest, stream io.Writer, onReceipt func()) (*host.HostResponse, error) {
	if onReceipt != nil {
		onReceipt()
	}
	if h.entered != nil {
		h.entered <- struct{}{}
	}
	if h.release != nil {
		<-h.release
	}
	streamed := 0
	for _, event := range h.events {
		written, err := io.WriteString(stream, event)
		if err != nil {
			return nil, err
		}
		streamed += written
	}
	if h.err != nil {
		return nil, h.err
	}
	reply := &host.HostResponse{Nonce: request.Nonce, StreamBytesRead: int64(streamed)}
	if h.finishes {
		reply.Mempool = []*types.DevshardTx{{Tx: &types.DevshardTx_FinishInference{
			FinishInference: &types.MsgFinishInference{InferenceId: request.Nonce, EscrowId: liveEscrowID},
		}}}
	}
	return reply, nil
}

type recordingRaceMetrics struct {
	mu       sync.Mutex
	timeouts []engine.TimeoutEvent
}

func (m *recordingRaceMetrics) RecordRace(engine.RaceOutcome) {}

func (m *recordingRaceMetrics) RecordTimeout(event engine.TimeoutEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timeouts = append(m.timeouts, event)
}

func (m *recordingRaceMetrics) RecordClassifyOverflow(string, string) {}

func (m *recordingRaceMetrics) timeoutsRecorded() []engine.TimeoutEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]engine.TimeoutEvent(nil), m.timeouts...)
}

// gatedVotes parks the vote inside the poster the wiring resolved, which is the window the escrow must
// still be held through: the race is over and the client has already been answered.
type gatedVotes struct {
	engine.TimeoutPoster
	entered chan struct{}
	release chan struct{}
}

func (v gatedVotes) SettleTimeout(ctx context.Context, nonce uint64, sentAt time.Time) (string, error) {
	v.entered <- struct{}{}
	<-v.release
	return v.TimeoutPoster.SettleTimeout(ctx, nonce, sentAt)
}

type wiredGateway struct {
	engine  *engine.Engine
	escrows *registry.Registry
	escrow  registry.EscrowSession
	metrics *recordingRaceMetrics
	client  *clientSink
	params  user.InferenceParams

	voteEntered  chan struct{}
	releaseVotes func()
}

// newWiredGateway composes the request path the way main will. The clock is fixed a day in the past
// because the protocol's own timeout deadline is measured against the wall clock the chain shares, so a
// vote owed by this request is already due when the race ends.
func newWiredGateway(t *testing.T, upstream user.HostClient) *wiredGateway {
	t.Helper()
	session, machine := newLiveSession(t, upstream, upstream)
	escrow := registry.NewSessionHandle(session, machine)
	moment := time.Now().Add(-24 * time.Hour)
	clock := func() time.Time { return moment }

	weights := map[string]float64{}
	for _, participant := range escrow.HostParticipantKeyList() {
		weights[participant] = 50
	}
	capacity := limits.NewCapacity(nil)
	capacity.Update(chain.PhaseSnapshot{CurrentWeights: weights, FullWeights: weights})

	escrows := registry.New(registry.Deps{
		ServingSessions: func(context.Context, string) (registry.EscrowSession, error) { return escrow, nil },
		Membership:      capacity,
		Now:             clock,
	})
	if err := escrows.Add(context.Background(), liveEscrowID, liveModel); err != nil {
		t.Fatalf("Add(%s) = %v, want nil", liveEscrowID, err)
	}

	settings := config.Defaults()
	// One attempt per race: the budget is what keeps a failed attempt from escalating onto a second
	// nonce, so the nonces and the votes this test counts are the ones it arranged.
	settings.Engine.MaxSpeculativeAttempts = 1
	holder := config.NewHolder(&settings)

	limiter := limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(settings.Limits), clock)
	tracker := perf.NewTracker(holder, clock)
	router := scheduler.NewScheduler(scheduler.Deps{
		Escrows:   escrows,
		Capacity:  capacity,
		Limiter:   limiter,
		Perf:      tracker,
		Snapshots: fixedSnapshots{},
		Config:    holder,
		Now:       clock,
	})
	sessions := NewSessions(escrows)
	metrics := &recordingRaceMetrics{}
	voteEntered, voteRelease := make(chan struct{}, 8), make(chan struct{})
	releaseVotes := sync.OnceFunc(func() { close(voteRelease) })
	gateway := engine.NewEngine(engine.Deps{
		Picker:    router,
		Targets:   sessions,
		Windows:   limiter,
		Perf:      tracker,
		Snapshots: fixedSnapshots{},
		Config:    holder,
		Metrics:   metrics,
		Timeouts: func(escrowID string, params any) (engine.TimeoutPoster, bool) {
			poster, resolved := sessions.Poster(escrowID, params)
			if !resolved {
				return nil, false
			}
			return gatedVotes{TimeoutPoster: poster, entered: voteEntered, release: voteRelease}, true
		},
		Now: clock,
	})
	// The gate is opened before the barrier either way, so a parked vote can never turn into a hang.
	t.Cleanup(func() {
		releaseVotes()
		gateway.Stop()
		router.Stop()
		_ = escrows.Close()
	})

	return &wiredGateway{
		engine:       gateway,
		escrows:      escrows,
		escrow:       escrow,
		metrics:      metrics,
		client:       &clientSink{},
		params:       liveParams(),
		voteEntered:  voteEntered,
		releaseVotes: releaseVotes,
	}
}

func (w *wiredGateway) run(ctx context.Context) (engine.RaceOutcome, error) {
	return w.engine.Run(ctx, engine.Request{
		RequestID:   "request-1",
		Model:       liveModel,
		InputTokens: uint64(len(livePrompt)),
		Stream:      true,
		Params:      w.params,
	}, w.client)
}

func TestOneRequestCommitsItsNonceAcrossTheWholeRequestPath(t *testing.T) {
	upstream := &gatedHost{events: []string{contentEvent}, finishes: true}
	wired := newWiredGateway(t, upstream)

	outcome, err := wired.run(context.Background())

	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if got := wired.escrow.Nonce(); got != 1 {
		t.Fatalf("committed nonces = %d, want 1: the request path commits nothing", got)
	}
	record, committed := wired.escrow.SnapshotState().Inferences[1]
	if !committed {
		t.Fatal("nonce 1 has no inference record, want the one the request's params composed")
	}
	wantHash, err := devshard.CanonicalPromptHash(wired.params.Prompt)
	if err != nil {
		t.Fatalf("CanonicalPromptHash = %v, want nil", err)
	}
	if !bytes.Equal(record.PromptHash, wantHash) {
		t.Errorf("committed prompt hash = %x, want %x: the nonce carries someone else's payload", record.PromptHash, wantHash)
	}
	if record.Model != wired.params.Model || record.MaxTokens != wired.params.MaxTokens {
		t.Errorf("committed record = %s/%d tokens, want %s/%d", record.Model, record.MaxTokens, wired.params.Model, wired.params.MaxTokens)
	}
	if !outcome.Succeeded || outcome.WinnerNonce != 1 {
		t.Errorf("outcome = winner %d succeeded %t, want winner 1 succeeded true", outcome.WinnerNonce, outcome.Succeeded)
	}
	if got := string(wired.client.written); got != contentEvent {
		t.Errorf("bytes reaching the client = %q, want %q", got, contentEvent)
	}
}

// The escrow is retired while the race still holds its nonce, which is the rotation the settlement lookup
// exists for: the vote is the only thing that can resolve the MsgStart that nonce already committed.
// Nothing here counts the request in flight by hand -- that the request path does it is the assertion.
func TestARaceStillPostsTheVotesOfAnEscrowRetiredBeneathIt(t *testing.T) {
	upstream := &gatedHost{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		err:     errHostGone,
	}
	wired := newWiredGateway(t, upstream)
	t.Cleanup(upstream.open)

	served := make(chan struct{})
	go func() {
		defer close(served)
		_, _ = wired.run(context.Background())
	}()
	<-upstream.entered
	if !wired.escrows.IsBusy(liveEscrowID) {
		t.Fatal("IsBusy while the race holds a committed nonce = false: settlement may claim funds beneath it")
	}
	if err := wired.escrows.Retire(liveEscrowID); err != nil {
		t.Fatalf("Retire(%s) = %v, want nil", liveEscrowID, err)
	}
	if _, routable := wired.escrows.RoutableSession(liveEscrowID); routable {
		t.Fatal("the retired escrow is still routable: draining finishes existing work, it takes no new work")
	}
	if !wired.escrows.IsBusy(liveEscrowID) {
		t.Fatal("IsBusy after the retire = false: the escrow drained before its nonce was voted on")
	}
	upstream.open()
	<-served
	<-wired.voteEntered
	if !wired.escrows.IsBusy(liveEscrowID) {
		t.Fatal("IsBusy while the vote is being posted = false: a rotation can close the session it needs")
	}
	wired.releaseVotes()
	wired.engine.Stop()

	posted := postedVotes(wired.metrics.timeoutsRecorded())
	if len(posted) != 1 {
		t.Fatalf("votes posted = %+v, want one for the nonce the retired escrow committed", posted)
	}
	if posted[0].Nonce != 1 || posted[0].Vote == "" {
		t.Errorf("posted vote = %+v, want a vote for nonce 1", posted[0])
	}
	if wired.escrows.IsBusy(liveEscrowID) {
		t.Error("IsBusy once the vote is posted = true: the escrow is never released for settlement")
	}
}

func postedVotes(events []engine.TimeoutEvent) []engine.TimeoutEvent {
	posted := make([]engine.TimeoutEvent, 0, len(events))
	for _, event := range events {
		if event.Vote != "" {
			posted = append(posted, event)
		}
	}
	return posted
}
