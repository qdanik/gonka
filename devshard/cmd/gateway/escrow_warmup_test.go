package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/internal/leakcheck"
	"devshard/cmd/gateway/registry"
	"devshard/host"
	"devshard/user"
)

type recordedRace struct {
	escrowID string
	attempts []accounting.Attempt
}

type spyLedger struct {
	races  []recordedRace
	reject error
}

func (s *spyLedger) RecordRace(escrowID string, attempts []accounting.Attempt) error {
	s.races = append(s.races, recordedRace{escrowID: escrowID, attempts: attempts})
	return s.reject
}

type stubEscrows struct {
	session  registry.EscrowSession
	live     bool
	released int
}

func (s *stubEscrows) Acquire(string) (registry.EscrowSession, func(), bool) {
	if !s.live {
		return nil, nil, false
	}
	return s.session, func() { s.released++ }, true
}

func warmupClock() func() time.Time {
	moment := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	return func() time.Time { return moment }
}

func TestWarmupIsSkippedWhenTheOperatorTurnedItOff(t *testing.T) {
	holder := config.NewHolder(&config.Config{})

	if warmup := newEscrowWarmup(holder, nil, warmupClock()); warmup != nil {
		t.Errorf("newEscrowWarmup() = %v with warming off, want nil so nothing observes publications", warmup)
	}
}

func TestWarmupIsBuiltWhenWarmingIsOn(t *testing.T) {
	holder := config.NewHolder(&config.Config{Scheduler: config.Scheduler{WarmNewEscrows: true}})

	if warmup := newEscrowWarmup(holder, nil, warmupClock()); warmup == nil {
		t.Error("newEscrowWarmup() = nil with warming on, want a warmup")
	}
}

func TestWarmupKeepsATypedNilLedgerOutOfItsInterface(t *testing.T) {
	holder := config.NewHolder(&config.Config{Scheduler: config.Scheduler{WarmNewEscrows: true}})

	warmup := newEscrowWarmup(holder, nil, warmupClock())

	if warmup.ledger != nil {
		t.Error("ledger is non-nil for a nil *Book: a typed nil there panics on the first record")
	}
}

func TestWarmupSkipsAnEscrowThatIsAlreadyGone(t *testing.T) {
	escrows := &stubEscrows{live: false}
	ledger := &spyLedger{}
	warmup := &escrowWarmup{escrows: escrows, ledger: ledger, now: warmupClock()}

	warmup.warm("escrow-1", "test-model")

	if len(ledger.races) != 0 {
		t.Errorf("recorded %d races for a retired escrow, want 0", len(ledger.races))
	}
}

func TestWarmupSettlesItsNonceAsWorkNobodyUsed(t *testing.T) {
	ledger := &spyLedger{}
	warmup := &escrowWarmup{ledger: ledger, now: warmupClock()}

	warmup.record("escrow-1", 7, true, nil)

	if len(ledger.races) != 1 {
		t.Fatalf("recorded %d races, want 1", len(ledger.races))
	}
	attempt := ledger.races[0].attempts[0]
	switch {
	case attempt.Nonce != 7:
		t.Errorf("nonce = %d, want 7", attempt.Nonce)
	case !attempt.Sent:
		t.Error("Sent = false, want true: the gateway dispatched this nonce")
	case !attempt.Finished:
		t.Error("Finished = false, want true: the host answered")
	case attempt.Usage != accounting.UsageLoser:
		t.Errorf("Usage = %q, want %q: nobody consumed a warmup answer", attempt.Usage, accounting.UsageLoser)
	}
}

func TestARefusedProbeIsNotSettledAsFinished(t *testing.T) {
	ledger := &spyLedger{}
	warmup := &escrowWarmup{ledger: ledger, now: warmupClock()}

	warmup.record("escrow-1", 7, false, errors.New("host refused"))

	attempt := ledger.races[0].attempts[0]
	switch {
	case !attempt.Sent:
		t.Error("Sent = false, want true: the nonce is spent either way")
	case attempt.Finished:
		t.Error("Finished = true, want false: the host delivered nothing")
	case attempt.Acknowledged:
		t.Error("Acknowledged = true, want false: no receipt came back")
	}
}

// A nil *Book assigned straight to the interface field is not a nil interface, so record would call
// RecordRace on it and dereference nothing.
func TestWarmupWithoutALedgerRecordsNothingAndDoesNotPanic(t *testing.T) {
	holder := config.NewHolder(&config.Config{Scheduler: config.Scheduler{WarmNewEscrows: true}})

	warmup := newEscrowWarmup(holder, nil, warmupClock())
	warmup.record("escrow-1", 7, true, nil)

	if warmup.ledger != nil {
		t.Fatalf("ledger = %#v, want a nil interface: record dereferences a typed nil instead of skipping", warmup.ledger)
	}
}

func TestABurnedNonceAndAWarmupProbeAgreeOnTheirTokenFloor(t *testing.T) {
	var body struct {
		MaxTokens uint64 `json:"max_tokens"`
	}
	if err := json.Unmarshal(warmupPrompt, &body); err != nil {
		t.Fatalf("parsing warmupPrompt: %v", err)
	}
	if body.MaxTokens != warmupMaxTokens {
		t.Errorf("warmupPrompt max_tokens = %d, reservation = %d: a host refuses a probe declaring less than it reserved",
			body.MaxTokens, warmupMaxTokens)
	}
}

// warm spawns a watchdog bound to a twenty-minute budget; without the deferred cancel it outlives the
// escrow by that whole budget, once per publication.
func TestEscrowPublishedLeavesNoGoroutineBehind(t *testing.T) {
	defer leakcheck.VerifyNoneStarted(t)()

	caughtUp := make(chan struct{})
	warmup := &escrowWarmup{
		escrows: &stubEscrows{session: stubSession{}, live: true},
		ledger:  &spyLedger{},
		probe: func(_ context.Context, _ registry.EscrowSession, _ user.InferenceParams, nonceCommitted func()) (uint64, bool, error) {
			nonceCommitted()
			return 1, true, nil
		},
		catchUp: func(context.Context, registry.EscrowSession) error {
			close(caughtUp)
			return nil
		},
		stop: make(chan struct{}),
		now:  warmupClock(),
	}

	warmup.EscrowPublished("escrow-1", "model-a")

	select {
	case <-caughtUp:
	case <-time.After(5 * time.Second):
		t.Fatal("warm never reached its catch-up: the probe path did not run")
	}
}

func TestTheWarmupStopsWithTheGateway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	warmup := &escrowWarmup{now: warmupClock()}

	warmup.start(ctx)
	cancel()

	select {
	case <-warmup.stop:
	case <-time.After(time.Second):
		t.Error("the warmup did not see the gateway stop: it would keep spending nonces after shutdown")
	}
}

// stubSession implements only what the warmup reads; anything else panics, which is the signal that the
// warmup grew a dependency this test does not describe.
type stubSession struct {
	registry.EscrowSession
	nonce uint64
}

func (s stubSession) Nonce() uint64 { return s.nonce }

func newWarmupUnderTest(session registry.EscrowSession, probeErr error) (*escrowWarmup, *spyLedger, *int) {
	caughtUp := 0
	ledger := &spyLedger{}
	warmup := &escrowWarmup{
		escrows: &stubEscrows{session: session, live: true},
		ledger:  ledger,
		probe: func(_ context.Context, _ registry.EscrowSession, _ user.InferenceParams, nonceCommitted func()) (uint64, bool, error) {
			nonceCommitted()
			return 1, probeErr == nil, probeErr
		},
		catchUp: func(context.Context, registry.EscrowSession) error {
			caughtUp++
			return nil
		},
		now: warmupClock(),
	}
	return warmup, ledger, &caughtUp
}

func TestEveryHostLearnsTheEscrowAfterTheProbe(t *testing.T) {
	warmup, _, caughtUp := newWarmupUnderTest(stubSession{}, nil)

	warmup.warm("escrow-1", "test-model")

	if *caughtUp != 1 {
		t.Errorf("catch-up ran %d times, want 1: without it a host the dispatch missed never holds the escrow", *caughtUp)
	}
}

func TestTheGroupLearnsTheEscrowEvenWhenTheProbeWasRefused(t *testing.T) {
	warmup, _, caughtUp := newWarmupUnderTest(stubSession{}, errors.New("host refused"))

	warmup.warm("escrow-1", "test-model")

	if *caughtUp != 1 {
		t.Error("catch-up was skipped after a refused probe: the diff is persisted before the send, so there is state to replay")
	}
}

func TestAnEscrowThatAlreadyServedIsNeitherProbedNorCaughtUp(t *testing.T) {
	warmup, ledger, caughtUp := newWarmupUnderTest(stubSession{nonce: 917}, nil)

	warmup.warm("escrow-1", "test-model")

	if len(ledger.races) != 0 || *caughtUp != 0 {
		t.Errorf("races = %d, catch-ups = %d, want 0 and 0: its hosts already hold the escrow", len(ledger.races), *caughtUp)
	}
}

func TestOnlyAnExecutorReceiptCountsAsAnAnsweredProbe(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		response *host.HostResponse
		want     bool
	}{
		{name: "executor receipt", response: &host.HostResponse{Receipt: []byte("sig")}, want: true},
		{name: "state signature only", response: &host.HostResponse{StateSig: []byte("sig")}, want: false},
		{name: "nothing", response: &host.HostResponse{}, want: false},
		{name: "no response", response: nil, want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := executorAcknowledged(testCase.response); got != testCase.want {
				t.Errorf("executorAcknowledged() = %t, want %t: every host signs state, only the executor answers",
					got, testCase.want)
			}
		})
	}
}

// A host cannot sign or vote until it holds the session, and the first request lands long before the
// probe's answer, so the catch-up must not wait for it.
func TestTheGroupIsTaughtWhileTheProbeIsStillStreaming(t *testing.T) {
	caughtUp := make(chan struct{})
	sawCatchUp := make(chan struct{})
	warmup := &escrowWarmup{
		escrows: &stubEscrows{session: stubSession{}, live: true},
		ledger:  &spyLedger{},
		probe: func(_ context.Context, _ registry.EscrowSession, _ user.InferenceParams, nonceCommitted func()) (uint64, bool, error) {
			nonceCommitted()
			select {
			case <-caughtUp:
				close(sawCatchUp)
			case <-time.After(2 * time.Second):
			}
			return 1, true, nil
		},
		catchUp: func(context.Context, registry.EscrowSession) error {
			close(caughtUp)
			return nil
		},
		now: warmupClock(),
	}

	warmup.warm("escrow-1", "test-model")

	select {
	case <-sawCatchUp:
	default:
		t.Error("the catch-up waited for the probe's answer: the group holds nothing for that whole inference")
	}
}

// The probe is answered for this gateway, not for a client, and the ledger can only tell the two apart
// if the nonce says so. Without the mark every rotation reads as a race the host lost.
func TestTheWarmupNonceIsSettledAsAProbe(t *testing.T) {
	warmup, ledger, _ := newWarmupUnderTest(stubSession{}, nil)

	warmup.warm("escrow-1", "test-model")

	if len(ledger.races) != 1 || len(ledger.races[0].attempts) != 1 {
		t.Fatalf("recorded %+v, want one attempt for the one nonce the warmup spends", ledger.races)
	}
	if got := ledger.races[0].attempts[0].Terminal; got != accounting.TerminalWarmupProbe {
		t.Errorf("terminal = %q, want %q", got, accounting.TerminalWarmupProbe)
	}
}
