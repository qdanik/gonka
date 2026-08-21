package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"devshard/cmd/gateway/config"
)

type traceKey struct{}

type flushRecorder struct {
	written  bytes.Buffer
	flushes  int
	writeErr error
}

func (r *flushRecorder) Write(chunk []byte) (int, error) {
	if r.writeErr != nil {
		return 0, r.writeErr
	}
	return r.written.Write(chunk)
}

func (r *flushRecorder) Flush() { r.flushes++ }

func TestDrainTimeoutFromConfig(t *testing.T) {
	timeout := DrainTimeoutFromConfig(config.Stream{DrainTimeoutSeconds: 2_400})
	if timeout != 40*time.Minute {
		t.Fatalf("timeout = %v, want 40m", timeout)
	}
	if unset := newDrain(context.Background(), 0); unset.timeout != defaultDrainTimeout {
		t.Fatalf("unset timeout = %v, want %v", unset.timeout, defaultDrainTimeout)
	}
}

func TestDrainRaceContextOutlivesTheClientKeepingItsValues(t *testing.T) {
	clientCtx, cancel := context.WithCancel(context.WithValue(context.Background(), traceKey{}, "request-1"))
	contexts := newDrain(clientCtx, time.Minute)
	cancel()

	select {
	case <-contexts.clientGone():
	default:
		t.Fatal("client cancellation is not observable")
	}
	if err := contexts.race.Err(); err != nil {
		t.Fatalf("race context error = %v, want the race to outlive the client", err)
	}
	if trace := contexts.race.Value(traceKey{}); trace != "request-1" {
		t.Fatalf("race context value = %v, want the client's", trace)
	}
	if !errors.Is(contexts.clientErr(), context.Canceled) {
		t.Fatalf("client error = %v, want context.Canceled", contexts.clientErr())
	}
}

func TestDrainDeadlineArmsOnlyOnClientDone(t *testing.T) {
	contexts := newDrain(context.Background(), time.Minute)
	if armed := contexts.deadline(time.Time{}); !armed.IsZero() {
		t.Fatalf("deadline while the client is connected = %v, want none", armed)
	}
	if armed := contexts.deadline(testEpoch); !armed.Equal(testEpoch.Add(time.Minute)) {
		t.Fatalf("deadline = %v, want %v", armed, testEpoch.Add(time.Minute))
	}
}

func TestClientStreamDropsBytesOnceTheClientIsGoneAndKeepsFlushing(t *testing.T) {
	clientCtx, cancel := context.WithCancel(context.Background())
	recorder := &flushRecorder{}
	gated := newDrain(clientCtx, time.Minute).gate(recorder)

	if _, err := gated.Write([]byte("connected")); err != nil {
		t.Fatalf("write while connected: %v", err)
	}
	gated.(interface{ Flush() }).Flush()
	cancel()

	written, err := gated.Write([]byte("departed"))
	if err != nil {
		t.Fatalf("write after departure = %v, want the host's write to succeed", err)
	}
	if written != len("departed") {
		t.Fatalf("written = %d, want %d", written, len("departed"))
	}
	if got := recorder.written.String(); got != "connected" {
		t.Fatalf("client received %q, want only what it was still there for", got)
	}
	if recorder.flushes != 1 {
		t.Fatalf("flushes = %d, want the gate to pass Flush through", recorder.flushes)
	}
	if gate := newDrain(clientCtx, time.Minute).gate(nil); gate != nil {
		t.Fatalf("gate over no client = %v, want none", gate)
	}
}

// A write error would end the attempt carrying it, so only a connected client's error propagates.
func TestClientStreamPropagatesAConnectedClientsWriteError(t *testing.T) {
	failure := errors.New("client write failed")
	recorder := &flushRecorder{writeErr: failure}
	gated := newDrain(context.Background(), time.Minute).gate(recorder)

	if _, err := gated.Write([]byte("chunk")); !errors.Is(err, failure) {
		t.Fatalf("error = %v, want %v", err, failure)
	}
}

func TestRunRaceCompletesTheWinnerAfterTheClientDisconnects(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.deps.DrainTimeout = time.Hour
	streaming := make(chan uint64, 1)
	resume := make(chan struct{})
	fixture.host(200, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(200), "data: tail\n\n"},
		streaming: streaming,
		resume:    resume,
		confirmed: true,
		finished:  true,
	})

	clientCtx, disconnect := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() {
		_, err := fixture.run(clientCtx)
		returned <- err
	}()

	<-streaming
	disconnect()
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("runRace error = %v, want context.Canceled", err)
	}
	close(resume)

	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if len(reported.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(reported.Attempts))
	}
	winner := reported.Attempts[0]
	if reported.WinnerNonce != 200 || winner.Terminal != TerminalWon || !reported.Succeeded {
		t.Fatalf("winner %d terminal %v succeeded %v, want 200/won/true", reported.WinnerNonce, winner.Terminal, reported.Succeeded)
	}
	if !winner.NonceFinished || !winner.Confirmed {
		t.Fatalf("nonce finished = %v confirmed = %v, want both", winner.NonceFinished, winner.Confirmed)
	}
	forwarded := string(fixture.client.forwarded())
	if !strings.Contains(forwarded, contentChunk(200)) {
		t.Fatalf("client received %q, want what it was still there for", forwarded)
	}
	if strings.Contains(forwarded, "tail") {
		t.Fatalf("client received %q after leaving", forwarded)
	}
	assertOneSlotPerAttempt(t, fixture, len(reported.Attempts))
}

func TestRunRaceAnswersACrownClaimRaisedAfterTheClientLeft(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.deps.DrainTimeout = time.Hour
	streaming := make(chan uint64, 1)
	resume := make(chan struct{})
	fixture.host(210, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{"data: role\n\n", contentChunk(210)},
		streaming: streaming,
		resume:    resume,
		confirmed: true,
		finished:  true,
	})

	clientCtx, disconnect := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() {
		_, err := fixture.run(clientCtx)
		returned <- err
	}()

	<-streaming
	disconnect()
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("runRace error = %v, want context.Canceled", err)
	}
	close(resume)

	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if reported.WinnerNonce != 210 || reported.Attempts[0].Terminal != TerminalWon {
		t.Fatalf("winner %d terminal %v, want 210/won: the claim went unanswered", reported.WinnerNonce, reported.Attempts[0].Terminal)
	}
	if forwarded := fixture.client.forwarded(); len(forwarded) != 0 {
		t.Fatalf("client received %q after leaving", forwarded)
	}
	assertOneSlotPerAttempt(t, fixture, len(reported.Attempts))
}

func TestRunRaceDrainTimeoutBoundsAHostStreamingPastTheClient(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.deps.DrainTimeout = time.Millisecond
	fixture.clock.step = time.Second
	streaming := make(chan uint64, 1)
	// resume is never closed: only the drain deadline can end this attempt.
	fixture.host(220, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(220)},
		streaming: streaming,
		resume:    make(chan struct{}),
	})

	clientCtx, disconnect := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() {
		_, err := fixture.run(clientCtx)
		returned <- err
	}()

	<-streaming
	disconnect()
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("runRace error = %v, want context.Canceled", err)
	}

	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if reported.WinnerNonce != 220 {
		t.Fatalf("winner nonce = %d, want 220", reported.WinnerNonce)
	}
	if terminal := reported.Attempts[0].Terminal; terminal != TerminalClientCancelled {
		t.Fatalf("terminal = %v, want client cancelled", terminal)
	}
	assertOneSlotPerAttempt(t, fixture, len(reported.Attempts))
}

func assertOneSlotPerAttempt(t *testing.T, fixture *raceFixture, attempts int) {
	t.Helper()
	for range attempts {
		select {
		case <-fixture.limiter.releases:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %d host-slot releases", attempts)
		}
	}
	if extra := len(fixture.limiter.releases); extra != 0 {
		t.Fatalf("host-slot releases = %d more than attempts, want one each", extra)
	}
	if acquired, released := fixture.perf.counts(); acquired != attempts || released != attempts {
		t.Fatalf("perf bracket = %d/%d, want %d/%d", acquired, released, attempts, attempts)
	}
}
