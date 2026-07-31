package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"devshard/cmd/gateway/scheduler"
	"devshard/transport"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fakePrepared struct {
	nonce   uint64
	hostIdx int
}

func (p fakePrepared) Nonce() uint64 { return p.nonce }
func (p fakePrepared) HostIdx() int  { return p.hostIdx }

type fakeLimiter struct {
	releases int
}

func (l *fakeLimiter) Release(participant, model string) { l.releases++ }

type fakeClassifier struct {
	perChunk []chunkFacts
	tail     chunkFacts
	seen     int
	releases int
}

func (c *fakeClassifier) Classify(chunk []byte) chunkFacts {
	facts := chunkFacts{}
	if c.seen < len(c.perChunk) {
		facts = c.perChunk[c.seen]
	}
	c.seen++
	return facts
}

func (c *fakeClassifier) Flush() chunkFacts { return c.tail }
func (c *fakeClassifier) Release()          { c.releases++ }

type fakeResponse struct {
	confirmed bool
	bytesRead int64
}

func (r fakeResponse) Confirmed() bool    { return r.confirmed }
func (r fakeResponse) StreamBytes() int64 { return r.bytesRead }

// fakeDispatcher runs a script in place of a host: it writes the scripted chunks into the attempt's
// writer, optionally announcing a receipt first, then returns the scripted reply.
type fakeDispatcher struct {
	receipt  bool
	chunks   []string
	response Response
	err      error
	calls    int
}

func (d *fakeDispatcher) Send(ctx context.Context, nonce scheduler.Prepared, stream io.Writer, onReceipt func()) (Response, error) {
	d.calls++
	if d.receipt {
		onReceipt()
	}
	for _, chunk := range d.chunks {
		if _, err := stream.Write([]byte(chunk)); err != nil {
			return d.response, err
		}
	}
	return d.response, d.err
}

type attemptFixture struct {
	spec       AttemptSpec
	limiter    *fakeLimiter
	classifier *fakeClassifier
	dispatch   *fakeDispatcher
	sink       *bytes.Buffer
	events     chan AttemptEvent
}

func newAttemptFixture(dispatch *fakeDispatcher, classifier *fakeClassifier) *attemptFixture {
	limiter := &fakeLimiter{}
	sink := &bytes.Buffer{}
	events := make(chan AttemptEvent, 64)
	tick := testEpoch
	return &attemptFixture{
		limiter:    limiter,
		classifier: classifier,
		dispatch:   dispatch,
		sink:       sink,
		events:     events,
		spec: AttemptSpec{
			Escrow:      "escrow-1",
			Model:       testModel,
			Participant: testParticipant,
			HostIdx:     3,
			HostLabel:   "host-3",
			Role:        "primary",
			StartReason: "receipt_timeout",
			Nonce:       fakePrepared{nonce: 77, hostIdx: 3},
			Dispatch:    dispatch,
			Limiter:     limiter,
			Classifier:  classifier,
			Sink:        sink,
			Events:      events,
			Now: func() time.Time {
				tick = tick.Add(time.Millisecond)
				return tick
			},
		},
	}
}

func (f *attemptFixture) drain() []AttemptEvent {
	close(f.events)
	collected := make([]AttemptEvent, 0, len(f.events))
	for event := range f.events {
		collected = append(collected, event)
	}
	return collected
}

func kindsOf(events []AttemptEvent) []AttemptEventKind {
	kinds := make([]AttemptEventKind, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	return kinds
}

func doneEvent(t *testing.T, events []AttemptEvent) AttemptEvent {
	t.Helper()
	for _, event := range events {
		if event.Kind == AttemptDone {
			return event
		}
	}
	t.Fatalf("no AttemptDone in %v", kindsOf(events))
	return AttemptEvent{}
}

func contentFacts(source string) chunkFacts {
	return chunkFacts{Content: true, ContentSource: source}
}

func TestRunAttempt_HealthyAttemptEmitsEveryEventInOrder(t *testing.T) {
	t.Parallel()

	dispatch := &fakeDispatcher{
		receipt:  true,
		chunks:   []string{"data: role\n\n", "data: hello\n\n"},
		response: fakeResponse{confirmed: true, bytesRead: 512},
	}
	fixture := newAttemptFixture(dispatch, &fakeClassifier{perChunk: []chunkFacts{{}, contentFacts("delta.content")}})

	// Read concurrently so -race exercises the handover the coordinator actually performs.
	observed := make(chan []AttemptEvent, 1)
	go func() {
		collected := []AttemptEvent{}
		for event := range fixture.events {
			collected = append(collected, event)
		}
		observed <- collected
	}()

	runAttempt(context.Background(), fixture.spec)
	close(fixture.events)
	events := <-observed

	want := []AttemptEventKind{
		AttemptDispatched,
		AttemptReceipt,
		AttemptFirstToken,
		AttemptChunk,
		AttemptContent,
		AttemptChunk,
		AttemptDone,
	}
	if got := kindsOf(events); !equalKinds(got, want) {
		t.Fatalf("event kinds = %v, want %v", got, want)
	}

	for index := 1; index < len(events); index++ {
		if events[index].At.Before(events[index-1].At) {
			t.Fatalf("event %d timestamp %v precedes %v", index, events[index].At, events[index-1].At)
		}
	}

	done := doneEvent(t, events)
	outcome := done.Outcome
	switch {
	case outcome == nil:
		t.Fatal("AttemptDone carried no outcome")
	case outcome.Terminal != TerminalLost:
		t.Fatalf("terminal = %v, want TerminalLost", outcome.Terminal)
	case outcome.Nonce != 77 || outcome.HostIdx != 3 || outcome.Participant != testParticipant:
		t.Fatalf("identity not carried: %+v", outcome)
	case outcome.ContentChunks != 1:
		t.Fatalf("content chunks = %d, want 1", outcome.ContentChunks)
	case !outcome.Confirmed || outcome.ContentSource != "delta.content":
		t.Fatalf("response facts not carried: %+v", outcome)
	case outcome.ReceiptTime.IsZero() || outcome.FirstToken.IsZero() || outcome.Completed.IsZero():
		t.Fatalf("timestamps not stamped: %+v", outcome)
	}
	if fixture.sink.String() != "data: role\n\ndata: hello\n\n" {
		t.Fatalf("sink = %q", fixture.sink.String())
	}
}

func equalKinds(got, want []AttemptEventKind) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func statusError(path string, code int, body string) error {
	return fmt.Errorf("send: %w", &transport.UpstreamStatusError{Path: path, StatusCode: code, Body: body})
}

func TestRunAttempt_TerminalClassification(t *testing.T) {
	t.Parallel()

	const inferencePath = "/v1/devshard/chat/completions"

	testCases := []struct {
		name           string
		cancelled      bool
		dispatch       *fakeDispatcher
		classifier     *fakeClassifier
		want           Terminal
		wantDivergent  bool
		wantEscrowGone bool
	}{
		{
			name:       "content streamed",
			dispatch:   &fakeDispatcher{receipt: true, chunks: []string{"a"}, response: fakeResponse{confirmed: true}},
			classifier: &fakeClassifier{perChunk: []chunkFacts{contentFacts("delta.content")}},
			want:       TerminalLost,
		},
		{
			name:       "receipt then nothing",
			dispatch:   &fakeDispatcher{receipt: true, chunks: []string{"data: [DONE]\n\n"}, response: fakeResponse{confirmed: true}},
			classifier: &fakeClassifier{},
			want:       TerminalEmptyStream,
		},
		{
			name:       "content only in the newline-less tail",
			dispatch:   &fakeDispatcher{receipt: true, chunks: []string{"data: {"}, response: fakeResponse{confirmed: true}},
			classifier: &fakeClassifier{tail: contentFacts("delta.content")},
			want:       TerminalLost,
		},
		{
			name:       "empty stream that burned completion tokens",
			dispatch:   &fakeDispatcher{receipt: true, chunks: []string{"data: [DONE]\n\n"}, response: fakeResponse{confirmed: true}},
			classifier: &fakeClassifier{perChunk: []chunkFacts{{UsageCompletionTokens: 40, TokensBurned: true}}},
			want:       TerminalBurnEmpty,
		},
		{
			name:     "openai error event",
			dispatch: &fakeDispatcher{receipt: true, chunks: []string{"data: err\n\n"}, response: fakeResponse{confirmed: true}},
			classifier: &fakeClassifier{perChunk: []chunkFacts{{
				Error: true, ErrorSource: "sse", ErrorCode: "500", ErrorType: "server_error", ErrorMessage: "boom",
			}}},
			want: TerminalErrorStream,
		},
		{
			name:     "capability refusal another host can serve",
			dispatch: &fakeDispatcher{receipt: true, chunks: []string{"data: err\n\n"}, response: fakeResponse{confirmed: true}},
			classifier: &fakeClassifier{perChunk: []chunkFacts{{
				ErrorSource: "sse", ErrorMessage: "maximum context length is 8192",
				CapabilityRefused: true,
			}}},
			want: TerminalCapabilityRefused,
		},
		{
			name:       "stream completed without a receipt",
			dispatch:   &fakeDispatcher{chunks: []string{"a"}, response: fakeResponse{}},
			classifier: &fakeClassifier{perChunk: []chunkFacts{contentFacts("delta.content")}},
			want:       TerminalNoReceipt,
		},
		{
			name:       "http 429",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 429, "")},
			classifier: &fakeClassifier{},
			want:       TerminalThrottled,
		},
		{
			name:       "http 503",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 503, "")},
			classifier: &fakeClassifier{},
			want:       TerminalUnavailable,
		},
		{
			name:       "http 403",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 403, "")},
			classifier: &fakeClassifier{},
			want:       TerminalForbidden,
		},
		{
			name:       "http 404",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 404, "")},
			classifier: &fakeClassifier{},
			want:       TerminalNotFound,
		},
		{
			name:       "http 401 timestamp drift",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 401, "Timestamp Drift detected")},
			classifier: &fakeClassifier{},
			want:       TerminalTimestampDrift,
		},
		{
			name:       "http 401 without drift",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 401, "bad signature")},
			classifier: &fakeClassifier{},
			want:       TerminalRejected,
		},
		{
			name:       "http 400",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 400, "bad request")},
			classifier: &fakeClassifier{},
			want:       TerminalRejected,
		},
		{
			name:       "http 500",
			dispatch:   &fakeDispatcher{err: statusError(inferencePath, 500, "internal")},
			classifier: &fakeClassifier{},
			want:       TerminalRejected,
		},
		{
			name:           "escrow missing is reported not blamed",
			dispatch:       &fakeDispatcher{err: statusError(inferencePath, 500, "escrow not found")},
			classifier:     &fakeClassifier{},
			want:           TerminalRejected,
			wantEscrowGone: true,
		},
		{
			name:       "failure off the inference path",
			dispatch:   &fakeDispatcher{err: statusError("/v1/devshard/gossip/diffs", 500, "")},
			classifier: &fakeClassifier{},
			want:       TerminalOffPath,
		},
		{
			name:       "sse stream truncated",
			dispatch:   &fakeDispatcher{receipt: true, err: fmt.Errorf("read: %w", transport.ErrSSEStreamTruncated)},
			classifier: &fakeClassifier{},
			want:       TerminalStreamTruncated,
		},
		{
			name:       "unexpected eof",
			dispatch:   &fakeDispatcher{err: fmt.Errorf("read: %w", io.ErrUnexpectedEOF)},
			classifier: &fakeClassifier{},
			want:       TerminalUnexpectedEOF,
		},
		{
			name:       "clean eof",
			dispatch:   &fakeDispatcher{err: fmt.Errorf("read: %w", io.EOF)},
			classifier: &fakeClassifier{},
			want:       TerminalUnexpectedEOF,
		},
		{
			name:       "dial failure",
			dispatch:   &fakeDispatcher{err: errors.New("dial tcp: connection refused")},
			classifier: &fakeClassifier{},
			want:       TerminalDialFailure,
		},
		{
			name:       "cut by the coordinator",
			cancelled:  true,
			dispatch:   &fakeDispatcher{err: context.Canceled},
			classifier: &fakeClassifier{},
			want:       TerminalClientCancelled,
		},
		{
			name:          "post state root divergence",
			dispatch:      &fakeDispatcher{err: fmt.Errorf("apply: %w", ErrStateRootDivergence)},
			classifier:    &fakeClassifier{},
			want:          TerminalDialFailure,
			wantDivergent: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			fixture := newAttemptFixture(testCase.dispatch, testCase.classifier)
			ctx := context.Background()
			if testCase.cancelled {
				cancellable, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancellable
			}

			runAttempt(ctx, fixture.spec)
			done := doneEvent(t, fixture.drain())

			if done.Outcome.Terminal != testCase.want {
				t.Fatalf("terminal = %v, want %v", done.Outcome.Terminal, testCase.want)
			}
			if done.Outcome.StateDivergent != testCase.wantDivergent {
				t.Fatalf("state divergent = %v, want %v", done.Outcome.StateDivergent, testCase.wantDivergent)
			}
			if done.Lifecycle.EscrowMissing != testCase.wantEscrowGone {
				t.Fatalf("escrow missing = %v, want %v", done.Lifecycle.EscrowMissing, testCase.wantEscrowGone)
			}
		})
	}
}

// The slot the scheduler took when it committed the nonce comes back exactly once, whatever ends the
// attempt -- an attempt that never got as far as the host has one to give back just the same.
func TestRunAttempt_ReleasesTheHostSlotOnEveryExitPath(t *testing.T) {
	t.Parallel()

	failingSink := &fakeDispatcher{
		receipt:  true,
		chunks:   []string{"a", "b"},
		response: fakeResponse{confirmed: true},
		err:      errors.New("mid-stream reset"),
	}

	testCases := []struct {
		name      string
		cancelled bool
		dispatch  *fakeDispatcher
	}{
		{
			name:     "dispatch failed outright",
			dispatch: &fakeDispatcher{err: errors.New("dial tcp: connection refused")},
		},
		{
			name:     "stream failed after content",
			dispatch: failingSink,
		},
		{
			name:      "context cancelled",
			cancelled: true,
			dispatch:  &fakeDispatcher{err: context.Canceled},
		},
		{
			name:     "clean completion",
			dispatch: &fakeDispatcher{receipt: true, chunks: []string{"a"}, response: fakeResponse{confirmed: true}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			classifier := &fakeClassifier{perChunk: []chunkFacts{contentFacts("delta.content")}}
			fixture := newAttemptFixture(testCase.dispatch, classifier)
			ctx := context.Background()
			if testCase.cancelled {
				cancellable, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancellable
			}

			runAttempt(ctx, fixture.spec)
			fixture.drain()

			if fixture.limiter.releases != 1 {
				t.Fatalf("limiter releases = %d, want 1", fixture.limiter.releases)
			}
			if classifier.releases != 1 {
				t.Fatalf("classifier releases = %d, want 1", classifier.releases)
			}
		})
	}
}
