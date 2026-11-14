package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"devshard/cmd/gateway/engine"
)

const (
	firstEvent  = "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n"
	secondEvent = "data: {\"choices\":[{\"delta\":{\"content\":\"two\"},\"finish_reason\":\"stop\"}]}\n\n"
	doneEvent   = "data: [DONE]\n\n"
	fullStream  = firstEvent + secondEvent + doneEvent
	chatPath    = "/v1/chat/completions"
)

var replyChunks = []string{firstEvent, secondEvent, doneEvent}

type spyStreamEvents struct {
	mu       sync.Mutex
	outcomes []string
	resumes  []string
	attached chan struct{}
}

func newSpyStreamEvents() *spyStreamEvents {
	return &spyStreamEvents{attached: make(chan struct{}, 8)}
}

func (s *spyStreamEvents) OrphanOutcome(model, result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcomes = append(s.outcomes, model+"/"+result)
	if result == orphanAdopted {
		s.attached <- struct{}{}
	}
}

func (s *spyStreamEvents) Resume(model, result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resumes = append(s.resumes, model+"/"+result)
	if result == resumeSourceLive {
		s.attached <- struct{}{}
	}
}

func (s *spyStreamEvents) seenOutcomes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.outcomes...)
}

func (s *spyStreamEvents) seenResumes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.resumes...)
}

type departingEngine struct {
	runs        atomic.Int64
	chunks      []string
	leaveAfter  int
	outcome     engine.RaceOutcome
	reportFirst bool
	skipOutcome bool

	delivered chan struct{}
	proceed   chan struct{}
	finished  chan struct{}
	announce  sync.Once
}

func newDepartingEngine(leaveAfter int, outcome engine.RaceOutcome) *departingEngine {
	return &departingEngine{
		chunks: replyChunks, leaveAfter: leaveAfter, outcome: outcome,
		delivered: make(chan struct{}), proceed: make(chan struct{}), finished: make(chan struct{}),
	}
}

func (e *departingEngine) Run(ctx context.Context, request engine.Request, client io.Writer) (engine.RaceOutcome, error) {
	e.runs.Add(1)
	request.OnEscrow("7")
	for _, chunk := range e.chunks[:e.leaveAfter] {
		_, _ = client.Write([]byte(chunk))
	}
	e.announce.Do(func() { close(e.delivered) })
	<-ctx.Done()
	background := func() {
		<-e.proceed
		sink := client.(engine.DepartedSink)
		for _, chunk := range e.chunks[e.leaveAfter:] {
			sink.WriteDeparted([]byte(chunk))
		}
		if !e.skipOutcome {
			request.OnOutcome(e.outcome)
		}
		request.OnFinished()
		close(e.finished)
	}
	if e.reportFirst {
		close(e.proceed)
		background()
	} else {
		go background()
	}
	return engine.RaceOutcome{EscrowID: "7"}, ctx.Err()
}

type inlineEngine struct {
	runs     atomic.Int64
	arrived  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func newInlineEngine() *inlineEngine {
	return &inlineEngine{arrived: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
}

func (e *inlineEngine) Run(_ context.Context, request engine.Request, client io.Writer) (engine.RaceOutcome, error) {
	e.runs.Add(1)
	request.OnEscrow("7")
	_, _ = client.Write([]byte(replyChunks[0]))
	close(e.arrived)
	<-e.release
	for _, chunk := range replyChunks[1:] {
		_, _ = client.Write([]byte(chunk))
	}
	outcome := engine.RaceOutcome{EscrowID: "7", Succeeded: true}
	request.OnOutcome(outcome)
	request.OnFinished()
	close(e.finished)
	return outcome, nil
}

func resumeHeaders(caller, cursor string) map[string]string {
	headers := callerHeaders(caller)
	headers[ResumeHeader] = cursor
	return headers
}

func countingRequestIDs(live *harness) {
	var issued atomic.Int64
	live.server.requestIDs = func() string { return "request-" + strconv.FormatInt(issued.Add(1), 10) }
}

type resumeFixture struct {
	live    *harness
	engine  *departingEngine
	results *spyStreamEvents
}

func newResumeFixture(t *testing.T, leaveAfter int, outcome engine.RaceOutcome, adjust ...func(*departingEngine)) *resumeFixture {
	t.Helper()
	live := newHarness(t)
	countingRequestIDs(live)
	fixture := &resumeFixture{live: live, engine: newDepartingEngine(leaveAfter, outcome), results: newSpyStreamEvents()}
	for _, change := range adjust {
		change(fixture.engine)
	}
	live.server.inference = fixture.engine
	live.server.streamEvents = fixture.results
	return fixture
}

func (f *resumeFixture) startAndLeave(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	ctx, leave := context.WithCancel(context.Background())
	first := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		f.live.requestIntoWithContext(t, ctx, first, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))
	}()
	<-f.engine.delivered
	leave()
	<-returned
	return first
}

func (f *resumeFixture) finishRace() {
	close(f.engine.proceed)
	<-f.engine.finished
}

func succeeded() engine.RaceOutcome { return engine.RaceOutcome{EscrowID: "7", Succeeded: true} }

// Test flow:
//  1. Start a streamed request whose client leaves after the first event, while its race goes on.
//  2. Resume the stream with `request-1/1` before the race ends, and let the race finish.
//  3. Assert the resumed reply is the rest of the stream, labelled live, and no second race ran.
//  4. Assert the first reply carried the stream id and only the first event.
func TestAClientThatLeftMidStreamResumesTheRestOfTheSameReplyWhileItsRaceRuns(t *testing.T) {
	fixture := newResumeFixture(t, 1, succeeded())
	first := fixture.startAndLeave(t)

	resumed := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		defer close(served)
		fixture.live.requestInto(t, resumed, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/1"))
	}()
	awaitValue(t, fixture.results.attached, "the resume to take the log over")
	fixture.finishRace()
	<-served

	if got := first.Header().Get(StreamIDHeader); got != "request-1" {
		t.Fatalf("first reply stream id = %q, want request-1", got)
	}
	if got := first.Body.String(); got != firstEvent {
		t.Fatalf("first reply = %q, want only the event it received", got)
	}
	if got := resumed.Body.String(); got != secondEvent+doneEvent {
		t.Fatalf("resumed reply = %q, want %q", got, secondEvent+doneEvent)
	}
	header := resumed.Header()
	if header.Get(ResumeFromHeader) != "1" || header.Get(ResumeSourceHeader) != resumeSourceLive || header.Get(StreamIDHeader) != "request-1" {
		t.Fatalf("resumed headers = %v, want resume-from 1, source live, stream id request-1", header)
	}
	if header.Get(RequestIDHeader) != "request-2" || header.Get(EscrowHeader) != "7" {
		t.Fatalf("resumed request id %q escrow %q, want a new request id and the race's escrow", header.Get(RequestIDHeader), header.Get(EscrowHeader))
	}
	if got := fixture.engine.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1: a resume never starts one", got)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves after the first event and let the race finish with nobody reading.
//  2. Resume from every position from 0 through the event count.
//  3. Assert each reply is the matching suffix of the continuous stream, served from the cache, with no second race.
func TestEveryResumePositionOfAFinishedOrphanIsTheSuffixOfTheStream(t *testing.T) {
	fixture := newResumeFixture(t, 1, succeeded())
	fixture.startAndLeave(t)
	fixture.finishRace()

	for position := range len(replyChunks) + 1 {
		resumed := fixture.live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/"+strconv.Itoa(position)))

		want := strings.Join(replyChunks[position:], "")
		if resumed.Code != http.StatusOK || resumed.Body.String() != want {
			t.Fatalf("resume from %d = %d %q, want 200 %q", position, resumed.Code, resumed.Body.String(), want)
		}
		if got := resumed.Header().Get(ResumeSourceHeader); got != resumeSourceCache {
			t.Fatalf("resume from %d source = %q, want cache", position, got)
		}
	}
	if got := fixture.engine.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1", got)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves after the first event, and let the race finish.
//  2. Send the identical request again without a resume header.
//  3. Assert it is answered from the cache with the whole stream and the original stream id, and no second race ran.
func TestAnIdenticalRequestAfterAnOrphanFinishedIsServedFromTheCache(t *testing.T) {
	fixture := newResumeFixture(t, 1, succeeded())
	fixture.startAndLeave(t)
	fixture.finishRace()

	again := fixture.live.request(t, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))

	if again.Body.String() != fullStream || again.Header().Get(StreamIDHeader) != "request-1" {
		t.Fatalf("repeat = %q stream id %q, want the whole stream under the original id", again.Body.String(), again.Header().Get(StreamIDHeader))
	}
	if got := fixture.engine.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1", got)
	}
	if got := fixture.results.seenOutcomes(); len(got) != 1 || got[0] != "qwen/stored" {
		t.Fatalf("orphan outcomes = %v, want one stored", got)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves after the first event while its race runs.
//  2. Send the identical request without a resume header, then let the race finish.
//  3. Assert the repeat adopted the orphan and received the whole stream from event 0, with no second race.
func TestAnIdenticalRequestAdoptsAnOrphanedStreamFromTheStart(t *testing.T) {
	fixture := newResumeFixture(t, 1, succeeded())
	fixture.startAndLeave(t)

	adopted := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		defer close(served)
		fixture.live.requestInto(t, adopted, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))
	}()
	awaitValue(t, fixture.results.attached, "the resume to take the log over")
	fixture.finishRace()
	<-served

	if adopted.Body.String() != fullStream {
		t.Fatalf("adopted reply = %q, want %q", adopted.Body.String(), fullStream)
	}
	if got := fixture.engine.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1", got)
	}
	if got := fixture.results.seenOutcomes(); len(got) != 2 || got[0] != "qwen/adopted" || got[1] != "qwen/stored" {
		t.Fatalf("orphan outcomes = %v, want adopted then stored", got)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves before any event, so the orphan is empty, and let the race finish.
//  2. Resume from event 0.
//  3. Assert the reply is the whole stream.
func TestAClientThatLeftBeforeTheFirstEventResumesTheWholeReply(t *testing.T) {
	fixture := newResumeFixture(t, 0, succeeded())
	first := fixture.startAndLeave(t)
	fixture.finishRace()

	resumed := fixture.live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/0"))

	if first.Body.Len() != 0 {
		t.Fatalf("first reply = %q, want nothing", first.Body.String())
	}
	if resumed.Body.String() != fullStream {
		t.Fatalf("resumed reply = %q, want %q", resumed.Body.String(), fullStream)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves after the first event, and report the race's outcome before the handler returns.
//  2. Resume from event 1.
//  3. Assert the orphan was settled by the handler and the reply is the rest of the stream.
func TestAnOutcomeReportedBeforeTheHandlerReturnsStillStoresTheOrphan(t *testing.T) {
	fixture := newResumeFixture(t, 1, succeeded(), func(e *departingEngine) { e.reportFirst = true })
	fixture.startAndLeave(t)
	<-fixture.engine.finished

	resumed := fixture.live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/1"))

	if resumed.Body.String() != secondEvent+doneEvent {
		t.Fatalf("resumed reply = %q, want %q", resumed.Body.String(), secondEvent+doneEvent)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves after the first event, and let the race fail.
//  2. Assert nothing was stored, a resume is 410 and the identical request runs a new race.
func TestAFailedOrphanedRaceIsNotStoredAndAResumeIsGone(t *testing.T) {
	fixture := newResumeFixture(t, 1, engine.RaceOutcome{EscrowID: "7"})
	fixture.startAndLeave(t)
	fixture.finishRace()

	resumed := fixture.live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/1"))

	if resumed.Code != http.StatusGone {
		t.Fatalf("resume of a failed race = %d, want 410", resumed.Code)
	}
	if got := fixture.results.seenOutcomes(); len(got) != 1 || got[0] != "qwen/failed" {
		t.Fatalf("orphan outcomes = %v, want one failed", got)
	}
	if left := len(fixture.live.server.streams.logs); left != 0 {
		t.Fatalf("%d keys still hold logs after the race failed", left)
	}
}

// Test flow:
//  1. Start a streamed request whose client leaves after the first event, and finish the race without ever reporting an outcome.
//  2. Assert the orphan was dropped, counted as failed, and a resume is 410.
func TestAnOrphanWhoseRaceFinishedWithoutAnOutcomeIsDropped(t *testing.T) {
	fixture := newResumeFixture(t, 1, succeeded(), func(e *departingEngine) { e.skipOutcome = true })
	fixture.startAndLeave(t)
	fixture.finishRace()

	resumed := fixture.live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/1"))

	if resumed.Code != http.StatusGone {
		t.Fatalf("resume = %d, want 410", resumed.Code)
	}
	if got := fixture.results.seenOutcomes(); len(got) != 1 || got[0] != "qwen/failed" {
		t.Fatalf("orphan outcomes = %v, want one failed", got)
	}
}

// Test flow:
//  1. Send a streamed request that completes normally.
//  2. Resume it from the cache with a cursor in front of the end.
//  3. Assert the reply is the suffix, and that a cursor past the event count is a 400.
func TestAFinishedReplyResumesFromTheCacheAndRefusesACursorPastItsEnd(t *testing.T) {
	live := newHarness(t)
	countingRequestIDs(live)
	live.inference.chunks = replyChunks
	live.request(t, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))

	tail := live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/2"))
	past := live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/4"))

	if tail.Body.String() != doneEvent || tail.Header().Get(ResumeSourceHeader) != resumeSourceCache {
		t.Fatalf("resume from 2 = %q source %q, want the terminator from the cache", tail.Body.String(), tail.Header().Get(ResumeSourceHeader))
	}
	if past.Code != http.StatusBadRequest || !strings.Contains(past.Body.String(), errorCodeInvalidResumeCursor) {
		t.Fatalf("resume from 4 = %d %q, want 400 invalid_resume_cursor", past.Code, past.Body.String())
	}
	if got := live.inference.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1", got)
	}
}

// Test flow:
//  1. Send a streamed request that completes, then send resumes with headers that do not parse.
//  2. Assert each is a 400 with `invalid_resume_cursor`, counted as an invalid cursor, and no race ran for it.
func TestAResumeHeaderThatDoesNotParseIsRefusedBeforeAnyNonceIsSpent(t *testing.T) {
	for _, cursor := range []string{"garbage", "request-1/", "/3", "request-1/-1", "request-1/x", "request-1/99999999999", "request-1/ 1"} {
		t.Run(cursor, func(t *testing.T) {
			live := newHarness(t)
			results := newSpyStreamEvents()
			live.server.streamEvents = results

			refused := live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", cursor))

			if refused.Code != http.StatusBadRequest || !strings.Contains(refused.Body.String(), errorCodeInvalidResumeCursor) {
				t.Fatalf("resume %q = %d %q, want 400 invalid_resume_cursor", cursor, refused.Code, refused.Body.String())
			}
			if live.inference.runs.Load() != 0 || int(live.limiter.acquires.Load()) != 0 {
				t.Fatalf("a refused cursor started %d races and took %d slots, want none", live.inference.runs.Load(), int(live.limiter.acquires.Load()))
			}
			if got := results.seenResumes(); len(got) != 1 || got[0] != "qwen/"+resumeResultInvalidCursor {
				t.Fatalf("resume results = %v, want one invalid cursor", got)
			}
		})
	}
}

// Test flow:
//  1. Send a streamed request that completes under one caller.
//  2. Resume with another stream id, as another caller, with another body and on another escrow.
//  3. Assert each is a 410 with `resume_unavailable`, and no race ran or slot was taken beyond the first.
func TestAResumeThatNamesNothingTheCallerHoldsIsGoneAndSpendsNothing(t *testing.T) {
	live := newHarness(t)
	countingRequestIDs(live)
	results := newSpyStreamEvents()
	live.server.streamEvents = results
	live.inference.chunks = replyChunks
	live.request(t, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))
	acquired := int(live.limiter.acquires.Load())
	otherBody := `{"model":"qwen","messages":[{"role":"user","content":"other"}],"stream":true}`

	for name, refused := range map[string]*httptest.ResponseRecorder{
		"another id":     live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-9/0")),
		"another caller": live.request(t, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-b", "request-1/0")),
		"another body":   live.request(t, http.MethodPost, chatPath, otherBody, resumeHeaders("caller-a", "request-1/0")),
		"another escrow": live.request(t, http.MethodPost, "/devshard/7/v1/chat/completions", streamChatBody, resumeHeaders("caller-a", "request-1/0")),
	} {
		if refused.Code != http.StatusGone || !strings.Contains(refused.Body.String(), errorCodeResumeUnavailable) {
			t.Errorf("%s = %d %q, want 410 resume_unavailable", name, refused.Code, refused.Body.String())
		}
	}
	if live.inference.runs.Load() != 1 || int(live.limiter.acquires.Load()) != acquired {
		t.Fatalf("races %d slots %d, want 1 race and %d slots: a refused resume spends nothing", live.inference.runs.Load(), int(live.limiter.acquires.Load()), acquired)
	}
	if got := len(results.seenResumes()); got != 4 {
		t.Fatalf("resume results = %v, want four unavailable", results.seenResumes())
	}
}

// Test flow:
//  1. Start a streamed request that delivers one event and waits, so its socket is still attached.
//  2. Resume from event 1 while the original handler is still running, then let its race continue and finish.
//  3. Assert the displaced handler's reply stops after the first event, the resumed reply is the rest, and the finished stream is cached whole.
func TestAResumeDisplacesTheSocketOfTheHandlerStillRunning(t *testing.T) {
	live := newHarness(t)
	countingRequestIDs(live)
	inline := newInlineEngine()
	results := newSpyStreamEvents()
	live.server.inference = inline
	live.server.streamEvents = results
	first := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		live.requestInto(t, first, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))
	}()
	<-inline.arrived

	resumed := httptest.NewRecorder()
	resumedDone := make(chan struct{})
	go func() {
		defer close(resumedDone)
		live.requestInto(t, resumed, http.MethodPost, chatPath, streamChatBody, resumeHeaders("caller-a", "request-1/1"))
	}()
	awaitValue(t, results.attached, "the resume to take the log over")
	close(inline.release)
	<-firstDone
	<-resumedDone

	if first.Body.String() != firstEvent {
		t.Fatalf("displaced reply = %q, want only what it had before the takeover", first.Body.String())
	}
	if resumed.Body.String() != secondEvent+doneEvent {
		t.Fatalf("resumed reply = %q, want %q", resumed.Body.String(), secondEvent+doneEvent)
	}
	cached := live.request(t, http.MethodPost, chatPath, streamChatBody, callerHeaders("caller-a"))
	if cached.Body.String() != fullStream || inline.runs.Load() != 1 {
		t.Fatalf("repeat = %q after %d races, want the whole stream from the cache", cached.Body.String(), inline.runs.Load())
	}
}
