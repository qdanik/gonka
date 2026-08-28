package engine

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/scheduler"
	"devshard/transport"
)

const maxUpstreamBodyLogged = 256

// ErrStateRootDivergence is wrapped by the dispatcher adapter around the session's own detection, so
// the race can branch on a diverging post state root without reading upstream error text.
var ErrStateRootDivergence = errors.New("post state root divergence")

// dispatcher is the engine's only route to a host; an adapter over devshard/user.Session satisfies it.
type dispatcher interface {
	Send(ctx context.Context, nonce scheduler.Prepared, stream io.Writer, onReceipt func()) (Response, error)
}

// Response is one host's reply, forwarded untouched: only the session that produced it can apply the rest.
type Response interface {
	Confirmed() bool
	// ConfirmedAt is the executor's wall clock in seconds when it signed; 0 when not an executor.
	ConfirmedAt() int64
}

// hostLimiter is satisfied by *limits.ParticipantLimiter; the attempt that spends the nonce gives the
// host's concurrency slot back. See gateway-invariants.md, "5. The slot and the escrow hold are taken
// with the nonce, and given back after the vote".
type hostLimiter interface {
	Release(participant, model string)
}

// chunkFacts is what one SSE chunk carried. Error excludes the capability refusals a different host can
// still serve, which travel as CapabilityRefused instead; TokensBurned separates a model that produced
// nothing from a host that carried nothing.
type chunkFacts struct {
	Content       bool
	ContentSource string

	Error             bool
	ErrorSource       string
	ErrorCode         string
	ErrorType         string
	ErrorMessage      string
	ErrorPayload      string
	CapabilityRefused bool
	Capability        CapabilitySignal

	UsageCompletionTokens int64
	TokensBurned          bool
	LogprobsDecoded       bool
}

// streamClassifier reassembles one attempt's SSE bytes and reports what they contained. Release frees
// the reassembly buffer and the byte reservations behind it.
type streamClassifier interface {
	Classify(chunk []byte) chunkFacts
	Flush() chunkFacts
	Release()
}

// AttemptSpec is everything one attempt needs, fixed at construction and never written afterwards. Events
// must be drained until every started attempt has delivered its AttemptDone. See gateway-invariants.md,
// "1. A committed nonce is always settled".
type AttemptSpec struct {
	Escrow      string
	Model       string
	Participant string
	HostIdx     int
	HostLabel   string
	Role        string
	StartReason string
	Suspicious  bool

	Nonce scheduler.Prepared

	Dispatch   dispatcher
	Limiter    hostLimiter
	Classifier streamClassifier
	Sink       io.Writer
	Now        func() time.Time

	Events chan<- AttemptEvent
}

type AttemptEventKind int

const (
	AttemptDispatched AttemptEventKind = iota
	AttemptReceipt
	AttemptFirstToken
	AttemptContent
	AttemptChunk
	AttemptDone
)

type AttemptEvent struct {
	Kind  AttemptEventKind
	Nonce uint64
	At    time.Time

	Outcome   *AttemptOutcome
	Lifecycle Lifecycle
}

// attemptState is owned by the attempt's own goroutine and is never read by another goroutine. Every
// fact the race needs travels as an AttemptEvent instead.
type attemptState struct {
	sendTime     time.Time
	receiptTime  time.Time
	firstToken   time.Time
	firstContent time.Time
	completed    time.Time

	contentChunks int64
	streamChunks  int64
	outputBytes   int64

	lastChunk     time.Time
	maxChunkGap   time.Duration
	maxGapChunk   int64
	droppedEvents int64

	usageCompletionTokens int64
	tokensBurned          bool
	logprobsDecoded       bool
	contentSource         string
	capability            CapabilitySignal
	errorSource           string
	errorCode             string
	errorType             string
	errorMessage          string
	errorPayload          string
	capabilityRefused     bool

	confirmed      bool
	confirmedAt    int64
	stateDivergent bool
	terminal       Terminal
	lifecycle      Lifecycle

	upstreamStatus int
	upstreamBody   string
}

func runAttempt(ctx context.Context, spec AttemptSpec) {
	defer spec.Classifier.Release()
	defer spec.Limiter.Release(spec.Participant, spec.Model)

	nonce := spec.Nonce.Nonce()
	state := &attemptState{sendTime: spec.Now()}
	spec.emit(AttemptEvent{Kind: AttemptDispatched, Nonce: nonce, At: state.sendTime})

	writer := &attemptWriter{spec: spec, state: state, nonce: nonce}
	response, err := spec.Dispatch.Send(ctx, spec.Nonce, writer, func() {
		if !state.receiptTime.IsZero() {
			return
		}
		state.receiptTime = spec.Now()
		spec.emit(AttemptEvent{Kind: AttemptReceipt, Nonce: nonce, At: state.receiptTime})
	})

	state.completed = spec.Now()
	if response != nil {
		state.confirmed, state.confirmedAt = response.Confirmed(), response.ConfirmedAt()
	}
	state.classify(ctx, spec, err)

	spec.emit(AttemptEvent{
		Kind:      AttemptDone,
		Nonce:     nonce,
		At:        state.completed,
		Outcome:   state.outcome(spec),
		Lifecycle: state.lifecycle,
	})
}

type attemptWriter struct {
	spec  AttemptSpec
	state *attemptState
	nonce uint64
}

func (w *attemptWriter) Write(chunk []byte) (int, error) {
	now := w.spec.Now()

	w.state.streamChunks++
	w.state.outputBytes += int64(len(chunk))
	w.state.recordChunkGap(now, chunk)
	if w.state.firstToken.IsZero() {
		w.state.firstToken = now
		w.spec.emit(w.progress(AttemptFirstToken, now))
	}

	facts := w.spec.Classifier.Classify(chunk)
	w.state.record(facts)
	if facts.Content && w.state.firstContent.IsZero() {
		w.state.firstContent = now
		w.spec.emit(w.progress(AttemptContent, now))
	}
	if !w.spec.offer(w.progress(AttemptChunk, now)) {
		w.state.droppedEvents++
	}

	return w.spec.Sink.Write(chunk)
}

// Flush is how the transport's per-line flush reaches the client: without it the assertion the
// transport makes on its writer fails and a crowned winner's bytes sit in the server's buffer.
func (w *attemptWriter) Flush() {
	if flusher, ok := w.spec.Sink.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *attemptWriter) progress(kind AttemptEventKind, at time.Time) AttemptEvent {
	return AttemptEvent{Kind: kind, Nonce: w.nonce, At: at}
}

// recordChunkGap keeps the longest silence a host left mid-stream, which a chunk count cannot show.
// The silence before [DONE] is the end of the stream, not a host that went quiet.
func (s *attemptState) recordChunkGap(now time.Time, chunk []byte) {
	previous := s.lastChunk
	s.lastChunk = now
	if previous.IsZero() || filters.HasSSEDone(chunk) {
		return
	}
	if gap := now.Sub(previous); gap > s.maxChunkGap {
		s.maxChunkGap, s.maxGapChunk = gap, s.streamChunks
	}
}

// meanChunkGap is the average silence between chunks, which is the inverse of the delivered rate.
func (s *attemptState) meanChunkGap() time.Duration {
	if s.streamChunks < 2 || s.firstToken.IsZero() || !s.lastChunk.After(s.firstToken) {
		return 0
	}
	return s.lastChunk.Sub(s.firstToken) / time.Duration(s.streamChunks-1)
}

func (s *attemptState) record(facts chunkFacts) {
	if facts.Content || facts.Error {
		s.contentChunks++
	}

	if facts.ContentSource != "" && s.contentSource == "" {
		s.contentSource = facts.ContentSource
	}
	if facts.ErrorSource != "" && s.errorSource == "" {
		s.errorSource = facts.ErrorSource
		s.errorCode = facts.ErrorCode
		s.errorType = facts.ErrorType
		s.errorMessage = facts.ErrorMessage
		s.errorPayload = facts.ErrorPayload
		s.capabilityRefused = facts.CapabilityRefused
		s.capability = facts.Capability
	}
	if facts.UsageCompletionTokens > 0 {
		s.usageCompletionTokens = facts.UsageCompletionTokens
	}
	s.tokensBurned = s.tokensBurned || facts.TokensBurned
	s.logprobsDecoded = s.logprobsDecoded || facts.LogprobsDecoded
}

func (s *attemptState) classify(ctx context.Context, spec AttemptSpec, err error) {
	if err != nil {
		s.lifecycle.EscrowMissing = transport.IsUpstreamEscrowNotFound(err)
		s.stateDivergent = errors.Is(err, ErrStateRootDivergence)
		s.upstreamStatus, s.upstreamBody = upstreamRefusal(err)
		s.terminal = classifyDispatchError(ctx, err)
		s.capability = capabilityOfDispatchError(err)
		return
	}
	s.record(spec.Classifier.Flush())

	switch {
	case s.receiptTime.IsZero():
		s.terminal = TerminalNoReceipt
	case s.errorSource != "" && s.capabilityRefused:
		s.terminal = TerminalCapabilityRefused
	case s.errorSource != "":
		s.terminal = TerminalErrorStream
	case s.contentChunks > 0:
		s.terminal = TerminalLost
	case s.tokensBurned:
		s.terminal = TerminalBurnEmpty
	default:
		s.terminal = TerminalEmptyStream
	}
}

// upstreamRefusal keeps what the host said when it refused. The body is truncated because a log line
// needs the reason, not the payload.
func upstreamRefusal(err error) (int, string) {
	var status *transport.UpstreamStatusError
	if !errors.As(err, &status) {
		return 0, ""
	}
	body := strings.TrimSpace(status.Body)
	if len(body) > maxUpstreamBodyLogged {
		body = body[:maxUpstreamBodyLogged]
	}
	return status.StatusCode, body
}

func classifyDispatchError(ctx context.Context, err error) Terminal {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return TerminalClientCancelled
	}

	var status *transport.UpstreamStatusError
	if errors.As(err, &status) {
		if !strings.Contains(status.Path, "/chat/completions") {
			return TerminalOffPath
		}
		if status.StatusCode == http.StatusUnauthorized &&
			!strings.Contains(strings.ToLower(status.Body), "timestamp drift") {
			return TerminalRejected
		}
		if terminal, recovered := terminalForStatus[status.StatusCode]; recovered {
			return terminal
		}
		return TerminalRejected
	}

	switch {
	case errors.Is(err, transport.ErrSSEEventTooLarge), errors.Is(err, transport.ErrResponseBodyTooLarge):
		return TerminalResponseTooLarge
	case errors.Is(err, transport.ErrSSEStreamTruncated):
		return TerminalStreamTruncated
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return TerminalUnexpectedEOF
	}
	return TerminalDialFailure
}

func (s *attemptState) outcome(spec AttemptSpec) *AttemptOutcome {
	return &AttemptOutcome{
		Participant: spec.Participant,
		HostIdx:     spec.HostIdx,
		HostLabel:   spec.HostLabel,
		Nonce:       spec.Nonce.Nonce(),
		Role:        spec.Role,
		StartReason: spec.StartReason,
		Suspicious:  spec.Suspicious,

		SendTime:     s.sendTime,
		ReceiptTime:  s.receiptTime,
		FirstToken:   s.firstToken,
		FirstContent: s.firstContent,
		LastChunk:    s.lastChunk,
		Completed:    s.completed,

		ContentChunks:         s.contentChunks,
		StreamChunks:          s.streamChunks,
		UsageCompletionTokens: s.usageCompletionTokens,
		OutputBytes:           s.outputBytes,
		LogprobsDecoded:       s.logprobsDecoded,
		MaxChunkGap:           s.maxChunkGap,
		MaxChunkGapAt:         s.maxGapChunk,
		MeanChunkGap:          s.meanChunkGap(),
		DroppedEvents:         s.droppedEvents,

		Terminal:    s.terminal,
		Confirmed:   s.confirmed,
		ConfirmedAt: s.confirmedAt,

		UpstreamStatus: s.upstreamStatus,
		UpstreamBody:   s.upstreamBody,

		ContentSource: s.contentSource,
		Capability:    s.capability,
		ErrorSource:   s.errorSource,
		ErrorCode:     s.errorCode,
		ErrorType:     s.errorType,
		ErrorMessage:  s.errorMessage,
		ErrorPayload:  s.errorPayload,

		StateDivergent: s.stateDivergent,
	}
}

func (spec AttemptSpec) emit(event AttemptEvent) {
	spec.Events <- event
}

// offer drops progress the coordinator is too busy to take, where emit blocks; expire drains the
// queue before reading lastChunk, so a drop only ever ages it between reads.
func (spec AttemptSpec) offer(event AttemptEvent) bool {
	select {
	case spec.Events <- event:
		return true
	default:
		return false
	}
}
