package engine

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"devshard/cmd/gateway/scheduler"
	"devshard/transport"
)

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
	StreamBytes() int64
}

// hostLimiter is satisfied by *limits.ParticipantLimiter. The scheduler took the host's concurrency
// slot in the same step that committed the nonce; the attempt that spends the nonce gives it back.
type hostLimiter interface {
	Release(participant, model string)
}

type chunkFacts struct {
	Content       bool
	ContentSource string

	// Error excludes the capability refusals a different host can still serve, which are reported
	// through ErrorSource and CapabilityRefused instead.
	Error             bool
	ErrorSource       string
	ErrorCode         string
	ErrorType         string
	ErrorMessage      string
	ErrorPayload      string
	CapabilityRefused bool
	ContextLimit      uint64
	ToolsUnsupported  bool

	UsageCompletionTokens int64
	// TokensBurned separates a model that produced nothing from a host that carried nothing.
	TokensBurned bool
}

// streamClassifier reassembles one attempt's SSE bytes and reports what they contained. Release frees
// the reassembly buffer and the byte reservations behind it.
type streamClassifier interface {
	Classify(chunk []byte) chunkFacts
	Flush() chunkFacts
	Release()
}

// AttemptSpec is everything one attempt needs, fixed at construction and never written afterwards.
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

	// Events must be drained until every started attempt has delivered its AttemptDone: that delivery
	// is what lets a committed nonce be settled, so the attempt goroutine waits for it rather than
	// dropping it.
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

	outputChunks  int64
	contentChunks int64
	outputBytes   int64
	streamBytes   int64

	usageCompletionTokens int64
	tokensBurned          bool
	contentSource         string
	errorSource           string
	errorCode             string
	errorType             string
	errorMessage          string
	errorPayload          string
	capabilityRefused     bool
	contextLimit          uint64
	toolsUnsupported      bool

	confirmed      bool
	stateDivergent bool
	terminal       Terminal
	lifecycle      Lifecycle
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
		state.confirmed = response.Confirmed()
		state.streamBytes = response.StreamBytes()
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
	w.state.outputChunks++
	w.state.outputBytes += int64(len(chunk))

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
	w.spec.offer(w.progress(AttemptChunk, now))

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
		s.contextLimit = facts.ContextLimit
		s.toolsUnsupported = facts.ToolsUnsupported
	}
	if facts.UsageCompletionTokens > 0 {
		s.usageCompletionTokens = facts.UsageCompletionTokens
	}
	s.tokensBurned = s.tokensBurned || facts.TokensBurned
}

func (s *attemptState) classify(ctx context.Context, spec AttemptSpec, err error) {
	if err != nil {
		s.lifecycle.EscrowMissing = transport.IsUpstreamEscrowNotFound(err)
		s.stateDivergent = errors.Is(err, ErrStateRootDivergence)
		s.terminal = classifyDispatchError(ctx, err)
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

func classifyDispatchError(ctx context.Context, err error) Terminal {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return TerminalClientCancelled
	}

	var status *transport.UpstreamStatusError
	if errors.As(err, &status) {
		if !strings.Contains(status.Path, "/chat/completions") {
			return TerminalOffPath
		}
		switch status.StatusCode {
		case http.StatusTooManyRequests:
			return TerminalThrottled
		case http.StatusServiceUnavailable:
			return TerminalUnavailable
		case http.StatusForbidden:
			return TerminalForbidden
		case http.StatusNotFound:
			return TerminalNotFound
		case http.StatusUnauthorized:
			if strings.Contains(strings.ToLower(status.Body), "timestamp drift") {
				return TerminalTimestampDrift
			}
		}
		return TerminalRejected
	}

	switch {
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

		SendTime:    s.sendTime,
		ReceiptTime: s.receiptTime,
		FirstToken:  s.firstToken,
		Completed:   s.completed,

		OutputChunks:          s.outputChunks,
		ContentChunks:         s.contentChunks,
		OutputBytes:           s.outputBytes,
		StreamBytes:           s.streamBytes,
		UsageCompletionTokens: s.usageCompletionTokens,

		Terminal:  s.terminal,
		Confirmed: s.confirmed,

		ContentSource: s.contentSource,
		ErrorSource:   s.errorSource,
		ErrorCode:     s.errorCode,
		ErrorType:     s.errorType,
		ErrorMessage:  s.errorMessage,
		ErrorPayload:  s.errorPayload,

		ContextLimit:     s.contextLimit,
		ToolsUnsupported: s.toolsUnsupported,

		StateDivergent: s.stateDivergent,
	}
}

func (spec AttemptSpec) emit(event AttemptEvent) {
	spec.Events <- event
}

// offer drops progress the coordinator is too busy to take: a full channel means it is already awake,
// and the next delivered event carries the same running totals.
func (spec AttemptSpec) offer(event AttemptEvent) {
	select {
	case spec.Events <- event:
	default:
	}
}
