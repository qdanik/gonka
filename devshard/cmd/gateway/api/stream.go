package api

import (
	"cmp"
	"fmt"
	"net/http"

	"devshard/cmd/gateway/filters"

	json "github.com/goccy/go-json"
)

const (
	// EscrowHeader names the escrow that served a reply. A live race sets it as soon as the escrow is
	// picked, which is why writeChatHeaders takes it as an argument rather than reading it back.
	EscrowHeader = "X-Devshard-ID"

	// RequestIDHeader carries the correlation id a caller reconciles against the accounting ledger. The
	// pre-stream failure path sets it without the rest of the reply headers, so the name lives here
	// rather than inside writeChatHeaders.
	RequestIDHeader = "X-Request-Id"

	// maxBufferedResponseBytes bounds what a non-streaming reply accumulates in memory. The streaming
	// path forwards bytes as they arrive and only bounds an unterminated tail; a buffered reply holds
	// the whole response until Close, so without this a host that wins the race by producing one token
	// first can then send unlimited valid SSE and take the process out.
	maxBufferedResponseBytes = filters.MaxStreamCarryBytes
)

// clientStream is the sink the race writes its winner into: it holds the status open until the first
// byte and strips the gateway's internal response fields on the way out. See
// gateway-request-lifecycle.md, "9. Streaming out".
type clientStream struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
	requestID  string
	streaming  bool
	logprobs   filters.LogprobIntent
	rewriter   *filters.StreamRewriter
	buffered   []byte
	written    int64
	started    bool
	terminated bool
}

func newClientStream(w http.ResponseWriter, requestID string, streaming bool, logprobs filters.LogprobIntent) *clientStream {
	stream := &clientStream{
		writer:     w,
		controller: http.NewResponseController(w),
		requestID:  requestID,
		streaming:  streaming,
		logprobs:   logprobs,
	}
	if streaming {
		stream.rewriter = filters.NewStreamRewriter(logprobs)
	}
	return stream
}

func (c *clientStream) Header() http.Header { return c.writer.Header() }

// Started reports whether the client has already seen a status, after which no error can replace it.
func (c *clientStream) Started() bool { return c.started }

// delivered reports what actually left for the client: bytes written and whether the terminator the
// client waits for went with them. Nothing downstream can reconstruct this once the request is over.
func (c *clientStream) delivered() (written int64, terminated bool) {
	return c.written, c.terminated
}

func (c *clientStream) Write(chunk []byte) (int, error) {
	if !c.streaming {
		if len(c.buffered)+len(chunk) > maxBufferedResponseBytes {
			return 0, fmt.Errorf("buffered response exceeds %d bytes", maxBufferedResponseBytes)
		}
		c.buffered = append(c.buffered, chunk...)
		return len(chunk), nil
	}
	c.begin("text/event-stream")
	rewritten, err := c.rewriter.Write(chunk)
	if err != nil {
		return 0, err
	}
	if len(rewritten) > 0 {
		if _, writeErr := c.emit(rewritten); writeErr != nil {
			return 0, writeErr
		}
	}
	return len(chunk), nil
}

// Flush reaches the real writer through a ResponseController rather than a type assertion, so a
// wrapper that embeds http.ResponseWriter without re-exposing http.Flusher cannot swallow it.
func (c *clientStream) Flush() {
	if c.started {
		_ = c.controller.Flush()
	}
}

// Close emits whatever the response shape held back: a streamed tail event and the terminator, or
// the whole non-streaming body, which can only be assembled and stripped once it is complete.
func (c *clientStream) Close() error {
	if c.streaming {
		tailErr := c.flushTail()
		if err := c.terminate(); err != nil {
			return err
		}
		return tailErr
	}
	c.begin("application/json")
	written, err := c.writer.Write(filters.StripResponseBody(filters.AssembleSSEBody(c.buffered), c.logprobs))
	c.written += int64(written)
	return err
}

// Fail ends a stream the client is already reading with the failure and the terminator it would
// otherwise wait out its own timeout for.
func (c *clientStream) Fail(cause error) error {
	if !c.streaming || !c.started {
		return nil
	}
	tailErr := c.flushTail()
	if _, err := c.emit(errorEvent(cause)); err != nil {
		return err
	}
	if err := c.terminate(); err != nil {
		return err
	}
	return tailErr
}

func (c *clientStream) flushTail() error {
	tail, err := c.rewriter.Close()
	if len(tail) > 0 {
		if _, writeErr := c.emit(tail); writeErr != nil {
			return writeErr
		}
	}
	return err
}

// terminate sends the SSE terminator unless the host already sent one, then flushes.
func (c *clientStream) terminate() error {
	var err error
	if !c.terminated {
		_, err = c.emit(filters.SSEDoneEvent)
	}
	c.Flush()
	return err
}

// emit is the streaming path's only write, so no frame can reach the client without the response
// having begun and without being weighed against the terminator the client is waiting for.
func (c *clientStream) emit(events []byte) (int, error) {
	c.begin("text/event-stream")
	c.terminated = c.terminated || filters.HasSSEDone(events)
	written, err := c.writer.Write(events)
	c.written += int64(written)
	return written, err
}

// errorEvent renders a failure as the single SSE data event an OpenAI-compatible client decodes.
func errorEvent(cause error) []byte {
	payload, err := json.Marshal(errorEnvelope{Error: errorDetail{Message: cause.Error()}})
	if err != nil {
		return nil
	}
	return append(append([]byte("data: "), payload...), '\n', '\n')
}

// writeChatHeaders decides every header of a chat reply that reached the engine, so a replay from
// the cache and an answer from a live race cannot hand the same request different headers -- a
// difference a client would see intermittently, since it would depend on cache state. A request
// that failed before the stream started never gets here and carries RequestIDHeader alone.
func writeChatHeaders(header http.Header, requestID, escrowID, contentType string, streaming bool) {
	if requestID != "" {
		header.Set(RequestIDHeader, requestID)
	}
	if escrowID != "" {
		header.Set(EscrowHeader, escrowID)
	}
	header.Set("Content-Type", cmp.Or(contentType, "application/json"))
	if streaming {
		header.Set("Cache-Control", "no-cache")
		header.Set("Connection", "keep-alive")
	}
}

func (c *clientStream) begin(contentType string) {
	if c.started {
		return
	}
	writeChatHeaders(c.writer.Header(), c.requestID, "", contentType, c.streaming)
	c.writer.WriteHeader(http.StatusOK)
	c.started = true
}
