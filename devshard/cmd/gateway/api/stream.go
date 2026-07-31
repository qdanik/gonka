package api

import (
	"encoding/json"
	"net/http"

	"devshard/cmd/gateway/filters"
)

// clientStream is the sink the race writes its winner into. It holds the status open until the
// first byte, so a failure before any content still returns a real status instead of a 200 with an
// error body, and it strips the gateway's internal response fields on the way out.
type clientStream struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
	requestID  string
	streaming  bool
	rewriter   *filters.StreamRewriter
	buffered   []byte
	started    bool
	terminated bool
}

func newClientStream(w http.ResponseWriter, requestID string, streaming bool) *clientStream {
	stream := &clientStream{
		writer:     w,
		controller: http.NewResponseController(w),
		requestID:  requestID,
		streaming:  streaming,
	}
	if streaming {
		stream.rewriter = filters.NewStreamRewriter()
	}
	return stream
}

func (c *clientStream) Header() http.Header { return c.writer.Header() }

// Started reports whether the client has already seen a status, after which no error can replace it.
func (c *clientStream) Started() bool { return c.started }

func (c *clientStream) Write(chunk []byte) (int, error) {
	if !c.streaming {
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
	_, err := c.writer.Write(filters.StripResponseBody(filters.AssembleSSEBody(c.buffered)))
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
	return c.writer.Write(events)
}

// errorEvent renders a failure as the single SSE data event an OpenAI-compatible client decodes.
func errorEvent(cause error) []byte {
	payload, err := json.Marshal(errorEnvelope{Error: errorDetail{Message: cause.Error()}})
	if err != nil {
		return nil
	}
	return append(append([]byte("data: "), payload...), '\n', '\n')
}

func (c *clientStream) begin(contentType string) {
	if c.started {
		return
	}
	header := c.writer.Header()
	if c.requestID != "" {
		header.Set("X-Request-Id", c.requestID)
	}
	header.Set("Content-Type", contentType)
	if c.streaming {
		header.Set("Cache-Control", "no-cache")
		header.Set("Connection", "keep-alive")
	}
	c.writer.WriteHeader(http.StatusOK)
	c.started = true
}
