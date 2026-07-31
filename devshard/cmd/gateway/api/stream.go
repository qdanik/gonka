package api

import (
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
		if _, writeErr := c.writer.Write(rewritten); writeErr != nil {
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

// Close emits whatever the response shape held back: a streamed tail event, or the whole
// non-streaming body, which can only be stripped once it is complete.
func (c *clientStream) Close() error {
	if c.streaming {
		tail, err := c.rewriter.Close()
		if len(tail) > 0 {
			c.begin("text/event-stream")
			if _, writeErr := c.writer.Write(tail); writeErr != nil {
				return writeErr
			}
		}
		c.Flush()
		return err
	}
	c.begin("application/json")
	_, err := c.writer.Write(filters.StripResponseBody(c.buffered))
	return err
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
