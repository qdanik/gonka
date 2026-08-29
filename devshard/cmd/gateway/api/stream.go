package api

import (
	"bytes"
	"cmp"
	"fmt"
	"net/http"
	"sync"

	json "github.com/goccy/go-json"

	"devshard/cmd/gateway/filters"
)

const (
	EscrowHeader = "X-Devshard-ID"

	RequestIDHeader = "X-Request-Id"

	// Without this a host that wins the race on its first token can then send unlimited valid SSE and
	// take the process out. It tracks the carry cap on purpose: this buffer holds the same
	// prompt_token_ids frames that cap was derived from, so lowering one without the other would
	// refuse the large-context replies the arithmetic was meant to admit.
	maxBufferedResponseBytes = filters.MaxStreamCarryBytes
)

type clientStream struct {
	// An attempt goroutine can still be writing after the handler returned, and Go invalidates the
	// writer at that moment: every path that touches it locks and stops once closed.
	mu     sync.Mutex
	closed bool

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

func newClientStream(w http.ResponseWriter, requestID string, streaming, usage bool, logprobs filters.LogprobIntent) *clientStream {
	stream := &clientStream{
		writer:     w,
		controller: http.NewResponseController(w),
		requestID:  requestID,
		streaming:  streaming,
		logprobs:   logprobs,
	}
	if streaming {
		stream.rewriter = filters.NewStreamRewriter(logprobs, usage)
	}
	return stream
}

func (c *clientStream) Header() http.Header { return c.writer.Header() }

func (c *clientStream) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func (c *clientStream) delivered() (written int64, terminated bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.written, c.terminated
}

func (c *clientStream) Write(chunk []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return len(chunk), nil
	}
	if !c.streaming {
		if len(c.buffered)+len(chunk) > maxBufferedResponseBytes {
			return 0, fmt.Errorf("buffered response exceeds %d bytes", maxBufferedResponseBytes)
		}
		c.buffered = append(c.buffered, chunk...)
		return len(chunk), nil
	}
	c.beginLocked("text/event-stream")
	rewritten, err := c.rewriter.Write(chunk)
	if err != nil {
		return 0, err
	}
	if len(rewritten) > 0 {
		if _, writeErr := c.emitLocked(rewritten); writeErr != nil {
			return 0, writeErr
		}
	}
	return len(chunk), nil
}

func (c *clientStream) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushLocked()
}

func (c *clientStream) flushLocked() {
	if c.started && !c.closed {
		_ = c.controller.Flush()
	}
}

func (c *clientStream) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	defer func() { c.closed = true }()
	if c.streaming {
		tailErr := c.flushTailLocked()
		if err := c.terminateLocked(); err != nil {
			return err
		}
		return tailErr
	}
	body := filters.StripResponseBody(filters.AssembleSSEBody(c.buffered), c.logprobs)
	c.beginStatusLocked("application/json", statusForAssembled(body))
	written, err := c.writer.Write(body)
	c.written += int64(written)
	return err
}

func (c *clientStream) Fail(cause error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.streaming || !c.started {
		return nil
	}
	defer func() { c.closed = true }()
	tailErr := c.flushTailLocked()
	if _, err := c.emitLocked(errorEvent(cause)); err != nil {
		return err
	}
	if err := c.terminateLocked(); err != nil {
		return err
	}
	return tailErr
}

func (c *clientStream) flushTailLocked() error {
	tail, err := c.rewriter.Close()
	if len(tail) > 0 {
		if _, writeErr := c.emitLocked(tail); writeErr != nil {
			return writeErr
		}
	}
	return err
}

func (c *clientStream) terminateLocked() error {
	var err error
	if !c.terminated {
		_, err = c.emitLocked(filters.SSEDoneEvent)
	}
	c.flushLocked()
	return err
}

func (c *clientStream) emitLocked(events []byte) (int, error) {
	c.beginLocked("text/event-stream")
	c.terminated = c.terminated || filters.HasSSEDone(events)
	written, err := c.writer.Write(events)
	c.written += int64(written)
	return written, err
}

func errorEvent(cause error) []byte {
	payload, err := json.Marshal(errorEnvelope{Error: errorDetail{Message: cause.Error()}})
	if err != nil {
		return nil
	}
	return append(append([]byte("data: "), payload...), '\n', '\n')
}

// One place for every chat reply header, so a cache replay and a live race cannot answer the same
// request differently.
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

// statusForAssembled answers 502 for the bodies the assembler substitutes for an answer it could not
// fold: the host is upstream, and neither body carries a status of its own.
func statusForAssembled(body []byte) int {
	if bytes.Equal(body, filters.TruncatedResponseBody) || bytes.Equal(body, filters.NoResponseDataBody) {
		return http.StatusBadGateway
	}
	return http.StatusOK
}

func (c *clientStream) beginLocked(contentType string) {
	c.beginStatusLocked(contentType, http.StatusOK)
}

func (c *clientStream) beginStatusLocked(contentType string, status int) {
	if c.started {
		return
	}
	writeChatHeaders(c.writer.Header(), c.requestID, "", contentType, c.streaming)
	c.writer.WriteHeader(status)
	c.started = true
}
