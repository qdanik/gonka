package filters

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	sseEventSeparator     = []byte("\n\n")
	sseEventSeparatorCRLF = []byte("\r\n\r\n")
	sseDataPrefix         = []byte("data: ")
	sseDataObjectHead     = []byte("data: {")
)

// MaxStreamCarryBytes bounds the unterminated tail held per stream: a host that never sends an
// event terminator would otherwise grow it without limit. The largest legitimate frame carries
// prompt_token_ids, which the gateway forces on with return_token_ids — roughly 7 bytes per prompt
// token, so a million-token context already needs ~6.7 MiB. This covers ~4.8M tokens.
const MaxStreamCarryBytes = 32 << 20

var (
	// ErrStreamCarryOverflow reports an unterminated SSE event larger than MaxStreamCarryBytes.
	ErrStreamCarryOverflow = errors.New("sse event exceeds carry limit")
	// ErrStreamTruncatedEvent reports a trailing partial event dropped at end of stream.
	ErrStreamTruncatedEvent = errors.New("sse stream ended mid-event")
)

// StreamRewriter strips clientStrippedFields from an SSE stream delivered in arbitrary chunks,
// emitting complete events only and holding the trailing partial until it completes.
type StreamRewriter struct {
	carry   []byte
	scanned int
	failed  bool
}

// NewStreamRewriter returns a rewriter with an empty carry buffer.
func NewStreamRewriter() *StreamRewriter { return &StreamRewriter{} }

// Write appends chunk to the carry buffer and returns every event it completes, rewritten.
// Once the carry exceeds MaxStreamCarryBytes the rewriter fails permanently.
func (r *StreamRewriter) Write(chunk []byte) ([]byte, error) {
	if r.failed {
		return nil, ErrStreamCarryOverflow
	}
	r.carry = append(r.carry, chunk...)
	var out bytes.Buffer
	eventStart, searchFrom := 0, r.scanned
	for {
		offset := indexEventEnd(r.carry[searchFrom:])
		if offset < 0 {
			break
		}
		eventEnd := searchFrom + offset
		out.Write(rewriteEvent(r.carry[eventStart:eventEnd]))
		eventStart, searchFrom = eventEnd, eventEnd
	}
	r.carry = append(r.carry[:0], r.carry[eventStart:]...)
	r.scanned = max(0, len(r.carry)-len(sseEventSeparatorCRLF)+1)
	if len(r.carry) > MaxStreamCarryBytes {
		carried := len(r.carry)
		r.carry, r.scanned = nil, 0
		r.failed = true
		return out.Bytes(), fmt.Errorf("%w: %d bytes", ErrStreamCarryOverflow, carried)
	}
	return out.Bytes(), nil
}

// Close returns the trailing partial rewritten when it is a well-formed final event, and drops it
// with ErrStreamTruncatedEvent when its payload does not parse.
func (r *StreamRewriter) Close() ([]byte, error) {
	carry := r.carry
	r.carry, r.scanned = nil, 0
	if r.failed {
		return nil, ErrStreamCarryOverflow
	}
	if len(carry) == 0 {
		return nil, nil
	}
	final := rewriteEvent(carry)
	if final == nil {
		return nil, ErrStreamTruncatedEvent
	}
	return final, nil
}

// RewriteStreamChunk strips clientStrippedFields from every complete "data: {...}" SSE event in
// chunk, dropping events whose payload is not valid JSON; [DONE], comments and non-object lines
// pass through. It holds no state across calls, so streaming callers must use StreamRewriter.
func RewriteStreamChunk(chunk []byte) []byte {
	rewriter := NewStreamRewriter()
	emitted, err := rewriter.Write(chunk)
	if err != nil {
		return emitted
	}
	final, err := rewriter.Close()
	if err != nil {
		return emitted
	}
	return append(emitted, final...)
}

// rewriteEvent returns event with clientStrippedFields removed, or nil when its "data: {...}"
// payload does not parse and must be dropped rather than forwarded.
func rewriteEvent(event []byte) []byte {
	if !hasStrippableField(event) {
		return event
	}
	line := bytes.TrimRight(event, "\r\n")
	if !bytes.HasPrefix(line, sseDataObjectHead) {
		return event
	}
	filtered, outcome := stripInternalFields(bytes.TrimSpace(line[len(sseDataPrefix):]))
	switch outcome {
	case stripRewritten:
		rewritten := make([]byte, 0, len(sseDataPrefix)+len(filtered)+len(sseEventSeparator))
		rewritten = append(rewritten, sseDataPrefix...)
		rewritten = append(rewritten, filtered...)
		return append(rewritten, sseEventSeparator...)
	case stripUnchanged:
		return event
	default:
		return nil
	}
}

// indexEventEnd returns the offset just past the first LF or CRLF event terminator in buf, or -1.
func indexEventEnd(buf []byte) int {
	lineFeedIndex := bytes.Index(buf, sseEventSeparator)
	carriageReturnIndex := bytes.Index(buf, sseEventSeparatorCRLF)
	switch {
	case lineFeedIndex < 0 && carriageReturnIndex < 0:
		return -1
	case carriageReturnIndex < 0 || (lineFeedIndex >= 0 && lineFeedIndex <= carriageReturnIndex):
		return lineFeedIndex + len(sseEventSeparator)
	default:
		return carriageReturnIndex + len(sseEventSeparatorCRLF)
	}
}
