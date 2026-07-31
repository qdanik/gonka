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
	sseDoneMarker         = []byte("[DONE]")
)

// sseDataParsePrefix deliberately omits the space sseDataPrefix emits: the wire allows "data:{...}"
// and a reader that demanded the space would classify every space-less frame as no data at all.
var sseDataParsePrefix = []byte("data:")

// SSEDoneEvent is the terminator an SSE client reads until; without it the client waits out its
// own timeout instead of finishing.
var SSEDoneEvent = []byte("data: [DONE]\n\n")

// NoResponseDataBody is the reply a non-streaming caller gets when the stream carried no payload.
var NoResponseDataBody = []byte(`{"error":{"message":"no response data"}}`)

// eachSSELine visits the trimmed payload of every "data:" line in events, including the empty ones
// and the terminator, and stops as soon as visit reports the line it wanted.
func eachSSELine(events []byte, visit func(payload []byte) bool) {
	for rest := events; len(rest) > 0; {
		var line []byte
		line, rest, _ = bytes.Cut(rest, []byte("\n"))
		data, isData := bytes.CutPrefix(bytes.TrimRight(line, "\r"), sseDataParsePrefix)
		if !isData {
			continue
		}
		if visit(bytes.TrimSpace(data)) {
			return
		}
	}
}

// EachSSEDataPayload visits the trimmed payload of every "data:" line in events, skipping empty
// lines and [DONE], and stops as soon as visit reports the payload it wanted.
func EachSSEDataPayload(events []byte, visit func(payload []byte) bool) {
	eachSSELine(events, func(payload []byte) bool {
		if len(payload) == 0 || bytes.Equal(payload, sseDoneMarker) {
			return false
		}
		return visit(payload)
	})
}

// HasSSEDone reports whether events already carry the terminator, so it is never sent twice. The
// check is line-anchored: "[DONE]" inside a content delta is not a terminator.
func HasSSEDone(events []byte) bool {
	terminated := false
	eachSSELine(events, func(payload []byte) bool {
		terminated = bytes.Equal(payload, sseDoneMarker)
		return terminated
	})
	return terminated
}

// AssembleSSEBody reduces an SSE-framed reply to the single JSON body a non-streaming caller
// expects: the payload of the last data event. A body carrying no data line at all is already that
// body and passes through; an SSE-framed one carrying no payload becomes NoResponseDataBody.
func AssembleSSEBody(body []byte) []byte {
	var assembled []byte
	framed := false
	eachSSELine(body, func(payload []byte) bool {
		framed = true
		if len(payload) > 0 && !bytes.Equal(payload, sseDoneMarker) {
			assembled = payload
		}
		return false
	})
	switch {
	case len(assembled) > 0:
		return assembled
	case framed || len(bytes.TrimSpace(body)) == 0:
		return NoResponseDataBody
	default:
		return body
	}
}

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
