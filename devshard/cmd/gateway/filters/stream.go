package filters

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	// MaxStreamCarryBytes bounds the unterminated tail held per stream: a host that never sends an
	// event terminator would otherwise grow it without limit. The size is derived, not picked — see
	// gateway-request-filtering.md, "The response side".
	MaxStreamCarryBytes = 32 << 20

	// chunkObject is the object name every synthesised event carries; it is what tells a streaming
	// client to read choices[].delta instead of choices[].message.
	chunkObject = "chat.completion.chunk"
)

var (
	sseEventSeparator     = []byte("\n\n")
	sseEventSeparatorCRLF = []byte("\r\n\r\n")
	sseDataPrefix         = []byte("data: ")
	sseDoneMarker         = []byte("[DONE]")

	// sseMessageKey is the cheap pre-check for a complete chat.completion: only a completion carries
	// choices[].message, and only such an event has to be converted into chunks.
	sseMessageKey = []byte(`"message"`)

	// sseDataParsePrefix deliberately omits the space sseDataPrefix emits; the two must not be
	// unified. See gateway-request-filtering.md, "Two `data:` prefixes that must not be unified".
	sseDataParsePrefix = []byte("data:")

	// SSEDoneEvent is the terminator an SSE client reads until; without it the client waits out its
	// own timeout instead of finishing.
	SSEDoneEvent = []byte("data: [DONE]\n\n")

	// NoResponseDataBody is the reply a non-streaming caller gets when the stream carried no payload.
	NoResponseDataBody = []byte(`{"error":{"message":"no response data"}}`)

	// ErrStreamCarryOverflow reports an unterminated SSE event larger than MaxStreamCarryBytes.
	ErrStreamCarryOverflow = errors.New("sse event exceeds carry limit")
	// ErrStreamTruncatedEvent reports a trailing partial event dropped at end of stream.
	ErrStreamTruncatedEvent = errors.New("sse stream ended mid-event")
)

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

// rewriteEvent returns the event as the client must read it: a complete chat.completion becomes the
// chunk events an OpenAI streaming client renders, and clientStrippedFields are removed from the JSON
// payload. It returns nil when that payload does not parse and must be dropped rather than forwarded.
func rewriteEvent(event []byte) []byte {
	convertible := bytes.Contains(event, sseMessageKey)
	if !convertible && !hasStrippableField(event) {
		return event
	}
	start, end, isObject := soleDataObject(event)
	if !isObject {
		return event
	}
	payload := event[start:end]
	if convertible {
		filtered, outcome := stripInternalFields(payload)
		switch outcome {
		case stripMalformed:
			return nil
		case stripUnchanged:
			filtered = payload
		}
		if chunks, converted := completionAsChunks(filtered); converted {
			return chunks
		}
		if outcome != stripRewritten {
			return event
		}
		return spliceEvent(event, start, end, filtered)
	}

	rewritten := append(make([]byte, 0, len(event)), event[:start]...)
	rewritten, changed, err := appendStripped(rewritten, payload)
	switch {
	case err != nil:
		return nil
	case !changed:
		return event
	}
	return append(rewritten, event[end:]...)
}

func spliceEvent(event []byte, start, end int, payload []byte) []byte {
	rewritten := make([]byte, 0, len(event)-(end-start)+len(payload))
	rewritten = append(rewritten, event[:start]...)
	rewritten = append(rewritten, payload...)
	return append(rewritten, event[end:]...)
}

// soleDataObject locates the JSON object an event carries, accepting "data:" with or without the
// space the wire only recommends, and whatever event/id/comment lines precede it. An event with no
// data line, with more than one, or whose payload is not an object has nothing to rewrite.
func soleDataObject(event []byte) (start, end int, ok bool) {
	found := false
	for offset := 0; offset < len(event); {
		line, lineEnd := event[offset:], len(event)
		if breakAt := bytes.IndexByte(line, '\n'); breakAt >= 0 {
			line, lineEnd = line[:breakAt], offset+breakAt+1
		}
		if data, isData := bytes.CutPrefix(line, sseDataParsePrefix); isData {
			if found {
				return 0, 0, false
			}
			found = true
			leading := bytes.TrimLeft(data, " \t")
			start = offset + len(line) - len(leading)
			end = start + len(bytes.TrimRight(leading, " \t\r"))
		}
		offset = lineEnd
	}
	return start, end, found && end > start && event[start] == '{'
}

// sseCompletion is the complete chat.completion some hosts answer a streaming request with, and
// sseChunk is the chat.completion.chunk an OpenAI streaming client actually reads.
type sseCompletion struct {
	ID                string                `json:"id"`
	Created           int64                 `json:"created"`
	Model             string                `json:"model"`
	SystemFingerprint string                `json:"system_fingerprint,omitempty"`
	Choices           []sseCompletionChoice `json:"choices"`
	Usage             json.RawMessage       `json:"usage"`
}

type sseCompletionChoice struct {
	Index        int                        `json:"index"`
	Message      map[string]json.RawMessage `json:"message"`
	FinishReason *string                    `json:"finish_reason"`
	StopReason   json.RawMessage            `json:"stop_reason"`
}

type sseChunk struct {
	ID                string           `json:"id"`
	Object            string           `json:"object"`
	Created           int64            `json:"created"`
	Model             string           `json:"model"`
	SystemFingerprint string           `json:"system_fingerprint,omitempty"`
	Choices           []sseChunkChoice `json:"choices"`
	Usage             json.RawMessage  `json:"usage,omitempty"`
}

type sseChunkChoice struct {
	Index        int                        `json:"index"`
	Delta        map[string]json.RawMessage `json:"delta"`
	FinishReason *string                    `json:"finish_reason"`
	StopReason   json.RawMessage            `json:"stop_reason,omitempty"`
}

// completionAsChunks converts a complete chat.completion into the chunk events a streaming client
// renders. A host that answers a stream with a whole response hands the client a message where it
// reads a delta, so the client renders nothing while the nonce is settled and the money is spent.
// The role, the payload and the finish reason travel as separate chunks, as a real stream sends them.
func completionAsChunks(payload []byte) ([]byte, bool) {
	var completion sseCompletion
	if json.Unmarshal(payload, &completion) != nil {
		return nil, false
	}
	var events bytes.Buffer
	for _, choice := range completion.Choices {
		if choice.Message == nil {
			continue
		}
		if role, named := choice.Message["role"]; named {
			emitChunk(&events, completion, []sseChunkChoice{{
				Index: choice.Index,
				Delta: map[string]json.RawMessage{"role": role},
			}}, nil)
		}
		delta := make(map[string]json.RawMessage, len(choice.Message))
		for field, value := range choice.Message {
			if field != "role" {
				delta[field] = value
			}
		}
		if len(delta) > 0 {
			emitChunk(&events, completion, []sseChunkChoice{{Index: choice.Index, Delta: delta}}, nil)
		}
		if stopReason := presentValue(choice.StopReason); choice.FinishReason != nil || stopReason != nil {
			emitChunk(&events, completion, []sseChunkChoice{{
				Index:        choice.Index,
				Delta:        map[string]json.RawMessage{},
				FinishReason: choice.FinishReason,
				StopReason:   stopReason,
			}}, nil)
		}
	}
	if events.Len() == 0 {
		return nil, false
	}
	if usage := presentValue(completion.Usage); usage != nil {
		emitChunk(&events, completion, []sseChunkChoice{}, usage)
	}
	return events.Bytes(), true
}

func emitChunk(events *bytes.Buffer, completion sseCompletion, choices []sseChunkChoice, usage json.RawMessage) {
	encoded, err := json.Marshal(sseChunk{
		ID:                completion.ID,
		Object:            chunkObject,
		Created:           completion.Created,
		Model:             completion.Model,
		SystemFingerprint: completion.SystemFingerprint,
		Choices:           choices,
		Usage:             usage,
	})
	if err != nil {
		return
	}
	events.Write(sseDataPrefix)
	events.Write(encoded)
	events.Write(sseEventSeparator)
}

// presentValue returns a raw JSON field only when it was sent with a value, JSON null counting as
// absent so a field the host spelled out as null is not re-sent as one the client must interpret.
func presentValue(raw json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return trimmed
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
