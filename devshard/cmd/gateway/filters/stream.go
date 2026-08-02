package filters

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"

	json "github.com/goccy/go-json"
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

// StreamRewriter strips the fields a client must not see from an SSE stream delivered in arbitrary
// chunks, emitting complete events only and holding the trailing partial until it completes.
type StreamRewriter struct {
	intent  LogprobIntent
	carry   []byte
	scanned int
	failed  bool
}

func NewStreamRewriter(intent LogprobIntent) *StreamRewriter { return &StreamRewriter{intent: intent} }

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
		out.Write(rewriteEvent(r.carry[eventStart:eventEnd], r.intent))
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
	final := rewriteEvent(carry, r.intent)
	if final == nil {
		return nil, ErrStreamTruncatedEvent
	}
	return final, nil
}

// rewriteEvent returns the event as the client must read it: a complete chat.completion becomes the
// chunk events an OpenAI streaming client renders, and the fields this client must not see are removed
// from the JSON payload. It returns nil when the payload does not parse and must be dropped rather
// than forwarded.
//
// Both decisions are taken on the decoded payload, never on the event's raw bytes. A host controls
// those bytes: it can spell a key with a \u escape, or split one object across two data lines, and
// either defeats a byte-wise check while the client's own decoder reads the object whole. See
// gateway-request-filtering.md, "The response side".
func rewriteEvent(event []byte, intent LogprobIntent) []byte {
	lines, payload, held := eventPayload(event)
	if !held {
		return event
	}
	filtered, outcome := stripInternalFields(payload, intent)
	switch outcome {
	case stripMalformed:
		// A payload that opens as an object and does not parse is a host sending something no client
		// can read; forwarding it would carry whatever it hides.
		if bytes.HasPrefix(bytes.TrimLeft(payload, " \t"), []byte("{")) {
			return nil
		}
		return event
	case stripUnchanged:
		filtered = payload
	}
	if chunks, converted := completionAsChunks(filtered); converted {
		return chunks
	}
	if outcome != stripRewritten && len(lines) == 1 {
		return event
	}
	return rebuildEvent(event, filtered)
}

// eventPayload joins the event's data lines the way a client does -- with a newline, per the SSE spec
// -- and reports where they were. One object split across two lines reaches the client as one object,
// so it must reach the strip as one too.
func eventPayload(event []byte) (dataLines []int, payload []byte, held bool) {
	var joined []byte
	for offset := 0; offset < len(event); {
		line, lineEnd := event[offset:], len(event)
		if breakAt := bytes.IndexByte(line, '\n'); breakAt >= 0 {
			line, lineEnd = line[:breakAt], offset+breakAt+1
		}
		data, isData := bytes.CutPrefix(bytes.TrimRight(line, "\r"), sseDataParsePrefix)
		if isData {
			dataLines = append(dataLines, offset)
			if len(joined) > 0 {
				joined = append(joined, '\n')
			}
			joined = append(joined, bytes.TrimLeft(data, " \t")...)
		}
		offset = lineEnd
	}
	if len(dataLines) == 0 {
		return nil, nil, false
	}
	return dataLines, joined, true
}

// rebuildEvent emits the event with its data lines replaced by the payload, keeping every other line
// where it was: a client reads event, id and retry from the lines around the data.
func rebuildEvent(event, payload []byte) []byte {
	rewritten := make([]byte, 0, len(event)+len(payload))
	emitted := false
	for offset := 0; offset < len(event); {
		line, lineEnd := event[offset:], len(event)
		if breakAt := bytes.IndexByte(line, '\n'); breakAt >= 0 {
			line, lineEnd = line[:breakAt], offset+breakAt+1
		}
		if _, isData := bytes.CutPrefix(bytes.TrimRight(line, "\r"), sseDataParsePrefix); !isData {
			rewritten = append(rewritten, event[offset:lineEnd]...)
			offset = lineEnd
			continue
		}
		if !emitted {
			// One data line per segment, the inverse of the join eventPayload did. A payload written
			// after a single prefix would put its embedded newlines at the start of lines carrying no
			// data: prefix, and a client drops those -- it would rejoin a truncated object.
			for index, segment := range bytes.Split(payload, []byte("\n")) {
				if index > 0 {
					rewritten = append(rewritten, '\n')
				}
				rewritten = append(rewritten, sseDataPrefix...)
				rewritten = append(rewritten, segment...)
			}
			rewritten = append(rewritten, event[offset+len(line):lineEnd]...)
			emitted = true
		}
		offset = lineEnd
	}
	return rewritten
}

// sseCompletion is the complete chat.completion some hosts answer a streaming request with, and
// sseChunk is the chat.completion.chunk an OpenAI streaming client actually reads.
// Every field the host controls is carried raw. Decoding one into a typed field lets a host fail the
// conversion with a value of the wrong type -- a numeric id, a created past the float range -- and the
// client then reads a message where it renders a delta while the nonce is settled all the same.
type sseCompletion struct {
	ID                json.RawMessage       `json:"id"`
	Created           json.RawMessage       `json:"created"`
	Model             json.RawMessage       `json:"model"`
	SystemFingerprint json.RawMessage       `json:"system_fingerprint,omitempty"`
	Choices           []sseCompletionChoice `json:"choices"`
	Usage             json.RawMessage       `json:"usage"`
}

type sseCompletionChoice struct {
	Index        json.RawMessage            `json:"index"`
	Message      map[string]json.RawMessage `json:"message"`
	FinishReason json.RawMessage            `json:"finish_reason"`
	StopReason   json.RawMessage            `json:"stop_reason"`
}

type sseChunk struct {
	ID                json.RawMessage  `json:"id"`
	Object            string           `json:"object"`
	Created           json.RawMessage  `json:"created"`
	Model             json.RawMessage  `json:"model"`
	SystemFingerprint json.RawMessage  `json:"system_fingerprint,omitempty"`
	Choices           []sseChunkChoice `json:"choices"`
	Usage             json.RawMessage  `json:"usage,omitempty"`
}

type sseChunkChoice struct {
	Index        json.RawMessage            `json:"index"`
	Delta        map[string]json.RawMessage `json:"delta"`
	FinishReason json.RawMessage            `json:"finish_reason"`
	StopReason   json.RawMessage            `json:"stop_reason,omitempty"`
}

// completionAsChunks converts a complete chat.completion into the chunk events a streaming client
// renders. A host that answers a stream with a whole response hands the client a message where it
// reads a delta, so the client renders nothing while the nonce is settled and the money is spent.
// The role, the payload and the finish reason travel as separate chunks, as a real stream sends them.
func completionAsChunks(payload []byte) ([]byte, bool) {
	// The standard library here, as in stripInternalFields: goccy parses a number token even into a raw
	// message and errors past the float64 range, so a created of 1e999 would fail the conversion.
	var completion sseCompletion
	if stdjson.Unmarshal(payload, &completion) != nil {
		return nil, false
	}
	var events bytes.Buffer
	for _, choice := range completion.Choices {
		if choice.Message == nil {
			continue
		}
		if role, named := choice.Message["role"]; named {
			emitChunk(&events, completion, []sseChunkChoice{{
				Index: rawOr(choice.Index, "0"),
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
			emitChunk(&events, completion, []sseChunkChoice{{Index: rawOr(choice.Index, "0"), Delta: delta}}, nil)
		}
		if stopReason := presentValue(choice.StopReason); presentValue(choice.FinishReason) != nil || stopReason != nil {
			emitChunk(&events, completion, []sseChunkChoice{{
				Index:        rawOr(choice.Index, "0"),
				Delta:        map[string]json.RawMessage{},
				FinishReason: rawOr(choice.FinishReason, "null"),
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
	var encoded bytes.Buffer
	encoder := stdjson.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false) // the default inflates every < > & in generated content to six bytes
	if err := encoder.Encode(sseChunk{
		ID:                rawOr(completion.ID, `""`),
		Object:            chunkObject,
		Created:           rawOr(completion.Created, "0"),
		Model:             rawOr(completion.Model, `""`),
		SystemFingerprint: completion.SystemFingerprint,
		Choices:           choices,
		Usage:             usage,
	}); err != nil {
		return
	}
	events.Write(sseDataPrefix)
	events.Write(bytes.TrimRight(encoded.Bytes(), "\n"))
	events.Write(sseEventSeparator)
}

// rawOr keeps a field the host sent verbatim; an absent one takes the fallback, since an empty raw
// message is not JSON and would fail the encode this conversion exists to produce. The fallbacks are
// the zero values the typed fields used to encode, so an ordinary response converts byte for byte.
func rawOr(raw json.RawMessage, fallback string) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(fallback)
	}
	return raw
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
