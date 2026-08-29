package filters

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"

	json "github.com/goccy/go-json"
)

const (
	// MaxStreamCarryBytes bounds the unterminated tail held per stream. See README.md, "SSE framing".
	MaxStreamCarryBytes = 32 << 20

	// chunkObject tells a streaming client to read choices[].delta instead of choices[].message.
	chunkObject = "chat.completion.chunk"
)

var (
	sseEventSeparator     = []byte("\n\n")
	sseEventSeparatorCRLF = []byte("\r\n\r\n")
	sseDataPrefix         = []byte("data: ")
	sseDoneMarker         = []byte("[DONE]")

	// sseDataParsePrefix deliberately omits the space sseDataPrefix emits. See README.md, "Two `data:` prefixes that must not be unified".
	sseDataParsePrefix = []byte("data:")

	// usageKey spares every other event a decode it has nothing to gain from.
	usageKey = []byte(`"usage"`)

	// SSEDoneEvent is the terminator an SSE client reads until; without it the client waits out its own timeout.
	SSEDoneEvent = []byte("data: [DONE]\n\n")

	// NoResponseDataBody is the reply a non-streaming caller gets when the stream carried no payload.
	NoResponseDataBody = []byte(`{"error":{"message":"no response data"}}`)

	// TruncatedResponseBody replaces a fold past maxAssembledEvents; the prefix alone would look complete.
	TruncatedResponseBody = []byte(`{"error":{"message":"response exceeded the assembler's event budget"}}`)

	// ErrStreamCarryOverflow reports an unterminated SSE event larger than MaxStreamCarryBytes.
	ErrStreamCarryOverflow = errors.New("sse event exceeds carry limit")
	// ErrStreamTruncatedEvent reports a trailing partial event dropped at end of stream.
	ErrStreamTruncatedEvent = errors.New("sse stream ended mid-event")
)

// eachSSELine visits every "data:" line's trimmed payload, terminator included, stopping when visit says so.
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

// EachSSEDataPayload visits every "data:" payload, skipping empty lines and [DONE].
func EachSSEDataPayload(events []byte, visit func(payload []byte) bool) {
	eachSSELine(events, func(payload []byte) bool {
		if len(payload) == 0 || bytes.Equal(payload, sseDoneMarker) {
			return false
		}
		return visit(payload)
	})
}

// HasSSEDone is line-anchored, so a "[DONE]" inside a content delta is not read as the terminator.
func HasSSEDone(events []byte) bool {
	terminated := false
	eachSSELine(events, func(payload []byte) bool {
		terminated = bytes.Equal(payload, sseDoneMarker)
		return terminated
	})
	return terminated
}

// StreamRewriter strips the hidden fields from an SSE stream, emitting complete events only.
type StreamRewriter struct {
	intent    LogprobIntent
	keepUsage bool
	carry     []byte
	scanned   int
	failed    bool
}

// NewStreamRewriter drops the forced usage event unless keepUsage says the client asked for it.
func NewStreamRewriter(intent LogprobIntent, keepUsage bool) *StreamRewriter {
	return &StreamRewriter{intent: intent, keepUsage: keepUsage}
}

// Write returns every event chunk completes, rewritten; past MaxStreamCarryBytes the rewriter fails permanently.
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
		rewritten, _ := rewriteEvent(r.carry[eventStart:eventEnd], r.intent, r.keepUsage)
		out.Write(rewritten)
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

// Close rewrites a well-formed trailing partial, and drops an unparseable one with ErrStreamTruncatedEvent.
func (r *StreamRewriter) Close() ([]byte, error) {
	carry := r.carry
	r.carry, r.scanned = nil, 0
	if r.failed {
		return nil, ErrStreamCarryOverflow
	}
	if len(carry) == 0 {
		return nil, nil
	}
	final, malformed := rewriteEvent(carry, r.intent, r.keepUsage)
	if malformed {
		return nil, ErrStreamTruncatedEvent
	}
	return final, nil
}

// rewriteEvent returns the event as the client must read it, deciding on the decoded payload and never on the host-controlled raw bytes. See README.md, "Rewriting an event".
func rewriteEvent(event []byte, intent LogprobIntent, keepUsage bool) (rewritten []byte, malformed bool) {
	lines, payload, held := eventPayload(event)
	if !held {
		return event, false
	}
	filtered, outcome := stripInternalFields(payload, intent)
	switch outcome {
	case stripMalformed:
		// A payload that opens as an object and does not parse would carry whatever it hides.
		if bytes.HasPrefix(bytes.TrimLeft(payload, " \t"), []byte("{")) {
			return nil, true
		}
		return event, false
	case stripUnchanged:
		filtered = payload
	}
	if !keepUsage {
		withoutUsage, emptied, changed := stripUsage(filtered)
		if emptied {
			return nil, false
		}
		if changed {
			filtered, outcome = withoutUsage, stripRewritten
		}
	}
	if chunks, converted := completionAsChunks(filtered); converted {
		return chunks, false
	}
	if outcome != stripRewritten && len(lines) == 1 {
		return event, false
	}
	return rebuildEvent(event, filtered), false
}

// chunkHousekeepingFields are what the final usage event carries besides its usage.
var chunkHousekeepingFields = map[string]bool{
	"id": true, "object": true, "created": true, "model": true, "system_fingerprint": true,
	"service_tier": true, "choices": true,
}

// onlyHousekeepingLeft: testing for empty choices instead would delete a host's error event, which carries none either.
func onlyHousekeepingLeft(decoded map[string]any) bool {
	for field := range decoded {
		if !chunkHousekeepingFields[field] {
			return false
		}
	}
	choices, _ := decoded["choices"].([]any)
	return len(choices) == 0
}

// stripUsage removes a usage the client never asked for, reporting emptied for an event left with none.
func stripUsage(payload []byte) (rewritten []byte, emptied, changed bool) {
	if !bytes.Contains(payload, usageKey) {
		return payload, false, false
	}
	decoded, parsed := decodeStreamedEvent(payload)
	if !parsed {
		return payload, false, false
	}
	if _, held := decoded["usage"]; !held {
		return payload, false, false
	}
	delete(decoded, "usage")
	if onlyHousekeepingLeft(decoded) {
		return nil, true, true
	}
	encoded, err := encodeCompact(decoded)
	if err != nil {
		return payload, false, false
	}
	return encoded, false, true
}

// eventPayload joins the event's data lines with a newline as a client does, so a split object reaches the strip whole.
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

// rebuildEvent replaces the data lines and keeps every other one: a client reads event, id and retry from them.
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
			// One data line per segment, the inverse of eventPayload's join: a client drops continuation lines with no data: prefix.
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

// sseCompletion carries every host-controlled field raw: a typed field lets a host fail the conversion with a wrong type.
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
	Logprobs     json.RawMessage            `json:"logprobs"`
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
	Logprobs     json.RawMessage            `json:"logprobs,omitempty"`
	FinishReason json.RawMessage            `json:"finish_reason"`
	StopReason   json.RawMessage            `json:"stop_reason,omitempty"`
}

// completionAsChunks converts a complete chat.completion into the chunk events a streaming client renders. See README.md, "A complete reply on a streaming request is rewritten into chunks".
func completionAsChunks(payload []byte) ([]byte, bool) {
	// Standard library again: goccy errors past the float64 range even into a raw message.
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
			// The whole answer arrives as one delta, so the logprobs for it ride the same chunk.
			emitChunk(&events, completion, []sseChunkChoice{{
				Index:    rawOr(choice.Index, "0"),
				Delta:    delta,
				Logprobs: presentValue(choice.Logprobs),
			}}, nil)
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

// encodeCompact drops HTML escaping, which would inflate every < > & to six bytes; only the encoder can turn it off.
func encodeCompact(value any) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := stdjson.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(encoded.Bytes(), "\n"), nil
}

func emitChunk(events *bytes.Buffer, completion sseCompletion, choices []sseChunkChoice, usage json.RawMessage) {
	encoded, err := encodeCompact(sseChunk{
		ID:                rawOr(completion.ID, `""`),
		Object:            chunkObject,
		Created:           rawOr(completion.Created, "0"),
		Model:             rawOr(completion.Model, `""`),
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

// rawOr keeps a host's field verbatim; an absent one takes the fallback, since an empty raw message fails the encode.
func rawOr(raw json.RawMessage, fallback string) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(fallback)
	}
	return raw
}

// presentValue counts JSON null as absent, so a field the host spelled out as null is not re-sent.
func presentValue(raw json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return trimmed
}

// forEachSSEEvent visits each complete event and then whatever trailing bytes carried no terminator.
func forEachSSEEvent(stream []byte, visit func(event []byte) bool) {
	for rest := stream; len(rest) > 0; {
		offset := indexEventEnd(rest)
		if offset < 0 {
			visit(rest)
			return
		}
		if visit(rest[:offset]) {
			return
		}
		rest = rest[offset:]
	}
}

// indexEventEnd returns the offset past the first LF or CRLF terminator, or -1; it walks line by line to stay linear.
func indexEventEnd(buf []byte) int {
	for offset := 0; offset < len(buf); {
		lineEnd := bytes.IndexByte(buf[offset:], '\n')
		if lineEnd < 0 {
			return -1
		}
		next := offset + lineEnd + 1
		switch {
		case next < len(buf) && buf[next] == '\n':
			return next + 1
		case next+1 < len(buf) && buf[next] == '\r' && buf[next+1] == '\n':
			return next + 2
		}
		offset = next
	}
	return -1
}
