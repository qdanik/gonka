package engine

import (
	"strconv"
	"strings"
)

const (
	toolChoiceUnsupportedPhrase = "tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"
	contextLimitPhrase          = "maximum context length is "
	contextRequestedPhrase      = "for a total of at least "
)

type CapabilitySignal struct {
	ToolsUnsupported bool
	ContextLimit     uint64
	ContextRequested uint64
}

// Retriable reports a refusal another host may still satisfy, so the race must neither crown the
// attempt nor end on it.
func (s CapabilitySignal) Retriable() bool {
	return s.ToolsUnsupported || s.ContextLimit > 0
}

func ParseCapabilityError(message string) CapabilitySignal {
	if strings.Contains(message, toolChoiceUnsupportedPhrase) {
		return CapabilitySignal{ToolsUnsupported: true}
	}
	return CapabilitySignal{
		ContextLimit:     uintAfterPhrase(message, contextLimitPhrase),
		ContextRequested: uintAfterPhrase(message, contextRequestedPhrase),
	}
}

func CapabilityOf(a AttemptOutcome) CapabilitySignal {
	if a.ErrorSource == "" || a.ErrorMessage == "" {
		return CapabilitySignal{}
	}
	return ParseCapabilityError(a.ErrorMessage)
}

type CapabilityRecorder interface {
	RecordContextLimit(participant string, maxTokens uint64)
	RecordToolUnsupported(participant string)
}

func RecordCapability(recorder CapabilityRecorder, participant string, signal CapabilitySignal) {
	if recorder == nil || participant == "" {
		return
	}
	switch {
	case signal.ToolsUnsupported:
		recorder.RecordToolUnsupported(participant)
	case signal.ContextLimit > 0:
		recorder.RecordContextLimit(participant, signal.ContextLimit)
	}
}

// GrowContextHint raises the token count the next Pick screens hosts against, so a retry skips
// every host already known to be too small for this request.
func GrowContextHint(hint uint64, signal CapabilitySignal) uint64 {
	if signal.ContextRequested > hint {
		return signal.ContextRequested
	}
	return hint
}

// Both the search and the slice run on the lowered copy: lowercasing can shorten a string (U+212A
// lowers to a one-byte k), and an index taken from one string but applied to the other lands mid-word.
func uintAfterPhrase(message, phrase string) uint64 {
	lowered := strings.ToLower(message)
	_, after, ok := strings.Cut(lowered, phrase)
	if !ok {
		return 0
	}
	digits := after
	end := strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' })
	if end < 0 {
		end = len(digits)
	}
	if end == 0 {
		return 0
	}
	parsed, err := strconv.ParseUint(digits[:end], 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}
