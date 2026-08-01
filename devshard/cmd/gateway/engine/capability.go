package engine

import (
	"strings"

	"devshard/cmd/gateway/filters"
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
	if strings.Contains(message, filters.ToolChoiceUnsupportedMessage) {
		return CapabilitySignal{ToolsUnsupported: true}
	}
	contextLimit, contextRequested := filters.CapabilityLimits(message)
	return CapabilitySignal{ContextLimit: contextLimit, ContextRequested: contextRequested}
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

// GrowContextHint raises the token count the next Pick screens hosts against. See
// gateway-speculative-race.md, "An SSE error event counts as a chunk but never crowns".
func GrowContextHint(hint uint64, signal CapabilitySignal) uint64 {
	if signal.ContextRequested > hint {
		return signal.ContextRequested
	}
	return hint
}
