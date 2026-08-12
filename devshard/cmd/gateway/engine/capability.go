package engine

import (
	"errors"
	"strings"

	"devshard/cmd/gateway/filters"
	"devshard/transport"
)

// A host too old for the escrow's protocol version answers `version "v3" not found`. Both markers are
// required: the same status also carries `escrow not found`, which a teardown produces and waiting fixes.
const (
	versionRefusalSubject = `version "`
	versionRefusalVerdict = `" not found`
)

type CapabilitySignal struct {
	ToolsUnsupported   bool
	VersionUnsupported bool
	ContextLimit       uint64
	ContextRequested   uint64
}

// Retriable reports a refusal another host may still satisfy, so the race must neither crown the
// attempt nor end on it.
func (s CapabilitySignal) Retriable() bool {
	return s.ToolsUnsupported || s.VersionUnsupported || s.ContextLimit > 0
}

// ParseVersionRefusal reads the body a host returns when its build does not speak the protocol version
// the escrow runs on. Waiting cannot fix it, so it is a capability rather than a fault to back off from.
func ParseVersionRefusal(body string) CapabilitySignal {
	if !strings.Contains(body, versionRefusalSubject) || !strings.Contains(body, versionRefusalVerdict) {
		return CapabilitySignal{}
	}
	return CapabilitySignal{VersionUnsupported: true}
}

func ParseCapabilityError(message string) CapabilitySignal {
	if strings.Contains(message, filters.ToolChoiceUnsupportedMessage) {
		return CapabilitySignal{ToolsUnsupported: true}
	}
	contextLimit, contextRequested := filters.CapabilityLimits(message)
	return CapabilitySignal{ContextLimit: contextLimit, ContextRequested: contextRequested}
}

// capabilityOfDispatchError reads a refusal the host returned instead of a stream, where no SSE error
// event exists to carry it.
func capabilityOfDispatchError(err error) CapabilitySignal {
	var status *transport.UpstreamStatusError
	if !errors.As(err, &status) {
		return CapabilitySignal{}
	}
	return ParseVersionRefusal(status.Body)
}

func CapabilityOf(a AttemptOutcome) CapabilitySignal {
	if a.Capability.Retriable() {
		return a.Capability
	}
	if a.ErrorSource == "" || a.ErrorMessage == "" {
		return CapabilitySignal{}
	}
	return ParseCapabilityError(a.ErrorMessage)
}

type CapabilityRecorder interface {
	RecordContextLimit(participant string, maxTokens uint64)
	RecordToolUnsupported(participant string)
	RecordVersionUnsupported(participant string)
}

func RecordCapability(recorder CapabilityRecorder, participant string, signal CapabilitySignal) {
	if recorder == nil || participant == "" {
		return
	}
	switch {
	case signal.ToolsUnsupported:
		recorder.RecordToolUnsupported(participant)
	case signal.VersionUnsupported:
		recorder.RecordVersionUnsupported(participant)
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
