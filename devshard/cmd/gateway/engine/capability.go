package engine

import (
	"errors"
	"regexp"
	"strings"

	"devshard/cmd/gateway/filters"
	"devshard/storage"
	"devshard/transport"
)

var versionNotFound = regexp.MustCompile(`version "[^"]*" not found`)

type CapabilitySignal struct {
	ToolsUnsupported   bool
	VersionUnsupported bool
	ContextLimit       uint64
}

func (s CapabilitySignal) Retriable() bool {
	return s.ToolsUnsupported || s.VersionUnsupported || s.ContextLimit > 0
}

func ParseVersionRefusal(body string) CapabilitySignal {
	if !versionNotFound.MatchString(body) && !strings.Contains(body, storage.ErrSessionVersionConflict.Error()) {
		return CapabilitySignal{}
	}
	return CapabilitySignal{VersionUnsupported: true}
}

func ParseCapabilityError(message string) CapabilitySignal {
	if strings.Contains(message, filters.ToolChoiceUnsupportedMessage) {
		return CapabilitySignal{ToolsUnsupported: true}
	}
	contextLimit, _ := filters.CapabilityLimits(message)
	return CapabilitySignal{ContextLimit: contextLimit}
}

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
	RecordContextLimit(participant, model string, maxTokens uint64)
	RecordToolUnsupported(participant, model string)
	RecordVersionUnsupported(participant string)
}

func RecordCapability(recorder CapabilityRecorder, participant, model string, signal CapabilitySignal) {
	if recorder == nil || participant == "" {
		return
	}
	switch {
	case signal.ToolsUnsupported:
		recorder.RecordToolUnsupported(participant, model)
	case signal.VersionUnsupported:
		recorder.RecordVersionUnsupported(participant)
	case signal.ContextLimit > 0:
		recorder.RecordContextLimit(participant, model, signal.ContextLimit)
	}
}
