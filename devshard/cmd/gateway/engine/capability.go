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

// Retriable reports a refusal another host may not repeat: a tool call or protocol version the answering host's build lacks.
func (s CapabilitySignal) Retriable() bool {
	return s.ToolsUnsupported || s.VersionUnsupported
}

// Refused reports any refusal the gateway recognises, retriable or not.
func (s CapabilitySignal) Refused() bool {
	return s.Retriable() || s.ContextLimit > 0
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

func CapabilityOf(attempt AttemptOutcome) CapabilitySignal {
	if attempt.Capability.Refused() {
		return attempt.Capability
	}
	if attempt.ErrorSource == "" || attempt.ErrorMessage == "" {
		return CapabilitySignal{}
	}
	return ParseCapabilityError(attempt.ErrorMessage)
}

// rulesOutRetry reports a trusted host's refusal that is not Retriable, which the race takes as every host's. See race.md, "Escalation".
func rulesOutRetry(attempt AttemptOutcome) bool {
	refusal := CapabilityOf(attempt)
	return !attempt.Suspicious && refusal.Refused() && !refusal.Retriable()
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
