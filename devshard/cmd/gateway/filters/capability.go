package filters

import (
	"strconv"
	"strings"
)

// The phrases vLLM emits for a capability refusal, verbatim. See README.md, "Capability errors".
const (
	ToolChoiceUnsupportedMessage = "tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"
	contextLimitPhrase           = "maximum context length is "
	contextRequestedPhrase       = "for a total of at least "
)

// CapabilityLimits reads the context window and the tokens needed from a vLLM refusal; 0 when absent.
func CapabilityLimits(message string) (contextLimit, contextRequested uint64) {
	return uintAfterPhrase(message, contextLimitPhrase), uintAfterPhrase(message, contextRequestedPhrase)
}

// Search and slice both run on the lowered copy: lowercasing can shorten a string, so a mixed index lands mid-word.
func uintAfterPhrase(message, phrase string) uint64 {
	lowered := strings.ToLower(message)
	_, digits, found := strings.Cut(lowered, phrase)
	if !found {
		return 0
	}
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
