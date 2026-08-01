package filters

import (
	"strconv"
	"strings"
)

// The phrases vLLM emits for a capability refusal, verbatim; matched here and nowhere else. See
// gateway-request-filtering.md, "The vLLM capability-error parser lives here".
const (
	ToolChoiceUnsupportedMessage = "tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"
	contextLimitPhrase           = "maximum context length is "
	contextRequestedPhrase       = "for a total of at least "
)

// CapabilityLimits reads the model's context window and the tokens the request needed from a vLLM
// context refusal; each is 0 when the message does not carry it.
func CapabilityLimits(message string) (contextLimit, contextRequested uint64) {
	return uintAfterPhrase(message, contextLimitPhrase), uintAfterPhrase(message, contextRequestedPhrase)
}

// Both the search and the slice run on the lowered copy: lowercasing can shorten a string (U+212A
// lowers to a one-byte k), and an index taken from one string but applied to the other lands mid-word.
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
