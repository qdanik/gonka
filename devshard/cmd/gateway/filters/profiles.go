package filters

import "slices"

// ThinkingDisposition is the closed set of ways a profile handles the thinking/enable_thinking
// fields: normalize in place, mirror into chat_template_kwargs, or strip entirely.
type ThinkingDisposition int

const (
	ThinkingNormalizeInPlace ThinkingDisposition = iota // default/Qwen
	ThinkingMirrorToKwargs                              // Kimi
	ThinkingStrip                                       // MiniMax
)

// Profile captures one routed model's deltas from the default parameter pipeline. Rules
// consult these hooks instead of comparing model IDs. A nil *Profile is the default profile
// (e.g. Qwen): no deltas apply.
type Profile struct {
	Models []string

	MaxTokensFloor         uint64
	ForceZeroPenalties     bool
	RejectStructuredOutput bool
	AllowSafetyIdentifier  bool
	Thinking               ThinkingDisposition
	KeepReasoningSplit     bool
	ThinkingTokenBudget    bool
}

// Exact routed-model identifiers the parameter table dispatches on.
const (
	kimiModelID    = "moonshotai/Kimi-K2.6"
	minimaxModelID = "MiniMaxAI/MiniMax-M2.7"
)

var registeredProfiles = []*Profile{kimiProfile, minimaxProfile}

func ProfileFor(routedModel string) *Profile {
	for _, profile := range registeredProfiles {
		if slices.Contains(profile.Models, routedModel) {
			return profile
		}
	}
	return nil
}
