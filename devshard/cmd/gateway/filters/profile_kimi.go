package filters

// kimi's thinking_token_budget resolution constants.
const (
	kimiMaxTokensFloor                uint64 = 16
	kimiThinkingBudgetDivisor         uint64 = 2
	kimiThinkingBudgetAbsoluteMax     uint64 = 96_000
	kimiThinkingBudgetForceZeroBelow  uint64 = 256
	kimiThinkingBudgetContentHeadroom uint64 = 64
)

// kimiProfile: thinking is chat-template-only (mirrored, never sent top-level), penalties are
// fixed at 0 on the wire, and structured_outputs has no route (use response_format instead).
var kimiProfile = &Profile{
	Models:                 []string{kimiModelID},
	MaxTokensFloor:         kimiMaxTokensFloor,
	ForceZeroPenalties:     true,
	RejectStructuredOutput: true,
	AllowSafetyIdentifier:  true,
	Thinking:               ThinkingMirrorToKwargs,
	ThinkingTokenBudget:    true,
}
