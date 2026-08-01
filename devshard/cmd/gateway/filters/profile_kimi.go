package filters

// kimi's thinking_token_budget resolution constants.
const (
	kimiMaxTokensFloor                uint64 = 16
	kimiThinkingBudgetDivisor         uint64 = 2
	kimiThinkingBudgetAbsoluteMax     uint64 = 96_000
	kimiThinkingBudgetForceZeroBelow  uint64 = 256
	kimiThinkingBudgetContentHeadroom uint64 = 64
)

// kimiProfile is Kimi's delta set. See gateway-request-filtering.md, "Model profiles".
var kimiProfile = &Profile{
	Models:                 []string{kimiModelID},
	MaxTokensFloor:         kimiMaxTokensFloor,
	ForceZeroPenalties:     true,
	RejectStructuredOutput: true,
	AllowSafetyIdentifier:  true,
	Thinking:               ThinkingMirrorToKwargs,
	ThinkingTokenBudget:    true,
}
