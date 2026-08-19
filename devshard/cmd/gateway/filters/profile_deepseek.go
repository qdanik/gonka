package filters

// An omitted reasoning_effort renders as "high"; "max" is the strongest prefix the encoder defines.
const deepseekReasoningEffortDefault = "max"

var deepseekProfile = &Profile{
	Models:                 []string{deepseekModelID},
	ReasoningEffortDefault: deepseekReasoningEffortDefault,
}
