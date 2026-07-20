package filters

// minimaxProfile: no chat-template knob for enable_thinking/thinking (reasoning is
// interleaved and structural to the template); reasoning_split is a native passthrough field.
var minimaxProfile = &Profile{
	Models:             []string{minimaxModelID},
	Thinking:           ThinkingStrip,
	KeepReasoningSplit: true,
}
