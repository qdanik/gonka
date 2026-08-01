package filters

// minimaxProfile is MiniMax's delta set. See gateway-request-filtering.md, "Model profiles".
var minimaxProfile = &Profile{
	Models:             []string{minimaxModelID},
	Thinking:           ThinkingStrip,
	KeepReasoningSplit: true,
}
