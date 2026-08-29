package filters

// minimaxProfile is MiniMax's delta set. See README.md, "Model profiles".
var minimaxProfile = &Profile{
	Models:             []string{minimaxModelID},
	Thinking:           ThinkingStrip,
	KeepReasoningSplit: true,
}
