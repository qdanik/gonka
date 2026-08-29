package filters

// Options carries NormalizeRequest's per-call configuration. A set ModelTokenLimits overrides the global
// token limits for the routed model, and a zero value it returns leaves the corresponding global one alone.
type Options struct {
	Admin            bool
	KeepClientStream bool
	DefaultMaxTokens uint64
	MaxTokensCap     uint64
	RoutedModel      string
	ModelTokenLimits func(model string) (defaultMaxTokens, maxTokensCap uint64)
}

// Result is the normalized body plus the typed fields callers need without re-parsing it. Body asks the
// host to stream unless KeepClientStream turned that off; ClientStream and ClientUsage are what the
// caller asked for.
type Result struct {
	Body                []byte
	Model               string
	ClientStream        bool
	ClientUsage         bool
	RequiresTools       bool
	MaxTokens           uint64
	MaxCompletionTokens uint64
	Logprobs            LogprobIntent
}

func NormalizeRequest(body []byte, options Options) (Result, error) {
	document, err := ParseDocument(body)
	if err != nil {
		return Result{}, err
	}
	return runPipeline(document, options)
}
