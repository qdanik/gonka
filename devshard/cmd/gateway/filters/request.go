package filters

// Options carries NormalizeRequest's per-call configuration: admin bypass, token limits, routed model.
type Options struct {
	Admin            bool
	DefaultMaxTokens uint64
	MaxTokensCap     uint64
	RoutedModel      string
}

// Result is the normalized body plus the typed fields callers need without re-parsing it.
type Result struct {
	Body                []byte
	Model               string
	Stream              bool
	MaxTokens           uint64
	MaxCompletionTokens uint64
	N                   uint64
}

// NormalizeRequest is the package's public entry point.
func NormalizeRequest(body []byte, options Options) (Result, error) {
	document, err := ParseDocument(body)
	if err != nil {
		return Result{}, err
	}
	return runPipeline(document, options)
}
