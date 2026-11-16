package shadow

import (
	"math/rand/v2"

	"common/completionapi"

	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/scheduler"
	"devshard/user"
)

// Params repeats the client request in request on the shadow host, asking for as many output tokens as the draw gives. See README.md.
func (r *Runner) Params(request any, startedAt int64) (any, scheduler.ProbeSkip) {
	client, isInference := request.(user.InferenceParams)
	if !isInference {
		return nil, scheduler.ProbeSkipNoRequest
	}
	tokens := drawOutputTokens()
	body, skip := repeatedBody(client.Prompt, tokens)
	if skip != scheduler.ProbeSkipNone {
		return nil, skip
	}
	repeated := client
	repeated.Prompt = body
	repeated.InputLength = uint64(len(body))
	repeated.MaxTokens = tokens
	repeated.StartedAt = startedAt
	return repeated, scheduler.ProbeSkipNone
}

func drawOutputTokens() uint64 {
	return uint64(ProbeMinOutputTokens + rand.IntN(ProbeMaxOutputTokens-ProbeMinOutputTokens+1))
}

func carriesTools(document *filters.Document) bool {
	tools, present := document.Array(fieldTools)
	return present && len(tools) > 0
}

func repeatedBody(body []byte, tokens uint64) ([]byte, scheduler.ProbeSkip) {
	document, err := filters.ParseDocument(body)
	if err != nil {
		return nil, scheduler.ProbeSkipNoRequest
	}
	if completionapi.GrammarConstrained(document.Raw()) || carriesTools(document) {
		return nil, scheduler.ProbeSkipGrammarConstrained
	}
	document.Set(fieldMaxTokens, tokens)
	document.Set(fieldMinTokens, tokens)
	if document.Has(fieldMaxCompletionTokens) {
		document.Set(fieldMaxCompletionTokens, tokens)
	}
	document.Set(fieldStream, true)
	document.Set(fieldStreamOptions, map[string]any{fieldIncludeUsage: true})
	repeated, err := document.Marshal()
	if err != nil {
		return nil, scheduler.ProbeSkipNoRequest
	}
	return repeated, scheduler.ProbeSkipNone
}
