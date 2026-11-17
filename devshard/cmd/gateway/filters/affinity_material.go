package filters

import (
	stdjson "encoding/json"
)

// MaxAffinityLeadingBytes caps the leading messages a host-affinity key reads, so a huge system prompt is not marshalled whole; NoPromptCacheKey is what a request without a usable prompt_cache_key reads as.
const (
	MaxAffinityLeadingBytes = 64 << 10
	NoPromptCacheKey        = ""
)

// AffinityMaterial is what a host-affinity key is derived from: the client's prompt_cache_key, else the leading messages.
type AffinityMaterial struct {
	PromptCacheKey  string
	LeadingMessages []byte
}

func readPromptCacheKey(document *Document) string {
	raw, present := document.Get("prompt_cache_key")
	if !present {
		return NoPromptCacheKey
	}
	key, isString := raw.(string)
	if !isString {
		return NoPromptCacheKey
	}
	return key
}

func leadingMessages(document *Document) []byte {
	raw, present := document.Get("messages")
	messages, isList := raw.([]any)
	if !present || !isList {
		return nil
	}
	var leading []byte
	for _, message := range messages {
		encoded, err := stdjson.Marshal(message)
		if err != nil {
			return nil
		}
		leading = append(leading, encoded...)
		if len(leading) >= MaxAffinityLeadingBytes {
			return leading[:MaxAffinityLeadingBytes]
		}
		if fields, isObject := message.(map[string]any); isObject && fields["role"] == "user" {
			break
		}
	}
	return leading
}
