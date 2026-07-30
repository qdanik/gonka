package engine

import (
	"bytes"
	"encoding/json"
	"fmt"

	"devshard/cmd/gateway/filters"
)

var (
	sseDataPrefix = []byte("data:")
	sseDoneMarker = []byte("[DONE]")
	sseUsageKey   = []byte(`"usage"`)
)

type sseError struct {
	Source  string
	Code    string
	Type    string
	Message string
	Payload string
}

func (e sseError) present() bool { return e.Source != "" }

type chunkSignal struct {
	ContentSource         string
	Error                 sseError
	UsageCompletionTokens int64
}

// crownsWinner admits content and nothing else. An error event still counts as a chunk, but a host
// answering instantly with one must never beat a slower host that goes on to produce real tokens.
func (s chunkSignal) crownsWinner() bool { return s.ContentSource != "" }

// A thinking-budget route is the only one whose host-reported completion_tokens may separate a model
// that produced nothing from a host that carried nothing; elsewhere any host could fake usage to
// escape the empty-stream penalty.
func thinkingBudgetRoute(model string) bool {
	profile := filters.ProfileFor(model)
	return profile != nil && profile.ThinkingTokenBudget
}

func classifyChunk(events []byte) chunkSignal {
	var signal chunkSignal
	if source, ok := contentSource(events); ok {
		signal.ContentSource = source
	} else if failure, ok := errorPayload(events); ok {
		signal.Error = failure
	}
	if tokens, ok := usageCompletionTokens(events); ok {
		signal.UsageCompletionTokens = tokens
	}
	return signal
}

// contentSource names the field carrying the first client-renderable output in events. choices[].text
// is excluded: the gateway serves only /v1/chat/completions, where a host emitting it renders nothing.
func contentSource(events []byte) (string, bool) {
	var source string
	eachSSEPayload(events, func(payload []byte) bool {
		var event struct {
			Choices []struct {
				FinishReason string        `json:"finish_reason"`
				Delta        renderedParts `json:"delta"`
				Message      renderedParts `json:"message"`
			} `json:"choices"`
			Usage struct {
				CompletionTokens int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(payload, &event) != nil {
			return false
		}
		for _, choice := range event.Choices {
			if found, ok := choice.Delta.source("delta"); ok {
				source = found
				return true
			}
			if found, ok := choice.Message.source("message"); ok {
				source = found
				return true
			}
			if choice.FinishReason == "stop" && event.Usage.CompletionTokens > 0 {
				source = "message.empty_stop_completion_tokens"
				return true
			}
		}
		return false
	})
	return source, source != ""
}

type renderedParts struct {
	Content          string          `json:"content"`
	Reasoning        string          `json:"reasoning"`
	ReasoningContent string          `json:"reasoning_content"`
	ToolCalls        json.RawMessage `json:"tool_calls"`
}

func (p renderedParts) source(shape string) (string, bool) {
	switch {
	case p.Content != "":
		return shape + ".content", true
	case p.Reasoning != "":
		return shape + ".reasoning", true
	case p.ReasoningContent != "":
		return shape + ".reasoning_content", true
	case jsonArrayHasElements(p.ToolCalls):
		return shape + ".tool_calls", true
	}
	return "", false
}

// errorPayload extracts the first OpenAI-compatible error in events, in both the nested
// {"error":{...}} shape and the flat {"object":"error",...} one vLLM still emits.
func errorPayload(events []byte) (sseError, bool) {
	var found sseError
	eachSSEPayload(events, func(payload []byte) bool {
		var event struct {
			Error *struct {
				Type    string `json:"type"`
				Code    any    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
			Object  string `json:"object"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(payload, &event) != nil {
			return false
		}
		switch {
		case event.Error != nil:
			found = newSSEError(event.Error.Type, event.Error.Code, event.Error.Message, payload)
		case event.Object == "error" && event.Message != "":
			found = newSSEError(event.Type, event.Code, event.Message, payload)
		default:
			return false
		}
		return true
	})
	return found, found.present()
}

func newSSEError(errorType string, code any, message string, payload []byte) sseError {
	source := "error"
	if errorType != "" {
		source = "error." + errorType
	}
	return sseError{
		Source:  source,
		Code:    jsonCodeText(code),
		Type:    errorType,
		Message: message,
		Payload: string(payload),
	}
}

func jsonCodeText(code any) string {
	if code == nil {
		return ""
	}
	return fmt.Sprint(code)
}

// usageCompletionTokens reads usage.completion_tokens, which counts tokens vLLM stripped from
// content. vLLM emits the usage object once per stream, so the key check skips almost every chunk.
func usageCompletionTokens(events []byte) (int64, bool) {
	if !bytes.Contains(events, sseUsageKey) {
		return 0, false
	}
	var tokens int64
	eachSSEPayload(events, func(payload []byte) bool {
		var event struct {
			Usage *struct {
				CompletionTokens int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(payload, &event) != nil {
			return false
		}
		if event.Usage == nil || event.Usage.CompletionTokens <= 0 {
			return false
		}
		tokens = event.Usage.CompletionTokens
		return true
	})
	return tokens, tokens > 0
}

func eachSSEPayload(events []byte, visit func(payload []byte) bool) {
	for _, line := range bytes.Split(events, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if !bytes.HasPrefix(line, sseDataPrefix) {
			continue
		}
		payload := bytes.TrimSpace(line[len(sseDataPrefix):])
		if len(payload) == 0 || bytes.Equal(payload, sseDoneMarker) {
			continue
		}
		if visit(payload) {
			return
		}
	}
}

func jsonArrayHasElements(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return false
	}
	return len(bytes.TrimSpace(trimmed[1:len(trimmed)-1])) > 0
}
