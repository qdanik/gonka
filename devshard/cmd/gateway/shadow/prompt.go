package shadow

import (
	"fmt"
	"strings"
	"sync"
)

// TokenCounter counts the prompt tokens of a chat request body for model; known is false while the model's tokenizer is unavailable.
type TokenCounter interface {
	Tokens(model string, body []byte) (count uint64, known bool)
}

type prompts struct {
	counter TokenCounter
	mu      sync.Mutex
	fitted  map[string][]byte
}

func newPrompts(counter TokenCounter) *prompts {
	return &prompts{counter: counter, fitted: map[string][]byte{}}
}

func (p *prompts) body(model string) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cached, found := p.fitted[model]; found {
		return cached
	}
	text, counted := p.fit(model)
	encoded := fmt.Appendf(nil, probeBodyFormat, text, ProbeMaxTokens)
	if counted {
		p.fitted[model] = encoded
	}
	return encoded
}

func (p *prompts) fit(model string) (text string, counted bool) {
	if p.counter == nil {
		return fallbackText(), false
	}
	words := ProbeInputTokens
	for range fitAttempts {
		text = strings.Repeat(probeWord, words)
		count, known := p.counter.Tokens(model, fmt.Appendf(nil, countedBodyFormat, text))
		if !known || count == 0 {
			return fallbackText(), false
		}
		if count == ProbeInputTokens {
			return text, true
		}
		words = max(1, words*ProbeInputTokens/int(count))
	}
	return strings.Repeat(probeWord, words), true
}

func fallbackText() string {
	return strings.Repeat(probeWord, fallbackBytes)[:fallbackBytes]
}
