package affinity

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	kindPromptCacheKey byte = 'k'
	kindLeadingPrompt  byte = 'p'
	lengthPrefixBytes       = 8
)

// KeyFor derives the key of one request: the client's prompt_cache_key when it sent one, else the leading messages, always under the caller's own namespace and the model. See routing.md, "Host affinity".
func KeyFor(namespace, model, promptCacheKey string, leadingMessages []byte) Key {
	switch {
	case len(promptCacheKey) > 0:
		return digest(namespace, model, kindPromptCacheKey, []byte(promptCacheKey))
	case len(leadingMessages) > 0:
		return digest(namespace, model, kindLeadingPrompt, leadingMessages)
	}
	return NoKey
}

func digest(namespace, model string, kind byte, content []byte) Key {
	hasher := sha256.New()
	for _, field := range []string{namespace, model} {
		var length [lengthPrefixBytes]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		hasher.Write(length[:])
		hasher.Write([]byte(field))
	}
	hasher.Write([]byte{kind})
	hasher.Write(content)
	return Key(hasher.Sum(nil))
}
