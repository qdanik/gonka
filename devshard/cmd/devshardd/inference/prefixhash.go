package inference

import (
	"context"
	"encoding/base64"
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

const (
	prefixBlockBytes = 256
	maxPrefixHashes  = 512
)

// prefixRoute is what dapi and the ML node need to pick a node that already holds the prompt's KV prefix.
type prefixRoute struct {
	sessionKey string
	hashes     []uint64
}

var noRoute prefixRoute

// messagesPrefixHashes returns the chained xxhash64 of each full 256-byte block of the chat prompt,
// at most 512 of them. The prompt text is every message's role followed by its content, in order;
// content is a string or the "text" parts of a part list. It returns nil when the request has no messages.
func messagesPrefixHashes(requestMap map[string]interface{}) []uint64 {
	messages, _ := requestMap["messages"].([]interface{})
	if len(messages) == 0 {
		return nil
	}
	blocks := newBlockHasher()
	for _, raw := range messages {
		message, _ := raw.(map[string]interface{})
		role, _ := message["role"].(string)
		if !blocks.write(role) || !blocks.writeContent(message["content"]) {
			break
		}
	}
	return blocks.hashes
}

type prefixHashesKey struct{}

// withPrefixHashes carries the hashes from the request's one decode to executeMLRequest.
func withPrefixHashes(ctx context.Context, hashes []uint64) context.Context {
	if len(hashes) == 0 {
		return ctx
	}
	return context.WithValue(ctx, prefixHashesKey{}, hashes)
}

func prefixHashesFrom(ctx context.Context) []uint64 {
	hashes, _ := ctx.Value(prefixHashesKey{}).([]uint64)
	return hashes
}

// blockHasher cuts a byte stream into full blocks and chains their hashes.
type blockHasher struct {
	digest *xxhash.Digest
	block  [prefixBlockBytes]byte
	filled int
	prev   [8]byte
	hashes []uint64
}

func newBlockHasher() *blockHasher {
	return &blockHasher{digest: xxhash.New()}
}

// write appends text and reports whether more blocks are still wanted.
func (b *blockHasher) write(text string) bool {
	for len(text) > 0 && len(b.hashes) < maxPrefixHashes {
		n := copy(b.block[b.filled:], text)
		b.filled += n
		text = text[n:]
		if b.filled == prefixBlockBytes {
			b.seal()
		}
	}
	return len(b.hashes) < maxPrefixHashes
}

// writeContent appends a string content, or the "text" of each "text" part of a part list, and reports whether more blocks are still wanted.
func (b *blockHasher) writeContent(content interface{}) bool {
	switch typed := content.(type) {
	case string:
		return b.write(typed)
	case []interface{}:
		for _, raw := range typed {
			part, _ := raw.(map[string]interface{})
			if part["type"] != "text" {
				continue
			}
			text, _ := part["text"].(string)
			if !b.write(text) {
				return false
			}
		}
	}
	return len(b.hashes) < maxPrefixHashes
}

func (b *blockHasher) seal() {
	b.digest.Reset()
	if len(b.hashes) > 0 {
		_, _ = b.digest.Write(b.prev[:])
	}
	_, _ = b.digest.Write(b.block[:])
	sum := b.digest.Sum64()
	binary.LittleEndian.PutUint64(b.prev[:], sum)
	b.hashes = append(b.hashes, sum)
	b.filled = 0
}

// validSessionKeyHeader reports whether key is the 64 lowercase hex characters the ML node proxy accepts;
// the key comes from the peer, so anything else (control characters included) is not put in a header.
func validSessionKeyHeader(key string) bool {
	if len(key) != 64 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if c := key[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// prefixHashesHeader is the X-Prefix-Hashes value: the hashes as little-endian uint64s in padded standard base64.
func prefixHashesHeader(hashes []uint64) string {
	raw := make([]byte, 0, len(hashes)*8)
	for _, hash := range hashes {
		raw = binary.LittleEndian.AppendUint64(raw, hash)
	}
	return base64.StdEncoding.EncodeToString(raw)
}
