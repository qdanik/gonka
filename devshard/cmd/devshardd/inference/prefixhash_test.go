package inference

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"common/chain"
	nmgen "common/nodemanager/gen"
	"devshard"

	"github.com/stretchr/testify/require"
)

type chatMessage = map[string]any

func chatBody(t *testing.T, messages ...chatMessage) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"model": "m", "messages": messages})
	require.NoError(t, err)
	return body
}

// Test flow:
//  1. Build chat bodies whose block hashes were computed by the Python xxhash package (v4.1.0) over the text, as role then content per message, in 256-byte blocks chained through the little-endian previous hash.
//  2. Hash each body: expect the pinned hashes, with the first, second and last pinned when there are many.
//  3. Cover a part list with an image part, a block edge cut inside a multibyte character, JSON escapes, a prompt one byte short of a block, and a prompt of 513 blocks that keeps 512.
func TestPromptPrefixHashesMatchTheIndependentComputation(t *testing.T) {
	tests := []struct {
		name   string
		body   []byte
		count  int
		pinned map[int]uint64
	}{
		{
			name:   "two full blocks and a dropped tail",
			body:   chatBody(t, chatMessage{"role": "system", "content": strings.Repeat("x", 250)}, chatMessage{"role": "user", "content": strings.Repeat("y", 300)}),
			count:  2,
			pinned: map[int]uint64{0: 0x5cbe50d3004274af, 1: 0xd1993985e5af048d},
		},
		{
			name: "text parts are joined and other parts are skipped",
			body: chatBody(t, chatMessage{"role": "user", "content": []chatMessage{
				{"type": "text", "text": strings.Repeat("p", 130)},
				{"type": "image_url", "image_url": chatMessage{"url": "data:image/png;base64,AAAA"}},
				{"type": "text", "text": strings.Repeat("q", 130)},
			}}),
			count:  1,
			pinned: map[int]uint64{0: 0x545b960f3157f550},
		},
		{
			name:   "block edge inside a multibyte character",
			body:   chatBody(t, chatMessage{"role": "admin", "content": strings.Repeat("€", 100)}),
			count:  1,
			pinned: map[int]uint64{0: 0xd304b663c7762584},
		},
		{
			name:   "escaped characters hash as their decoded text",
			body:   chatBody(t, chatMessage{"role": "user", "content": strings.Repeat("line1\nsay \"hi\" \\ ü", 30)}),
			count:  2,
			pinned: map[int]uint64{0: 0x1e89271068eee6d4, 1: 0x4d2e9836b98170c0},
		},
		{
			name:   "exactly one block",
			body:   chatBody(t, chatMessage{"role": "user", "content": strings.Repeat("k", 252)}),
			count:  1,
			pinned: map[int]uint64{0: 0xa5c00b860a5e4cc5},
		},
		{
			name:  "one byte short of a block",
			body:  chatBody(t, chatMessage{"role": "user", "content": strings.Repeat("k", 251)}),
			count: 0,
		},
		{
			name:   "longer than the cap keeps the first 512",
			body:   chatBody(t, chatMessage{"role": "user", "content": strings.Repeat("z", 256*513)}),
			count:  512,
			pinned: map[int]uint64{0: 0xeaad99a3987d3820, 1: 0xa79df30da6b5ed5d, 511: 0x9110230e08d8d3ba},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashes := promptPrefixHashes(tt.body)
			require.Len(t, hashes, tt.count)
			for index, want := range tt.pinned {
				require.Equal(t, want, hashes[index], "hash %d", index)
			}
		})
	}
}

// Test flow:
//  1. Hash bodies that are empty, not JSON, a JSON array, without messages and with an empty message list.
//  2. Expect no hashes and no panic for each.
func TestPromptPrefixHashesAreEmptyWhenThereIsNothingToHash(t *testing.T) {
	for _, body := range []string{``, `not json`, `[1,2]`, `{}`, `{"messages":[]}`, `{"messages":"x"}`, `{"messages":[{"role":"user","content":null}]}`} {
		require.Empty(t, promptPrefixHashes([]byte(body)), body)
	}
}

// Test flow:
//  1. Encode the two hashes of the two-block vector.
//  2. Expect the padded standard base64 of their little-endian bytes, as the Python base64 module produced it, and the empty string for no hashes.
func TestPrefixHashesHeaderIsPaddedBase64OfLittleEndianWords(t *testing.T) {
	require.Equal(t, "r3RCANNQvlyNBK/lhTmZ0Q==", prefixHashesHeader([]uint64{0x5cbe50d3004274af, 0xd1993985e5af048d}))
	require.Empty(t, prefixHashesHeader(nil))
}

// Test flow:
//  1. Start a node manager recording each Acquire and an ML node recording its request headers; every request runs on its own engine, as the payload store keeps one prompt per epoch.
//  2. Execute a request with a session key and a two-block prompt: expect the key and both hashes on the Acquire and on the ML node request.
//  3. Execute a request with no session key and a one-line prompt: expect no hashes on the Acquire and neither header on the ML node request.
//  4. Execute a request whose session key holds a newline: expect it to succeed with no X-Session-Key header on the ML node request.
func TestAnExecutionSendsItsSessionKeyAndPrefixHashesToDapiAndTheMLNode(t *testing.T) {
	var mu sync.Mutex
	var headers []http.Header
	var acquired []*nmgen.AcquireMLNodeRequest
	mlNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers = append(headers, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":1}}`)
	}))
	t.Cleanup(mlNode.Close)
	ml := startEngineMLClient(t, &engineMockNM{
		acquireFunc: func(_ context.Context, req *nmgen.AcquireMLNodeRequest) (*nmgen.AcquireMLNodeResponse, error) {
			mu.Lock()
			acquired = append(acquired, req)
			mu.Unlock()
			return &nmgen.AcquireMLNodeResponse{LockId: "lock", NodeId: "node", Endpoint: mlNode.URL}, nil
		},
	})
	phase := new(chain.Phase)
	phase.SetEpoch(5)
	engine := func() *Engine {
		return NewEngine(ml, nil, nil, &memoryPayloads{}, fixedChainParams{}, phase, true, nil)
	}

	key := strings.Repeat("ab", 32)
	long := recoveryRequest(t, devshard.RecoveryNone, 5)
	long.SessionKey = key
	long.Prompt = chatBody(t, chatMessage{"role": "system", "content": strings.Repeat("x", 250)}, chatMessage{"role": "user", "content": strings.Repeat("y", 300)})
	canonical, err := devshard.CanonicalizeJSON(long.Prompt)
	require.NoError(t, err)
	promptHash := sha256.Sum256(canonical)
	long.PromptHash = promptHash[:]
	_, err = engine().Execute(context.Background(), long)
	require.NoError(t, err)

	short := recoveryRequest(t, devshard.RecoveryNone, 5)
	short.InferenceID = 2
	_, err = engine().Execute(context.Background(), short)
	require.NoError(t, err)

	badKey := recoveryRequest(t, devshard.RecoveryNone, 5)
	badKey.InferenceID = 3
	badKey.SessionKey = strings.Repeat("ab", 31) + "a\n"
	_, err = engine().Execute(context.Background(), badKey)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, acquired, 3)
	require.Equal(t, key, acquired[0].SessionKey)
	require.Equal(t, []uint64{0x5cbe50d3004274af, 0xd1993985e5af048d}, acquired[0].PrefixBlockHashes)
	require.Empty(t, acquired[1].PrefixBlockHashes)
	require.Len(t, headers, 3)
	require.Equal(t, key, headers[0].Get("X-Session-Key"))
	require.Equal(t, "r3RCANNQvlyNBK/lhTmZ0Q==", headers[0].Get("X-Prefix-Hashes"))
	require.Empty(t, headers[1].Values("X-Session-Key"))
	require.Empty(t, headers[1].Values("X-Prefix-Hashes"))
	require.Empty(t, headers[2].Values("X-Session-Key"))
}
