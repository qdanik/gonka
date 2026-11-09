package inference

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"common/completionapi"
	nmgen "common/nodemanager/gen"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	convATurn1 = `{"model":"m","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"plan a trip to Lisbon"}]}`
	convATurn2 = `{"model":"m","seed":7,"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"plan a trip to Lisbon"},{"role":"assistant","content":"Day 1: Alfama."},{"role":"user","content":"add a day"}]}`
	convATurn3 = `{"messages":[{"content":"be brief","role":"system"},{"content":"plan a trip to Lisbon","role":"user"},{"role":"assistant","content":"rewritten reply"},{"role":"user","content":"add a day"},{"role":"assistant","content":"Day 2: Belem."},{"role":"user","content":"now Porto"}],"model":"m"}`
	convB      = `{"model":"m","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"explain TCP slow start"}]}`
	convC      = `{"model":"m","messages":[{"role":"system","content":"answer in French"},{"role":"user","content":"plan a trip to Lisbon"}]}`
)

// Test flow:
//  1. Derive keys for three turns of one conversation: appended messages, reordered JSON keys, a rewritten assistant reply.
//  2. Assert all three turns share one 64-character key.
//  3. Assert the body posted after ModifyRequestBodyWithLogprobsMode keeps the key.
//  4. Derive keys for a conversation with another first user message and one with another system prompt.
//  5. Assert each differs from the first conversation and from each other.
func TestSessionKey_StablePerConversationDistinctAcross(t *testing.T) {
	keyA := sessionKey([]byte(convATurn1))
	require.Len(t, keyA, 64)
	assert.Equal(t, keyA, sessionKey([]byte(convATurn2)))
	assert.Equal(t, keyA, sessionKey([]byte(convATurn3)))

	modified, err := completionapi.ModifyRequestBodyWithLogprobsMode([]byte(convATurn2), 7, "")
	require.NoError(t, err)
	assert.Equal(t, keyA, sessionKey(modified.NewBody))

	keyB := sessionKey([]byte(convB))
	keyC := sessionKey([]byte(convC))
	require.Len(t, keyB, 64)
	require.Len(t, keyC, 64)
	assert.NotEqual(t, keyA, keyB)
	assert.NotEqual(t, keyA, keyC)
	assert.NotEqual(t, keyB, keyC)
}

// Test flow:
//  1. Derive keys for bodies with invalid JSON, no messages, an empty message list and no user message.
//  2. Assert each key is empty so dapi keeps its least-busy choice.
func TestSessionKey_EmptyWithoutOpening(t *testing.T) {
	for name, body := range map[string]string{
		"invalid json":    `{"messages":`,
		"no messages":     `{"model":"m","prompt":"hi"}`,
		"empty messages":  `{"model":"m","messages":[]}`,
		"no user message": `{"model":"m","messages":[{"role":"system","content":"be brief"}]}`,
	} {
		assert.Empty(t, sessionKey([]byte(body)), name)
	}
}

// Test flow:
//  1. Start a fake node manager that records SessionKey on every AcquireMLNode, and an ML node that fails the first POST with 500.
//  2. Execute turn 1 of a conversation (one retry), then turn 2, then another conversation, through Engine.executeMLRequest.
//  3. Validate turn 3 of the first conversation through Validator.executeMLRequest.
//  4. Assert every Acquire for the first conversation carried its key, including the retry and the validation, and the other conversation carried a different key.
func TestExecuteMLRequest_SendsSessionKeyOnAcquire(t *testing.T) {
	var posts atomic.Int32
	mlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if posts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(mlSrv.Close)

	var mu sync.Mutex
	var keys []string
	ml := startEngineMLClient(t, &engineMockNM{
		acquireFunc: func(_ context.Context, req *nmgen.AcquireMLNodeRequest) (*nmgen.AcquireMLNodeResponse, error) {
			mu.Lock()
			keys = append(keys, req.SessionKey)
			n := len(keys)
			mu.Unlock()
			return &nmgen.AcquireMLNodeResponse{
				LockId:   "lock",
				Endpoint: mlSrv.URL,
				NodeId:   fmt.Sprintf("node-%d", n),
			}, nil
		},
	})
	eng := newTestEngine(ml, nil, nil)
	val := &Validator{engine: eng}

	run := func(do func() (*http.Response, error)) {
		resp, err := do()
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	ctx := context.Background()
	run(func() (*http.Response, error) { return eng.executeMLRequest(ctx, "m", "42", []byte(convATurn1)) })
	run(func() (*http.Response, error) { return eng.executeMLRequest(ctx, "m", "42", []byte(convATurn2)) })
	run(func() (*http.Response, error) { return eng.executeMLRequest(ctx, "m", "42", []byte(convB)) })
	run(func() (*http.Response, error) { return val.executeMLRequest(ctx, "m", "43", []byte(convATurn3)) })

	keyA := sessionKey([]byte(convATurn1))
	keyB := sessionKey([]byte(convB))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{keyA, keyA, keyA, keyB, keyA}, keys)
}
