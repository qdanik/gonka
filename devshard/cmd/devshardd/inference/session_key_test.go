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

	"common/chain"
	nmgen "common/nodemanager/gen"
	"devshard"

	"github.com/stretchr/testify/require"
)

type acquiredWith struct {
	sessionKey string
	excluded   int
}

// Test flow:
//  1. Start a node manager that records each Acquire and an ML node that answers 503 once and then a completion.
//  2. Execute a request carrying a session key: expect two acquisitions, the retry excluding the failed node, both with the key.
//  3. Dispatch a validation: expect its acquisition to carry no key, since a validation is started from state and no gateway request reaches it.
//  4. Execute a request with no session key: expect its acquisition to carry none.
func TestTheSessionKeyReachesEveryAcquireOfItsExecution(t *testing.T) {
	var failedOnce atomic.Bool
	mlNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failedOnce.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":1}}`)
	}))
	t.Cleanup(mlNode.Close)
	var mu sync.Mutex
	var acquisitions []acquiredWith
	taken := func() []acquiredWith {
		mu.Lock()
		defer mu.Unlock()
		recorded := acquisitions
		acquisitions = nil
		return recorded
	}
	ml := startEngineMLClient(t, &engineMockNM{
		acquireFunc: func(_ context.Context, req *nmgen.AcquireMLNodeRequest) (*nmgen.AcquireMLNodeResponse, error) {
			mu.Lock()
			acquisitions = append(acquisitions, acquiredWith{sessionKey: req.SessionKey, excluded: len(req.ExcludedNodes)})
			mu.Unlock()
			return &nmgen.AcquireMLNodeResponse{LockId: "lock", NodeId: fmt.Sprint("node-", len(req.ExcludedNodes)), Endpoint: mlNode.URL}, nil
		},
	})
	phase := new(chain.Phase)
	phase.SetEpoch(5)
	engine := NewEngine(ml, nil, nil, &memoryPayloads{}, fixedChainParams{}, phase, true, nil)

	keyed := recoveryRequest(t, devshard.RecoveryNone, 5)
	keyed.SessionKey = "session-digest"
	_, err := engine.Execute(context.Background(), keyed)
	require.NoError(t, err)
	require.Equal(t, []acquiredWith{{sessionKey: "session-digest", excluded: 0}, {sessionKey: "session-digest", excluded: 1}}, taken())

	validator := &Validator{engine: engine}
	response, err := validator.executeMLRequest(context.Background(), "m", "e", []byte(`{}`))
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Equal(t, []acquiredWith{{sessionKey: noSessionKey, excluded: 0}}, taken())

	unkeyed := recoveryRequest(t, devshard.RecoveryNone, 5)
	unkeyed.InferenceID = 2
	_, err = engine.Execute(context.Background(), unkeyed)
	require.NoError(t, err)
	require.Equal(t, []acquiredWith{{sessionKey: noSessionKey, excluded: 0}}, taken())
}
