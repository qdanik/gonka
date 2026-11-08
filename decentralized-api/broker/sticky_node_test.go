package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"decentralized-api/apiconfig"
	"decentralized-api/mlnodeclient"

	"github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
)

const stickyTestModel = "model1"

func registerInferenceNode(t *testing.T, b *Broker, node apiconfig.InferenceNodeConfig) {
	t.Helper()
	register := NewRegisterNodeCommand(node)
	queueMessage(t, b, register)
	<-register.Response

	mlNode := types.MLNodeInfo{NodeId: node.Id, PocWeight: 10, TimeslotAllocation: []bool{true, false}}
	b.UpdateNodeEpochData([]*types.MLNodeInfo{&mlNode}, stickyTestModel, types.Model{Id: stickyTestModel})

	factory := b.mlNodeClientFactory.(*mlnodeclient.MockClientFactory)
	client := factory.CreateClient(fmt.Sprintf("http://%s:%d", node.Host, node.PoCPort), fmt.Sprintf("http://%s:%d", node.Host, node.InferencePort)).(*mlnodeclient.MockClient)
	client.Mu.Lock()
	client.CurrentState = mlnodeclient.MlNodeState_INFERENCE
	client.InferenceIsHealthy = true
	client.Mu.Unlock()

	inferenceUp := NewInferenceUpAllCommand()
	queueMessage(t, b, inferenceUp)
	<-inferenceUp.Response

	setStatus := NewSetNodesActualStatusCommand([]StatusUpdate{{
		NodeId:          node.Id,
		RegistrationSeq: nodeRegistrationSeq(t, b, node.Id),
		PrevStatus:      types.HardwareNodeStatus_UNKNOWN,
		NewStatus:       types.HardwareNodeStatus_INFERENCE,
		Timestamp:       time.Now(),
	}})
	queueMessage(t, b, setStatus)
	<-setStatus.Response

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := b.GetNodes()
		require.NoError(t, err)
		for _, n := range nodes {
			if n.Node.Id == node.Id && n.State.IntendedStatus == types.HardwareNodeStatus_INFERENCE &&
				n.State.CurrentStatus == types.HardwareNodeStatus_INFERENCE && n.State.ReconcileInfo == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s did not reach INFERENCE status within 2s", node.Id)
}

func newStickyTestBroker(t *testing.T, nodeCount int, maxConcurrent int) *Broker {
	t.Helper()
	b := NewTestBroker()
	for i := 0; i < nodeCount; i++ {
		registerInferenceNode(t, b, apiconfig.InferenceNodeConfig{
			Id: fmt.Sprintf("node-%d", i), Host: "localhost", InferencePort: 8080 + i,
			InferenceSegment: "/v1", PoCPort: 9080 + i,
			Models:        map[string]apiconfig.ModelConfig{stickyTestModel: {}},
			MaxConcurrent: maxConcurrent,
		})
	}
	return b
}

func acquireNode(t *testing.T, b *Broker, sessionKey string, skip ...string) (lockID, nodeID string) {
	t.Helper()
	lockID, _, nodeID, err := b.AcquireMLNode(context.Background(), stickyTestModel, skip, sessionKey)
	require.NoError(t, err)
	return lockID, nodeID
}

func acquireAndRelease(t *testing.T, b *Broker, sessionKey string, skip ...string) string {
	t.Helper()
	lockID, nodeID := acquireNode(t, b, sessionKey, skip...)
	require.NoError(t, b.ReleaseMLNode(lockID, InferenceSuccess{}))
	return nodeID
}

func lockCounts(b *Broker) map[string]int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	counts := make(map[string]int, len(b.nodes))
	for id, node := range b.nodes {
		counts[id] = node.State.LockCount
	}
	return counts
}

func setLockCount(b *Broker, nodeID string, count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[nodeID].State.LockCount = count
}

func setNodeEnabled(b *Broker, nodeID string, enabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[nodeID].State.AdminState = AdminState{Enabled: enabled, Epoch: 99}
}

// Test flow:
// 1. Register four nodes and, for forty session keys, acquire and release the node thirty times.
// 2. Assert every acquire of a key returns the node its first acquire returned.
// 3. Assert the forty keys together use more than one node.
// 4. Build a second broker with the same nodes and assert it maps every key to the same node.
func TestStickySession_SameKeyLandsOnSameNode(t *testing.T) {
	b := newStickyTestBroker(t, 4, 2)

	homes := make(map[string]string)
	distinctNodes := make(map[string]struct{})
	for k := 0; k < 40; k++ {
		key := fmt.Sprintf("conversation-%d", k)
		homes[key] = acquireAndRelease(t, b, key)
		distinctNodes[homes[key]] = struct{}{}
		for i := 0; i < 30; i++ {
			require.Equal(t, homes[key], acquireAndRelease(t, b, key), "key %s moved on acquire %d", key, i)
		}
	}
	require.Greater(t, len(distinctNodes), 1)

	restarted := newStickyTestBroker(t, 4, 2)
	for key, home := range homes {
		require.Equal(t, home, acquireAndRelease(t, restarted, key), "key %s differs after restart", key)
	}
}

// Test flow:
// 1. Register three nodes with three slots each and find the home node of a key.
// 2. Fill the home node with three acquires of the key and assert each lock count rose by exactly one.
// 3. Give the runner-up node by rendezvous rank a lock and acquire the key again with the home node full.
// 4. Assert the answer is the idle other node, which is the least-busy choice.
// 5. Release one home lock and assert the key returns to its home node.
func TestStickySession_FallsBackToLeastBusyWhenHomeIsFull(t *testing.T) {
	b := newStickyTestBroker(t, 3, 3)
	home := acquireAndRelease(t, b, "busy-conversation")

	var homeLockID string
	for i := 1; i <= 3; i++ {
		lockID, nodeID := acquireNode(t, b, "busy-conversation")
		require.Equal(t, home, nodeID)
		require.Equal(t, i, lockCounts(b)[home])
		homeLockID = lockID
	}

	var others []string
	for id := range lockCounts(b) {
		if id != home {
			others = append(others, id)
		}
	}
	require.Len(t, others, 2)
	if rendezvousScore("busy-conversation", others[1]) > rendezvousScore("busy-conversation", others[0]) {
		others[0], others[1] = others[1], others[0]
	}
	setLockCount(b, others[0], 1)

	_, fallbackNodeID := acquireNode(t, b, "busy-conversation")
	require.Equal(t, others[1], fallbackNodeID)
	require.Equal(t, 3, lockCounts(b)[home])

	require.NoError(t, b.ReleaseMLNode(homeLockID, InferenceSuccess{}))
	require.Equal(t, home, acquireAndRelease(t, b, "busy-conversation"))
}

// Test flow:
// 1. Register one node with one slot, lock it and assert a keyed acquire still answers ErrNoNodesAvailable.
func TestStickySession_NoCapacityStillRefusesWithoutQueueing(t *testing.T) {
	b := newStickyTestBroker(t, 1, 1)
	acquireNode(t, b, "conversation")

	_, _, _, err := b.AcquireMLNode(context.Background(), stickyTestModel, nil, "conversation")

	require.ErrorIs(t, err, ErrNoNodesAvailable)
}

// Test flow:
// 1. Register four nodes and find the home node of a key.
// 2. Exclude the home node, then disable it, and in each case acquire the key several times.
// 3. Assert the answer is never the home node and is the same node every time.
// 4. Lift the exclusion or re-enable the node and assert the key returns to its home node.
func TestStickySession_MovesWhenHomeIsExcludedOrDisabled(t *testing.T) {
	cases := []struct {
		name  string
		start func(b *Broker, home string) []string
		end   func(b *Broker, home string)
	}{
		{
			name:  "excluded",
			start: func(_ *Broker, home string) []string { return []string{home} },
			end:   func(_ *Broker, _ string) {},
		},
		{
			name: "disabled",
			start: func(b *Broker, home string) []string {
				setNodeEnabled(b, home, false)
				return nil
			},
			end: func(b *Broker, home string) { setNodeEnabled(b, home, true) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newStickyTestBroker(t, 4, 2)
			home := acquireAndRelease(t, b, "conversation-a")

			skip := tc.start(b, home)
			moved := acquireAndRelease(t, b, "conversation-a", skip...)
			require.NotEqual(t, home, moved)
			for i := 0; i < 5; i++ {
				require.Equal(t, moved, acquireAndRelease(t, b, "conversation-a", skip...))
			}

			tc.end(b, home)
			require.Equal(t, home, acquireAndRelease(t, b, "conversation-a"))
		})
	}
}

// Test flow:
// 1. Register four nodes with two slots each and acquire four times with an empty key without releasing.
// 2. Assert every node holds exactly one lock, the least-busy spread.
// 3. Release all, then acquire and release two hundred times with an empty key.
// 4. Assert more than one node was used, so ties are still broken without a key.
// 5. Acquire twice with one session key and assert both land on the same node, which then holds two locks.
func TestStickySession_EmptyKeyKeepsLeastBusyDistribution(t *testing.T) {
	b := newStickyTestBroker(t, 4, 2)

	var lockIDs []string
	for i := 0; i < 4; i++ {
		lockID, _ := acquireNode(t, b, "")
		lockIDs = append(lockIDs, lockID)
	}
	for id, count := range lockCounts(b) {
		require.Equal(t, 1, count, "node %s", id)
	}
	for _, lockID := range lockIDs {
		require.NoError(t, b.ReleaseMLNode(lockID, InferenceSuccess{}))
	}

	usedNodes := make(map[string]struct{})
	for i := 0; i < 200; i++ {
		usedNodes[acquireAndRelease(t, b, "")] = struct{}{}
	}
	require.Greater(t, len(usedNodes), 1)

	_, first := acquireNode(t, b, "conversation")
	_, second := acquireNode(t, b, "conversation")
	require.Equal(t, first, second)
	require.Equal(t, 2, lockCounts(b)[first])
}
