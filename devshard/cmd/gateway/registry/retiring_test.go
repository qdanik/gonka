package registry

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/types"
)

// The reading taken as an escrow retires is the last one there will ever be.
func TestARetiringEscrowIsReadBeforeItsSessionCloses(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.escrowState = types.EscrowState{LatestNonce: 7}
	var closedWhenRead int64
	var readNonce uint64
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Retiring: func(_ string, retiring EscrowSession) {
			closedWhenRead = session.closeCalls.Load()
			readNonce = retiring.SnapshotState().LatestNonce
		},
		Now: fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	require.NoError(t, registry.Retire("1"))

	require.Equal(t, uint64(7), readNonce, "the retiring escrow's own session never reached the observer")
	require.Zero(t, closedWhenRead, "the session was already closed when the observer read it")
	require.Equal(t, int64(1), session.closeCalls.Load())
}

// A retirement with a request still running is not over until the last release.
func TestADrainingEscrowIsReadOnlyWhenItsLastRequestHasEnded(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.escrowState = types.EscrowState{LatestNonce: 7}
	var readMu sync.Mutex
	var readNonces []uint64
	read := func() []uint64 {
		readMu.Lock()
		defer readMu.Unlock()
		return append([]uint64(nil), readNonces...)
	}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Retiring: func(_ string, retiring EscrowSession) {
			readMu.Lock()
			defer readMu.Unlock()
			readNonces = append(readNonces, retiring.SnapshotState().LatestNonce)
		},
		Now: fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	require.True(t, acquired)

	require.NoError(t, registry.Retire("1"))
	require.Empty(t, read(), "the escrow was read while a request was still spending nonces on it")

	session.escrowState = types.EscrowState{LatestNonce: 9}
	release()
	awaitDrainClose(t, registry, func() bool { return len(read()) == 1 })

	require.Equal(t, []uint64{9}, read(), "the reading did not wait for the last request to end")
}

// Every reconciliation retires each inactive escrow again.
func TestRetiringAnEscrowThatIsNotRoutableReadsNothing(t *testing.T) {
	t.Parallel()
	reads := 0
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Retiring:        func(string, EscrowSession) { reads++ },
		Now:             fixedClock(),
	})

	require.NoError(t, registry.Retire("1"))

	require.Zero(t, reads, "an escrow that was never published was read as if it were retiring")
}
