package registry

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingEscrowNarrator struct {
	mu    sync.Mutex
	calls []string
}

func (n *recordingEscrowNarrator) note(format string, values ...any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, fmt.Sprintf(format, values...))
}

func (n *recordingEscrowNarrator) recorded() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.calls...)
}

func (n *recordingEscrowNarrator) EscrowServing(escrowID, model string) {
	n.note("serving %s %s", escrowID, model)
}

func (n *recordingEscrowNarrator) EscrowRetired(escrowID string) { n.note("retired %s", escrowID) }

func (n *recordingEscrowNarrator) EscrowRetiredDraining(escrowID string, inFlight int64) {
	n.note("retired draining %s in flight %d", escrowID, inFlight)
}

func (n *recordingEscrowNarrator) DrainingEscrowClosed(escrowID string, closeErr error) {
	n.note("closed %s error %v", escrowID, closeErr)
}

func (n *recordingEscrowNarrator) RetirementPendingFlushed(escrowID string, pending int, err error) {
	n.note("flushed %s pending %d error %v", escrowID, pending, err)
}

func (n *recordingEscrowNarrator) SettlementUnverifiable(escrowID string, nonce uint64, _ error) {
	n.note("unverifiable %s nonce %d", escrowID, nonce)
}

func TestPublishingAndRetiringAnIdleEscrowIsNarrated(t *testing.T) {
	t.Parallel()
	narrator := &recordingEscrowNarrator{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Narrator:        narrator,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	require.NoError(t, registry.Retire("1"))

	require.Equal(t, []string{"serving 1 qwen", "retired 1"}, narrator.recorded())
}

// A retirement with a request still running is not over until the last release closes the session.
func TestADrainingEscrowIsNarratedUntilItCloses(t *testing.T) {
	t.Parallel()
	narrator := &recordingEscrowNarrator{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Narrator:        narrator,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	require.True(t, acquired)
	require.NoError(t, registry.Retire("1"))

	release()
	awaitDrainClose(t, func() bool { return len(narrator.recorded()) == 3 })

	require.Equal(t, []string{"serving 1 qwen", "retired draining 1 in flight 1", "closed 1 error <nil>"}, narrator.recorded())
}

// Nobody is left to hand this failure to; the narration and the counter are the only records of it.
func TestADrainingEscrowThatFailsToCloseIsNarratedWithTheFailure(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.closeErr = errors.New("storage refused to close")
	narrator := &recordingEscrowNarrator{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Narrator:        narrator,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	require.True(t, acquired)
	require.NoError(t, registry.Retire("1"))

	release()
	awaitDrainClose(t, func() bool { return len(narrator.recorded()) == 3 })

	require.Equal(t, []string{
		"serving 1 qwen", "retired draining 1 in flight 1", "closed 1 error closing escrow 1: storage refused to close",
	}, narrator.recorded())
	require.Equal(t, int64(1), registry.DrainCloseFailures())
}

// A registry built without a narrator publishes and retires all the same.
func TestARegistryWithoutANarratorStillRetires(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	require.NoError(t, registry.Retire("1"))

	require.Equal(t, int64(1), session.closeCalls.Load())
}
