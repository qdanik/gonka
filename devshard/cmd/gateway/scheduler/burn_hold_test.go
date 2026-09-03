package scheduler

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// A burn commits a nonce, so a retirement landing mid-commit is barred as it is for a serve.
func TestABurnTakesTheEscrowHold(t *testing.T) {
	var held, released atomic.Int64
	test := newHarness(t, harnessConfig{
		escrowHold: func() (func(), bool) { held.Add(1); return func() { released.Add(1) }, true },
	})
	stale := test.clock.Now().Add(-2 * matchWaitWindow)

	queued := test.submit(t, stale, hostB)
	awaitReply(t, queued)
	test.dispatcher.stop()

	require.Equal(t, []string{ghostExclude.reason()}, test.observer.burns(), "the fixture must produce a burn")
	require.Equal(t, int64(2), held.Load(), "the burn and the serve each hold the escrow while they commit")
	require.Equal(t, int64(1), released.Load(), "the burn gives its hold back at once; the serve keeps its own")
}

func TestABurnOnARetiredEscrowCommitsNothing(t *testing.T) {
	test := newHarness(t, harnessConfig{
		escrowHold: func() (func(), bool) { return nil, false },
	})
	stale := test.clock.Now().Add(-2 * matchWaitWindow)

	queued := test.submit(t, stale, hostB)
	result := awaitReply(t, queued)
	test.dispatcher.stop()

	require.ErrorIs(t, result.err, ErrEscrowGone, "a retired escrow answers the waiter rather than burning")
	require.Empty(t, test.observer.burns(), "no nonce may be burned on an escrow already out of service")
	_, _, commits := test.session.report()
	require.Empty(t, commits, "a retired escrow commits nothing at all")
}
