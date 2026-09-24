package scheduler

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test flow:
//  1. Build a harness counting escrow holds and releases, and submit a stale request past the match wait window against `hostB`.
//  2. Await the reply and stop the dispatcher.
//  3. Assert the fixture produced the expected ghost-exclude burn.
//  4. Assert the escrow was held twice, once for the burn and once for the serve, and released once, since the burn returns its hold at once.
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

// Test flow:
//  1. Build a harness whose escrow hold always refuses, and submit a stale request past the match wait window against `hostB`.
//  2. Await the reply and stop the dispatcher.
//  3. Assert the waiter's error is `ErrEscrowGone`.
//  4. Assert no burn and no commit were produced for the retired escrow.
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
