package warmup

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/registry"
	"devshard/user"
)

type recordingWarmupNarrator struct {
	calls         []string
	probeErrors   []error
	catchUpErrors []error
}

func (n *recordingWarmupNarrator) WarmupFoundNoNonce(escrowID string, probeErr error) {
	n.calls = append(n.calls, "no nonce "+escrowID)
	n.probeErrors = append(n.probeErrors, probeErr)
}

func (n *recordingWarmupNarrator) EscrowWarmed(escrowID, model string, nonce uint64, served bool, catchUpErr error) {
	n.calls = append(n.calls, fmt.Sprintf("warmed %s %s nonce %d served %t", escrowID, model, nonce, served))
	n.catchUpErrors = append(n.catchUpErrors, catchUpErr)
}

func (n *recordingWarmupNarrator) WarmupLedgerOpenFailed(escrowID string, err error) {
	n.calls = append(n.calls, fmt.Sprintf("ledger open failed %s: %v", escrowID, err))
}

func (n *recordingWarmupNarrator) WarmupVoted(escrowID string, nonce uint64, action, reason string) {
	n.calls = append(n.calls, fmt.Sprintf("voted %s nonce %d %s %s", escrowID, nonce, action, reason))
}

// A probe that committed nothing hands over the error it had, and nil when it had none; the journal decides what that renders.
func TestAProbeThatCommittedNothingIsNarratedWithTheErrorItHad(t *testing.T) {
	narrator := &recordingWarmupNarrator{}
	warmup := &Prober{
		escrows: &stubEscrows{session: stubSession{}, live: true},
		probe: func(context.Context, registry.EscrowSession, user.InferenceParams, func()) (uint64, bool, error) {
			return 0, false, nil
		},
		catchUp: func(context.Context, registry.EscrowSession) error { return nil },
		stop:    make(chan struct{}),
		now:     warmupClock(),
	}
	warmup.SetNarrator(narrator)

	warmup.warm("escrow-1", "model-a")

	require.Equal(t, []string{"no nonce escrow-1"}, narrator.calls)
	require.Equal(t, []error{nil}, narrator.probeErrors)
}

func TestAServedProbeIsNarratedAsWarmed(t *testing.T) {
	narrator := &recordingWarmupNarrator{}
	warmup, _, _ := newWarmupUnderTest(stubSession{}, nil)
	warmup.SetNarrator(narrator)

	warmup.warm("escrow-1", "model-a")

	require.Equal(t, []string{"warmed escrow-1 model-a nonce 1 served true"}, narrator.calls)
	require.Equal(t, []error{nil}, narrator.catchUpErrors)
}

// The vote is narrated with its action, which is what the journal reads to write a failed vote at Warn.
func TestAFailedWarmupVoteIsNarratedWithItsAction(t *testing.T) {
	narrator := &recordingWarmupNarrator{}
	warmup := refusedWarmup(&stubPoster{vote: "refused", err: errors.New("collect timeout votes")}, &spyTimeouts{}, true)
	warmup.SetNarrator(narrator)

	warmup.warm("escrow-1", "model-a")

	require.Equal(t, []string{
		"voted escrow-1 nonce 7 failed timeout_collection_error",
		"warmed escrow-1 model-a nonce 7 served false",
	}, narrator.calls)
}

func TestALedgerThatRefusesTheEscrowIsNarrated(t *testing.T) {
	narrator := &recordingWarmupNarrator{}
	warmup := &Prober{ledger: &spyLedger{openRefusal: errors.New("ledger closed")}, epochs: stubEpochs{epoch: 3}, now: warmupClock()}
	warmup.SetNarrator(narrator)

	warmup.openLedger("escrow-1", "model-a", stubSession{})

	require.Equal(t, []string{"ledger open failed escrow-1: ledger closed"}, narrator.calls)
}

// main binds the narrator whether or not warming is on, and a warmup that is off is nil.
func TestBindingANarratorToAnAbsentWarmupDoesNothing(t *testing.T) {
	var warmup *Prober

	require.NotPanics(t, func() { warmup.SetNarrator(&recordingWarmupNarrator{}) })
}
