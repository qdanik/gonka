package engine

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type heldFinishResponse struct {
	fakeResponse
	releases *atomic.Int32
}

func (r heldFinishResponse) ReleaseFinish() { r.releases.Add(1) }

type provingClassifier struct {
	*fakeClassifier
}

func (c provingClassifier) missProof() (MissProof, bool) {
	return MissProof{ResponsePayload: []byte(`{"events":["data: err"]}`), Complete: true}, true
}

func (c provingClassifier) releaseMissProof() {}

func runHeldFinishAttempt(t *testing.T, facts chunkFacts) (AttemptOutcome, *atomic.Int32) {
	t.Helper()
	releases := &atomic.Int32{}
	dispatch := &fakeDispatcher{receipt: true, chunks: []string{"data: chunk\n\n"}, response: heldFinishResponse{fakeResponse: fakeResponse{confirmed: true}, releases: releases}}
	fixture := newAttemptFixture(dispatch, &fakeClassifier{perChunk: []chunkFacts{facts}})
	fixture.spec.Classifier = provingClassifier{fakeClassifier: fixture.classifier}

	runAttempt(context.Background(), fixture.spec)

	return *doneEvent(t, fixture.drain()).Outcome, releases
}

func TestAnAttemptWithAMissToClaimKeepsItsFinishHeld(t *testing.T) {
	outcome, releases := runHeldFinishAttempt(t, chunkFacts{Error: true, ErrorSource: "sse", ErrorCode: "500", ErrorType: "server_error", ErrorMessage: "boom"})

	require.True(t, outcome.claimsMiss(), "the fixture must end in an error the attempt can prove")
	require.Zero(t, releases.Load(), "the Finish the miss is claimed against must stay out of every diff until the claim is posted")
}

func TestAnAttemptWithNoMissToClaimReleasesItsFinish(t *testing.T) {
	outcome, releases := runHeldFinishAttempt(t, contentFacts("content"))

	require.False(t, outcome.claimsMiss())
	require.Equal(t, int32(1), releases.Load(), "an answered attempt's Finish must reach the next diff")
}

type releaseWatchingPoster struct {
	failingPoster
	released          *atomic.Int32
	releasedAtPosting atomic.Int32
}

func (p *releaseWatchingPoster) SettleTimeout(ctx context.Context, step TimeoutStep) (TimeoutVote, error) {
	p.releasedAtPosting.Store(p.released.Load())
	return p.failingPoster.SettleTimeout(ctx, step)
}

func TestSettlingARaceReleasesTheFinishItsMissWasClaimedAgainstOnlyAfterTheClaim(t *testing.T) {
	released := &atomic.Int32{}
	poster := &releaseWatchingPoster{released: released}
	races, harness := retryEngine(t, poster)
	registration := races.admit()
	attempt := unsettledAttempt()
	attempt.Terminal = TerminalErrorStream
	attempt.MissProof = &MissProof{ResponsePayload: []byte(`{}`), releaseFinish: func() { released.Add(1) }}

	races.settle(race(attempt), nil, registration)
	harness.fire(0)
	awaitPosts(t, &poster.failingPoster, 1)
	awaitReleased(t, registration)

	require.Zero(t, poster.releasedAtPosting.Load(), "the claim must be posted while its Finish is still held")
	require.Equal(t, int32(1), released.Load(), "the held Finish is let go once its claim was posted")
}
