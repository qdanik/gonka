package app

import (
	"slices"
	"testing"

	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/metrics"
	"devshard/types"
	"devshard/user"
)

type invalidatedReply struct {
	escrowID string
	nonce    uint64
	reason   api.CacheInvalidationReason
}

type repliesSpy struct{ invalidated []invalidatedReply }

func (s *repliesSpy) InvalidateCachedReply(escrowID string, nonce uint64, reason api.CacheInvalidationReason) {
	s.invalidated = append(s.invalidated, invalidatedReply{escrowID, nonce, reason})
}

type diffsSpy struct{ composed int }

func (s *diffsSpy) DiffComposed(string, *types.Diff) { s.composed++ }

func validationTx(nonce uint64, valid bool) *types.DevshardTx {
	return &types.DevshardTx{Tx: &types.DevshardTx_Validation{Validation: &types.MsgValidation{InferenceId: nonce, Valid: valid}}}
}

// Test flow:
//  1. Compose a diff carrying a valid vote on nonce 4, an invalid vote on nonce 5 and an invalid vote on nonce 6.
//  2. Hand it to the verdict-watching journal.
//  3. Assert the wrapped journal got the diff and only the two invalid votes revoked a cached reply, each under the invalid-verdict reason.
func TestAnInvalidVoteInAComposedDiffRevokesTheCachedReply(t *testing.T) {
	spy := &repliesSpy{}
	cache := &cacheInvalidation{}
	cache.attach(spy)
	diffs := &diffsSpy{}
	watching := verdictWatchingDiffs{diffs: diffs, cache: cache}

	watching.DiffComposed("escrow-1", &types.Diff{Txs: []*types.DevshardTx{validationTx(4, true), validationTx(5, false), validationTx(6, false)}})

	want := []invalidatedReply{
		{"escrow-1", 5, api.CacheReasonInvalidVerdict},
		{"escrow-1", 6, api.CacheReasonInvalidVerdict},
	}
	if diffs.composed != 1 || !slices.Equal(spy.invalidated, want) {
		t.Fatalf("composed %d diffs, revoked %v, want 1 and %v", diffs.composed, spy.invalidated, want)
	}
}

// Test flow:
//  1. Hand a diff with an invalid vote to a journal whose cache signal was never attached, and to a nil signal.
//  2. Assert neither panics and the wrapped journal still got both diffs.
func TestADiffWithNoCacheAttachedIsStillJournaled(t *testing.T) {
	diffs := &diffsSpy{}
	diff := &types.Diff{Txs: []*types.DevshardTx{validationTx(5, false)}}

	verdictWatchingDiffs{diffs: diffs, cache: &cacheInvalidation{}}.DiffComposed("escrow-1", diff)
	verdictWatchingDiffs{diffs: diffs}.DiffComposed("escrow-1", diff)

	if diffs.composed != 2 {
		t.Fatalf("composed %d diffs, want 2", diffs.composed)
	}
}

// Test flow:
//  1. Report a bound and a mismatched served stream, then a mismatch with the cache signal attached.
//  2. Assert only the mismatch revokes a cached reply, under the served-mismatch reason.
func TestAServedStreamThatMismatchesItsFinishRevokesTheCachedReply(t *testing.T) {
	spy := &repliesSpy{}
	cache := &cacheInvalidation{}
	cache.attach(spy)
	mismatches := &recordedMismatches{}
	observe := servedBindingStrikes(mismatches, mismatches, metrics.NewServedBindingRecorder(metrics.New()), mismatches, cache)

	observe("escrow-1", "llama", "host-a", 7, user.ServedBindingBound)
	observe("escrow-1", "llama", "host-a", 8, user.ServedBindingMismatch)

	want := []invalidatedReply{{"escrow-1", 8, api.CacheReasonServedMismatch}}
	if !slices.Equal(spy.invalidated, want) {
		t.Fatalf("revoked %v, want %v", spy.invalidated, want)
	}
}

// Test flow:
//  1. Tell the signal of an invalidated record.
//  2. Assert the cached reply is revoked under the record-invalidated reason.
func TestAnInvalidatedRecordRevokesTheCachedReply(t *testing.T) {
	spy := &repliesSpy{}
	cache := &cacheInvalidation{}
	cache.attach(spy)

	cache.RecordInvalidated("escrow-1", 9)

	want := []invalidatedReply{{"escrow-1", 9, api.CacheReasonRecordInvalidated}}
	if !slices.Equal(spy.invalidated, want) {
		t.Fatalf("revoked %v, want %v", spy.invalidated, want)
	}
}
