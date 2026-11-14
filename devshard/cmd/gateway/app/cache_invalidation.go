package app

import (
	"sync/atomic"

	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/nonces"
	"devshard/types"
)

type servedReplies interface {
	ServedMismatch(escrowID string, nonce uint64)
}

type cachedReplies interface {
	InvalidateCachedReply(escrowID string, nonce uint64, reason api.CacheInvalidationReason)
}

type cacheInvalidation struct{ replies atomic.Pointer[cachedReplies] }

func (c *cacheInvalidation) attach(replies cachedReplies) { c.replies.Store(&replies) }

func (c *cacheInvalidation) invalidate(reason api.CacheInvalidationReason, escrowID string, nonce uint64) {
	if c == nil {
		return
	}
	if replies := c.replies.Load(); replies != nil {
		(*replies).InvalidateCachedReply(escrowID, nonce, reason)
	}
}

func (c *cacheInvalidation) RecordInvalidated(escrowID string, nonce uint64) {
	c.invalidate(api.CacheReasonRecordInvalidated, escrowID, nonce)
}

func (c *cacheInvalidation) ServedMismatch(escrowID string, nonce uint64) {
	c.invalidate(api.CacheReasonServedMismatch, escrowID, nonce)
}

type verdictWatchingDiffs struct {
	diffs nonces.DiffJournal
	cache *cacheInvalidation
}

func (g *gateway) diffJournal() nonces.DiffJournal {
	return verdictWatchingDiffs{diffs: g.events, cache: g.replies}
}

func (w verdictWatchingDiffs) DiffComposed(escrowID string, diff *types.Diff) {
	w.diffs.DiffComposed(escrowID, diff)
	for _, tx := range diff.Txs {
		if validation := tx.GetValidation(); validation != nil && !validation.Valid {
			w.cache.invalidate(api.CacheReasonInvalidVerdict, escrowID, validation.InferenceId)
		}
	}
}
