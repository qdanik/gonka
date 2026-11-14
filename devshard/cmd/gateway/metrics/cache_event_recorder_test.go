package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Test flow:
//  1. Record a lookup, a bypass, a hit age, an invalidation of two entries with three replays, and a revoked overflow.
//  2. Assert each metric reads back with its labels and value.
func TestCacheEventsAreCountedUnderTheirLabels(t *testing.T) {
	telemetry := New()
	recorder := NewCacheEventRecorder(telemetry)

	recorder.Lookup("llama", "hit")
	recorder.Bypassed("llama", "no-cache")
	recorder.HitAge(30 * time.Second)
	recorder.Invalidated("epoch", "dropped", 2)
	recorder.InvalidatedReplays(3)
	recorder.RevokedOverflow()

	expected := `
# HELP devshard_gateway_cache_lookups_total Response cache lookups by model and result. A request that skipped the lookup is counted under bypass instead.
# TYPE devshard_gateway_cache_lookups_total counter
devshard_gateway_cache_lookups_total{model="llama",result="hit"} 1
# HELP devshard_gateway_cache_bypass_total Requests whose Cache-Control header skipped the response cache, by model and directive.
# TYPE devshard_gateway_cache_bypass_total counter
devshard_gateway_cache_bypass_total{directive="no-cache",model="llama"} 1
# HELP devshard_gateway_cache_invalidations_total Cached replies removed, by the signal that removed them and by whether a stored reply was dropped, a verdict was kept for a reply not yet stored, or the verdict was already kept.
# TYPE devshard_gateway_cache_invalidations_total counter
devshard_gateway_cache_invalidations_total{effect="dropped",reason="epoch"} 2
# HELP devshard_gateway_cache_invalidated_replays_total Times the replies removed by an invalidation had already been served from the cache.
# TYPE devshard_gateway_cache_invalidated_replays_total counter
devshard_gateway_cache_invalidated_replays_total 3
# HELP devshard_gateway_cache_revoked_overflow_total Verdicts forgotten because the set of verdicts awaiting their reply was full; a late reply under such a verdict is stored.
# TYPE devshard_gateway_cache_revoked_overflow_total counter
devshard_gateway_cache_revoked_overflow_total 1
`
	names := []string{
		"devshard_gateway_cache_lookups_total", "devshard_gateway_cache_bypass_total", "devshard_gateway_cache_invalidations_total",
		"devshard_gateway_cache_invalidated_replays_total", "devshard_gateway_cache_revoked_overflow_total",
	}
	if err := testutil.GatherAndCompare(telemetry.Registry(), strings.NewReader(expected), names...); err != nil {
		t.Fatalf("GatherAndCompare(cache events) = %v, want nil", err)
	}
	if got := testutil.CollectAndCount(recorder.hitAge); got != 1 {
		t.Fatalf("hit age series = %d, want 1", got)
	}
}

// Test flow:
//  1. Call every recorder method on a nil recorder.
//  2. Assert none panics.
func TestANilCacheEventRecorderRecordsNothing(t *testing.T) {
	var recorder *CacheEventRecorder

	recorder.Lookup("llama", "hit")
	recorder.Bypassed("llama", "no-store")
	recorder.HitAge(time.Second)
	recorder.Invalidated("epoch", "dropped", 1)
	recorder.InvalidatedReplays(1)
	recorder.RevokedOverflow()
}
