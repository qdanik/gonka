package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	cacheHitAgeBucketStartSeconds = 1
	cacheHitAgeBucketCount        = 13
)

var cacheHitAgeBuckets = prometheus.ExponentialBuckets(cacheHitAgeBucketStartSeconds, bucketGrowthFactor, cacheHitAgeBucketCount)

// CacheEventRecorder counts the response cache's client contract and its invalidation: lookups, bypasses, the age of what was served, and every removal.
type CacheEventRecorder struct {
	lookups         *prometheus.CounterVec
	bypasses        *prometheus.CounterVec
	hitAge          prometheus.Histogram
	invalidations   *prometheus.CounterVec
	replays         prometheus.Counter
	revokedOverflow prometheus.Counter
}

func NewCacheEventRecorder(telemetry *Metrics) *CacheEventRecorder {
	recorder := &CacheEventRecorder{
		lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_cache_lookups_total",
			Help: "Response cache lookups by model and result. A request that skipped the lookup is counted under bypass instead.",
		}, []string{"model", "result"}),
		bypasses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_cache_bypass_total",
			Help: "Requests whose Cache-Control header skipped the response cache, by model and directive.",
		}, []string{"model", "directive"}),
		hitAge: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "devshard_gateway_cache_hit_age_seconds",
			Help:    "Seconds a cached reply had been stored when it was served.",
			Buckets: cacheHitAgeBuckets,
		}),
		invalidations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_cache_invalidations_total",
			Help: "Cached replies removed, by the signal that removed them and by whether a stored reply was dropped, a verdict was kept for a reply not yet stored, or the verdict was already kept.",
		}, []string{"reason", "effect"}),
		replays: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "devshard_gateway_cache_invalidated_replays_total",
			Help: "Times the replies removed by an invalidation had already been served from the cache.",
		}),
		revokedOverflow: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "devshard_gateway_cache_revoked_overflow_total",
			Help: "Verdicts forgotten because the set of verdicts awaiting their reply was full; a late reply under such a verdict is stored.",
		}),
	}
	telemetry.Register(recorder.lookups, recorder.bypasses, recorder.hitAge, recorder.invalidations, recorder.replays, recorder.revokedOverflow)
	return recorder
}

func (r *CacheEventRecorder) Lookup(model, result string) {
	if r == nil {
		return
	}
	r.lookups.WithLabelValues(metricLabel(model, labelUnknown), metricLabel(result, labelUnknown)).Inc()
}

func (r *CacheEventRecorder) Bypassed(model, directive string) {
	if r == nil {
		return
	}
	r.bypasses.WithLabelValues(metricLabel(model, labelUnknown), metricLabel(directive, labelUnknown)).Inc()
}

func (r *CacheEventRecorder) HitAge(age time.Duration) {
	if r == nil {
		return
	}
	r.hitAge.Observe(age.Seconds())
}

func (r *CacheEventRecorder) Invalidated(reason, effect string, count int) {
	if r == nil {
		return
	}
	r.invalidations.WithLabelValues(metricLabel(reason, labelUnknown), metricLabel(effect, labelUnknown)).Add(float64(count))
}

func (r *CacheEventRecorder) InvalidatedReplays(count uint64) {
	if r == nil {
		return
	}
	r.replays.Add(float64(count))
}

func (r *CacheEventRecorder) RevokedOverflow() {
	if r == nil {
		return
	}
	r.revokedOverflow.Inc()
}
