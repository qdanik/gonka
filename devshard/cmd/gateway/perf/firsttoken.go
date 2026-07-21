package perf

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type timedFirstToken struct {
	ms float64
	at time.Time
}

// firstTokenReservoir holds a timestamped first-token-latency ring per
// (model, input bucket); p95 only reports once enough in-window samples
// exist. Goroutine-safe via mu.
type firstTokenReservoir struct {
	mu         sync.RWMutex
	byBucket   map[string]*ring[timedFirstToken]
	reservoir  int
	activation int
	percentile float64
	staleness  time.Duration
}

func newFirstTokenReservoir(reservoir, activation int, percentile float64, staleness time.Duration) *firstTokenReservoir {
	return &firstTokenReservoir{
		byBucket:   make(map[string]*ring[timedFirstToken]),
		reservoir:  reservoir,
		activation: activation,
		percentile: percentile,
		staleness:  staleness,
	}
}

func bucketKey(model string, bucket int) string {
	return model + "\x00" + strconv.Itoa(bucket)
}

func (r *firstTokenReservoir) record(model string, inputTokens uint64, firstTokenMs float64, now time.Time) {
	key := bucketKey(model, inputBucket(inputTokens))

	r.mu.Lock()
	defer r.mu.Unlock()
	bucketRing, ok := r.byBucket[key]
	if !ok {
		bucketRing = newRing[timedFirstToken](r.reservoir)
		r.byBucket[key] = bucketRing
	}
	bucketRing.add(timedFirstToken{ms: firstTokenMs, at: now})
}

// p95 uses nearest-rank: index = ceil(percentile*n)-1 into the ascending
// sort, clamped to the slice. Samples older than now-staleness are excluded
// from both the activation count and the value.
func (r *firstTokenReservoir) p95(model string, inputTokens uint64, now time.Time) (time.Duration, bool) {
	key := bucketKey(model, inputBucket(inputTokens))
	cutoff := now.Add(-r.staleness)

	r.mu.RLock()
	bucketRing, ok := r.byBucket[key]
	if !ok {
		r.mu.RUnlock()
		return 0, false
	}
	samples := make([]float64, 0, bucketRing.len())
	bucketRing.each(func(sample timedFirstToken) {
		if !sample.at.Before(cutoff) {
			samples = append(samples, sample.ms)
		}
	})
	r.mu.RUnlock()

	if len(samples) < r.activation {
		return 0, false
	}
	sort.Float64s(samples)
	idx := int(math.Ceil(r.percentile*float64(len(samples)))) - 1
	if idx < 0 {
		idx = 0
	} else if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return time.Duration(samples[idx] * float64(time.Millisecond)), true
}
