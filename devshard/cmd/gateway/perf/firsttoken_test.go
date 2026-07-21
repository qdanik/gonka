package perf

import (
	"sync"
	"testing"
	"time"
)

func TestInputBucketBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		tokens uint64
		want   int
	}{
		{"zero tokens is bucket 0", 0, 0},
		{"just under 1k is bucket 0", 999, 0},
		{"1k is bucket 1", 1_000, 1},
		{"just under 5k is bucket 1", 4_999, 1},
		{"5k is bucket 2", 5_000, 2},
		{"just under 15k is bucket 2", 14_999, 2},
		{"15k is bucket 3", 15_000, 3},
		{"just under 30k is bucket 3", 29_999, 3},
		{"30k is bucket 4", 30_000, 4},
		{"just under 100k is bucket 4", 99_999, 4},
		{"100k is bucket 5", 100_000, 5},
		{"far above 100k is bucket 5", 10_000_000, 5},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := inputBucket(testCase.tokens); got != testCase.want {
				t.Fatalf("inputBucket(%d) = %d, want %d", testCase.tokens, got, testCase.want)
			}
		})
	}
}

func TestFirstTokenReservoirRecordAndP95NearestRank(t *testing.T) {
	r := newFirstTokenReservoir(20, 20, 0.95, time.Hour)
	model, inputTokens := "model-a", uint64(500)
	for i := 1; i <= 20; i++ {
		r.record(model, inputTokens, float64(i), testEpoch.Add(time.Duration(i)*time.Second))
	}

	got, ok := r.p95(model, inputTokens, testEpoch.Add(time.Minute))
	if !ok {
		t.Fatal("p95() ok = false, want true with 20 samples at a 20-sample activation gate")
	}
	if want := 19 * time.Millisecond; got != want {
		t.Fatalf("p95() = %v, want %v (nearest-rank: ceil(0.95*20)=19th smallest of 1..20)", got, want)
	}
}

func TestFirstTokenReservoirActivationGateExactThreshold(t *testing.T) {
	r := newFirstTokenReservoir(20, 20, 0.95, time.Hour)
	model, inputTokens := "model-b", uint64(2_000)
	for i := 1; i <= 19; i++ {
		r.record(model, inputTokens, float64(i), testEpoch.Add(time.Duration(i)*time.Second))
	}
	if _, ok := r.p95(model, inputTokens, testEpoch.Add(time.Minute)); ok {
		t.Fatal("p95() ok = true with activation-1 (19) samples, want false")
	}

	r.record(model, inputTokens, 20, testEpoch.Add(20*time.Second))
	if _, ok := r.p95(model, inputTokens, testEpoch.Add(time.Minute)); !ok {
		t.Fatal("p95() ok = false with exactly activation (20) samples, want true")
	}
}

// Stale samples sit outside the staleness window at 1000ms; fresh ones carry
// 1..20ms. If staleness leaked into count or value, activation would double-
// trigger early or p95 would be dragged toward 1000ms instead of 19ms.
func TestFirstTokenReservoirStalenessExcludesOldSamples(t *testing.T) {
	r := newFirstTokenReservoir(50, 20, 0.95, 10*time.Minute)
	model, inputTokens := "model-c", uint64(500)
	now := testEpoch.Add(20 * time.Minute)

	for range 20 {
		r.record(model, inputTokens, 1000, testEpoch)
	}
	for i := 1; i <= 20; i++ {
		r.record(model, inputTokens, float64(i), now)
	}

	got, ok := r.p95(model, inputTokens, now)
	if !ok {
		t.Fatal("p95() ok = false, want true (20 fresh samples meet activation on their own)")
	}
	if want := 19 * time.Millisecond; got != want {
		t.Fatalf("p95() = %v, want %v (stale samples must be excluded from count and value)", got, want)
	}
}

func TestFirstTokenReservoirIsolatesDifferentModelAndBucket(t *testing.T) {
	r := newFirstTokenReservoir(10, 5, 0.95, time.Hour)
	recordFive := func(model string, inputTokens uint64, base float64) {
		for i := int64(0); i < 5; i++ {
			r.record(model, inputTokens, base*float64(i+1), testEpoch.Add(time.Duration(i)*time.Second))
		}
	}
	recordFive("model-a", 500, 10)
	recordFive("model-a", 2_000, 100)
	recordFive("model-b", 500, 1)

	now := testEpoch.Add(time.Minute)
	cases := []struct {
		name        string
		model       string
		inputTokens uint64
		wantMs      float64
	}{
		{"model-a bucket 0", "model-a", 500, 50},
		{"model-a bucket 1", "model-a", 2_000, 500},
		{"model-b bucket 0", "model-b", 500, 5},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, ok := r.p95(testCase.model, testCase.inputTokens, now)
			if !ok {
				t.Fatalf("p95(%s, %d) ok = false, want true", testCase.model, testCase.inputTokens)
			}
			if want := time.Duration(testCase.wantMs * float64(time.Millisecond)); got != want {
				t.Fatalf("p95(%s, %d) = %v, want %v", testCase.model, testCase.inputTokens, got, want)
			}
		})
	}

	if _, ok := r.p95("model-b", 2_000, now); ok {
		t.Fatal("p95() for a never-recorded (model,bucket) ok = true, want false")
	}
}

func TestFirstTokenReservoirConcurrentRecordAndP95NoRace(t *testing.T) {
	r := newFirstTokenReservoir(50, 10, 0.95, time.Hour)
	model, inputTokens := "model-race", uint64(500)

	var wg sync.WaitGroup
	for i := range 50 {
		at := testEpoch.Add(time.Duration(i) * time.Millisecond)
		wg.Add(2)
		go func(ms float64, at time.Time) {
			defer wg.Done()
			r.record(model, inputTokens, ms, at)
		}(float64(i%30), at)
		go func(at time.Time) {
			defer wg.Done()
			r.p95(model, inputTokens, at)
		}(at)
	}
	wg.Wait()
}
