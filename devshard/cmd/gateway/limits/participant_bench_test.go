package limits

import (
	"fmt"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
)

// Routing consults the participant limiter for every host of a group, and the capacity model consults
// it again for every host of every candidate escrow, so these run over a whole group rather than one host.

const (
	benchModel   = "model-bench"
	benchHosts   = 16
	benchNetwork = 32
)

var benchClock = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// The sinks keep what a benchmark measured from being optimised away.
var (
	windowSink []HostWindow
	floatSink  float64
)

// benchParticipant is bech32-shaped, so map keys hash over a realistic length.
func benchParticipant(index int) string {
	return fmt.Sprintf("gonka1%039d", index)
}

func benchParticipants(count int) []string {
	hosts := make([]string, 0, count)
	for index := range count {
		hosts = append(hosts, benchParticipant(index))
	}
	return hosts
}

func benchLimiter(hosts []string) *ParticipantLimiter {
	limiter := NewParticipantLimiter(
		ParticipantConfig{Initial: 8, Max: 32, AfterFailures: 3, BaseOpen: time.Second, MaxOpen: time.Minute},
		func() time.Time { return benchClock },
	)
	for _, participant := range hosts {
		limiter.Acquire(participant, benchModel)
		limiter.Release(participant, benchModel)
	}
	return limiter
}

// BenchmarkParticipantAvailable is the routing pre-filter: one call per host of the group, per drain.
func BenchmarkParticipantAvailable(b *testing.B) {
	hosts := benchParticipants(benchHosts)
	limiter := benchLimiter(hosts)
	b.ReportAllocs()
	for b.Loop() {
		for _, participant := range hosts {
			if !limiter.Available(participant, benchModel) {
				b.Fatal("host should be available")
			}
		}
	}
}

// BenchmarkParticipantAvailableParallel is the shape that matters: many request goroutines asking at once.
func BenchmarkParticipantAvailableParallel(b *testing.B) {
	hosts := benchParticipants(benchHosts)
	limiter := benchLimiter(hosts)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, participant := range hosts {
				limiter.Available(participant, benchModel)
			}
		}
	})
}

// BenchmarkParticipantAttempt is one dispatched attempt: the slot, its release, and the verdict.
func BenchmarkParticipantAttempt(b *testing.B) {
	hosts := benchParticipants(benchHosts)
	limiter := benchLimiter(hosts)
	b.ReportAllocs()
	index := 0
	for b.Loop() {
		participant := hosts[index%len(hosts)]
		index++
		limiter.Acquire(participant, benchModel)
		limiter.Release(participant, benchModel)
		limiter.OnResult(participant, benchModel, Success)
	}
}

// BenchmarkParticipantAttemptParallel measures the same attempt under the contention a busy gateway has.
func BenchmarkParticipantAttemptParallel(b *testing.B) {
	hosts := benchParticipants(benchHosts)
	limiter := benchLimiter(hosts)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		index := 0
		for pb.Next() {
			participant := hosts[index%len(hosts)]
			index++
			limiter.Acquire(participant, benchModel)
			limiter.Release(participant, benchModel)
			limiter.OnResult(participant, benchModel, Success)
		}
	})
}

// BenchmarkParticipantSnapshot is the metrics scrape over a whole group.
func BenchmarkParticipantSnapshot(b *testing.B) {
	limiter := benchLimiter(benchParticipants(benchHosts))
	b.ReportAllocs()
	for b.Loop() {
		windowSink = limiter.Snapshot()
	}
}

func benchCapacityModel(hosts []string) *Capacity {
	limiter := benchLimiter(hosts)
	capacity := NewCapacity(limiter.Available)
	network := benchParticipants(benchNetwork)
	current := make(map[string]float64, len(network))
	full := make(map[string]float64, len(network))
	for index, participant := range network {
		current[participant] = float64(1000 + index)
		full[participant] = float64(1200 + index)
	}
	capacity.Update(chain.PhaseSnapshot{
		CurrentWeightsByModel: map[string]map[string]float64{benchModel: current},
		FullWeightsByModel:    map[string]map[string]float64{benchModel: full},
	})
	shares := make(map[string]float64, len(hosts))
	for _, participant := range hosts {
		shares[participant] = 0.5
	}
	capacity.SetEscrowMembership("escrow-bench", shares)
	return capacity
}

// BenchmarkCapacityEscrowWeight is the escrow score: one call per candidate escrow, on every pick.
func BenchmarkCapacityEscrowWeight(b *testing.B) {
	capacity := benchCapacityModel(benchParticipants(benchHosts))
	b.ReportAllocs()
	for b.Loop() {
		floatSink = capacity.EscrowWeight("escrow-bench", benchModel)
	}
}

// BenchmarkCapacityForModel is what admission reads per request: the weights and the scale factor.
func BenchmarkCapacityForModel(b *testing.B) {
	capacity := benchCapacityModel(benchParticipants(benchHosts))
	b.ReportAllocs()
	for b.Loop() {
		current, baseline := capacity.Weights(benchModel)
		floatSink = current + baseline + capacity.ScaleFactor(benchModel, false)
	}
}

// BenchmarkCapacityForModelParallel is the same read from many request goroutines at once.
func BenchmarkCapacityForModelParallel(b *testing.B) {
	capacity := benchCapacityModel(benchParticipants(benchHosts))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			current, baseline := capacity.Weights(benchModel)
			floatSink = current + baseline + capacity.ScaleFactor(benchModel, false)
		}
	})
}
