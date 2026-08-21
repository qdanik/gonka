package scheduler

import (
	"fmt"
	"testing"

	"devshard/cmd/gateway/chain"
)

// The fakes in escrow_pick_test.go record every call under a mutex, which a benchmark would measure
// instead of the code under test. These record nothing.

type benchSession struct {
	latestNonce uint64
	groupSize   int
}

func (b *benchSession) Advance(func(HostBinding) NonceIntent) (Prepared, error) { return nil, nil }
func (b *benchSession) ParticipantKeys() []string                               { return nil }
func (b *benchSession) GroupSize() int                                          { return b.groupSize }
func (b *benchSession) LatestNonce() uint64                                     { return b.latestNonce }
func (b *benchSession) Balance() uint64                                         { return 1 << 40 }
func (b *benchSession) TokenPrice() uint64                                      { return 1 }

type benchEscrows struct{ byModel map[string][]Escrow }

func (b *benchEscrows) Candidates(model string) []Escrow { return b.byModel[model] }

type benchWeights struct{ byEscrow map[string]float64 }

func (b *benchWeights) EscrowWeight(escrowID, model string) float64 { return b.byEscrow[escrowID] }

func benchScheduler(escrows, models int) (*Scheduler, []string) {
	source := &benchEscrows{byModel: map[string][]Escrow{}}
	weights := &benchWeights{byEscrow: map[string]float64{}}
	modelNames := make([]string, 0, models)
	for index := range models {
		modelNames = append(modelNames, fmt.Sprintf("model-%d", index))
	}
	for index := range escrows {
		id := fmt.Sprintf("escrow-%d", index)
		model := modelNames[index%models]
		source.byModel[model] = append(source.byModel[model], Escrow{
			ID:          id,
			Model:       model,
			Session:     &benchSession{groupSize: 4},
			ActiveUsers: index % 7,
		})
		weights.byEscrow[id] = 1
	}
	return &Scheduler{escrows: source, capacity: weights}, modelNames
}

func BenchmarkPickEscrow(b *testing.B) {
	snapshot := chain.PhaseSnapshot{}
	for _, escrows := range []int{1, 10, 100, 1000} {
		models := min(4, escrows)
		scheduler, modelNames := benchScheduler(escrows, models)
		b.Run(fmt.Sprintf("escrows=%d", escrows), func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				if _, err := scheduler.pickEscrow(RequestProfile{Model: modelNames[i%len(modelNames)]}, snapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPickEscrowParallel(b *testing.B) {
	snapshot := chain.PhaseSnapshot{}
	scheduler, modelNames := benchScheduler(100, 4)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := scheduler.pickEscrow(RequestProfile{Model: modelNames[i%len(modelNames)]}, snapshot); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkPickEscrowDegraded measures the selection path as capacity disappears: the cost of
// deciding "no" is what a partial outage pays on every request.
func BenchmarkPickEscrowDegraded(b *testing.B) {
	snapshot := chain.PhaseSnapshot{}
	for _, deadPercent := range []int{0, 50, 90, 100} {
		scheduler, modelNames := benchScheduler(100, 4)
		weights := scheduler.capacity.(*benchWeights)
		dead := len(weights.byEscrow) * deadPercent / 100
		for index := range dead {
			weights.byEscrow[fmt.Sprintf("escrow-%d", index)] = 0
		}
		b.Run(fmt.Sprintf("dead=%d%%", deadPercent), func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				_, _ = scheduler.pickEscrow(RequestProfile{Model: modelNames[i%len(modelNames)]}, snapshot)
			}
		})
	}
}

// BenchmarkPickEscrowNonceSweep walks one escrow's whole nonce budget to its governance cap, which is
// what an escrow's lifetime costs in selection alone.
func BenchmarkPickEscrowNonceSweep(b *testing.B) {
	snapshot := chain.PhaseSnapshot{MaxNonce: 20_000}
	scheduler, modelNames := benchScheduler(100, 4)
	source := scheduler.escrows.(*benchEscrows)
	tracked := source.byModel[modelNames[0]][0].Session.(*benchSession)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		tracked.latestNonce = uint64(i % 20_000)
		if _, err := scheduler.pickEscrow(RequestProfile{Model: modelNames[0]}, snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPickEscrowSingleModel isolates per-candidate cost from the model-filter effect: with one
// model both trees scan the same number of escrows.
func BenchmarkPickEscrowSingleModel(b *testing.B) {
	snapshot := chain.PhaseSnapshot{}
	for _, escrows := range []int{10, 100, 1000} {
		scheduler, modelNames := benchScheduler(escrows, 1)
		b.Run(fmt.Sprintf("escrows=%d", escrows), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := scheduler.pickEscrow(RequestProfile{Model: modelNames[0]}, snapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
