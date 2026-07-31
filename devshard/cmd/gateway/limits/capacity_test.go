package limits

import (
	"sync"
	"testing"

	"devshard/cmd/gateway/chain"
)

func TestCapacityScaleFactor(t *testing.T) {
	baseSnapshot := func() chain.PhaseSnapshot {
		return chain.PhaseSnapshot{
			CurrentWeightsByModel: map[string]map[string]float64{
				"modelX": {"hostA": 40, "hostB": 20},
			},
			FullWeightsByModel: map[string]map[string]float64{
				"modelX": {"hostA": 60, "hostB": 40},
			},
			CurrentWeights: map[string]float64{"hostA": 40, "hostB": 20, "hostC": 40},
			FullWeights:    map[string]float64{"hostA": 60, "hostB": 40, "hostC": 100},
		}
	}

	t.Run("all hosts available uses W_tot over W_ref for the model", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(baseSnapshot())
		if got := capacity.ScaleFactor("modelX"); got != 0.6 {
			t.Errorf("ScaleFactor(modelX) = %v, want 0.6", got)
		}
	})

	t.Run("unavailable host drops from W_tot but not W_ref", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(func(participant, model string) bool {
			return participant != "hostB" || model != "modelX"
		})
		capacity.Update(baseSnapshot())
		if got := capacity.ScaleFactor("modelX"); got != 0.4 {
			t.Errorf("ScaleFactor(modelX) = %v, want 0.4", got)
		}
	})

	t.Run("model absent from a populated by-model view gets no capacity", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(baseSnapshot())
		if got := capacity.ScaleFactor("modelNeverSeen"); got != 0 {
			t.Errorf("ScaleFactor(modelNeverSeen) = %v, want 0: a model nobody serves must not inherit the generic view", got)
		}
		capacity.SetEscrowMembership("escrow1", map[string]float64{"hostA": 1})
		if got := capacity.EscrowWeight("escrow1", "modelNeverSeen"); got != 0 {
			t.Errorf("EscrowWeight(escrow1, modelNeverSeen) = %v, want 0", got)
		}
	})

	t.Run("with no by-model view at all the generic view applies to every model", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(chain.PhaseSnapshot{
			CurrentWeights: map[string]float64{"hostA": 40, "hostB": 60},
			FullWeights:    map[string]float64{"hostA": 100, "hostB": 100},
		})
		if got := capacity.ScaleFactor("anyModel"); got != 0.5 {
			t.Errorf("ScaleFactor(anyModel) = %v, want 0.5 (generic view, no per-model data yet)", got)
		}
	})

	t.Run("current-by-model entry is honored even without a matching full-by-model entry", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		snapshot := baseSnapshot()
		snapshot.CurrentWeightsByModel["modelPartial"] = map[string]float64{"hostA": 50}
		capacity.Update(snapshot)
		// full falls back to the generic FullWeights sum (200) since FullWeightsByModel has no
		// "modelPartial" entry; current stays the real per-model 50, not the generic sum of 100.
		if got := capacity.ScaleFactor("modelPartial"); got != 0.25 {
			t.Errorf("ScaleFactor(modelPartial) = %v, want 0.25 (real current 50 / generic full 200)", got)
		}
	})

	t.Run("RequestsBlocked forces scale to zero regardless of weights", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		snapshot := baseSnapshot()
		snapshot.RequestsBlocked = true
		capacity.Update(snapshot)
		if got := capacity.ScaleFactor("modelX"); got != 0 {
			t.Errorf("ScaleFactor(modelX) = %v, want 0 when RequestsBlocked", got)
		}
	})

	t.Run("zero baseline before any Update means unlimited", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		if got := capacity.ScaleFactor("modelX"); got != 1 {
			t.Errorf("ScaleFactor(modelX) = %v, want 1 (unlimited) before any Update", got)
		}
	})

	t.Run("nil available treats every host as available", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(baseSnapshot())
		if got := capacity.ScaleFactor("modelX"); got != 0.6 {
			t.Errorf("ScaleFactor(modelX) = %v, want 0.6 with nil availability", got)
		}
	})
}

func TestCapacityUpdateReplacesPriorSnapshot(t *testing.T) {
	t.Parallel()
	capacity := NewCapacity(nil)
	capacity.Update(chain.PhaseSnapshot{
		CurrentWeights: map[string]float64{"hostA": 10},
		FullWeights:    map[string]float64{"hostA": 100},
	})
	capacity.Update(chain.PhaseSnapshot{
		CurrentWeights: map[string]float64{"hostZ": 50},
		FullWeights:    map[string]float64{"hostZ": 100},
	})
	if got := capacity.ScaleFactor("modelX"); got != 0.5 {
		t.Errorf("ScaleFactor(modelX) = %v, want 0.5 from the second snapshot only", got)
	}
}

func TestCapacityEscrowWeight(t *testing.T) {
	t.Run("membership share weighted by per-model current weight", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(chain.PhaseSnapshot{
			CurrentWeightsByModel: map[string]map[string]float64{
				"modelX": {"hostA": 100, "hostB": 200},
			},
		})
		capacity.SetEscrowMembership("escrow1", map[string]float64{"hostA": 0.5, "hostB": 0.25})
		if got := capacity.EscrowWeight("escrow1", "modelX"); got != 100 {
			t.Errorf("EscrowWeight(escrow1, modelX) = %v, want 100", got)
		}
	})

	t.Run("an ejected host drops its contribution", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(func(participant, model string) bool {
			return participant != "hostB" || model != "modelX"
		})
		capacity.Update(chain.PhaseSnapshot{
			CurrentWeightsByModel: map[string]map[string]float64{
				"modelX": {"hostA": 100, "hostB": 200},
			},
		})
		capacity.SetEscrowMembership("escrow1", map[string]float64{"hostA": 0.5, "hostB": 0.25})
		if got := capacity.EscrowWeight("escrow1", "modelX"); got != 50 {
			t.Errorf("EscrowWeight(escrow1, modelX) = %v, want 50 with hostB ejected", got)
		}
	})

	t.Run("unknown escrow returns zero", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(chain.PhaseSnapshot{
			CurrentWeightsByModel: map[string]map[string]float64{"modelX": {"hostA": 100}},
		})
		if got := capacity.EscrowWeight("neverRegistered", "modelX"); got != 0 {
			t.Errorf("EscrowWeight(neverRegistered, modelX) = %v, want 0", got)
		}
	})

	t.Run("SetEscrowMembership replaces rather than merges", func(t *testing.T) {
		t.Parallel()
		capacity := NewCapacity(nil)
		capacity.Update(chain.PhaseSnapshot{
			CurrentWeightsByModel: map[string]map[string]float64{
				"modelX": {"hostA": 100, "hostB": 200},
			},
		})
		capacity.SetEscrowMembership("escrow1", map[string]float64{"hostA": 1})
		capacity.SetEscrowMembership("escrow1", map[string]float64{"hostB": 1})
		if got := capacity.EscrowWeight("escrow1", "modelX"); got != 200 {
			t.Errorf("EscrowWeight(escrow1, modelX) = %v, want 200 (only the latest membership)", got)
		}
	})
}

func TestCapacityRemoveEscrow(t *testing.T) {
	t.Parallel()
	capacity := NewCapacity(nil)
	capacity.Update(chain.PhaseSnapshot{
		CurrentWeightsByModel: map[string]map[string]float64{"modelX": {"hostA": 100}},
	})
	capacity.SetEscrowMembership("escrow1", map[string]float64{"hostA": 1})
	if got := capacity.EscrowWeight("escrow1", "modelX"); got != 100 {
		t.Fatalf("EscrowWeight(escrow1, modelX) = %v, want 100 before removal", got)
	}
	capacity.RemoveEscrow("escrow1")
	if got := capacity.EscrowWeight("escrow1", "modelX"); got != 0 {
		t.Errorf("EscrowWeight(escrow1, modelX) = %v, want 0 after RemoveEscrow", got)
	}
}

func TestCapacityConcurrentUpdateAndRead(t *testing.T) {
	capacity := NewCapacity(func(participant, model string) bool { return participant != "hostEjected" })
	const goroutineCount = 8
	const iterationsPerGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutineCount * 3)

	for goroutineIndex := 0; goroutineIndex < goroutineCount; goroutineIndex++ {
		go func(goroutineIndex int) {
			defer wg.Done()
			for iteration := 0; iteration < iterationsPerGoroutine; iteration++ {
				capacity.Update(chain.PhaseSnapshot{
					CurrentWeightsByModel: map[string]map[string]float64{
						"modelX": {"hostA": float64(goroutineIndex + iteration), "hostEjected": 50},
					},
					FullWeightsByModel: map[string]map[string]float64{
						"modelX": {"hostA": 100, "hostEjected": 100},
					},
				})
			}
		}(goroutineIndex)

		go func() {
			defer wg.Done()
			for iteration := 0; iteration < iterationsPerGoroutine; iteration++ {
				capacity.SetEscrowMembership("escrowA", map[string]float64{"hostA": 1})
				_ = capacity.EscrowWeight("escrowA", "modelX")
			}
		}()

		go func() {
			defer wg.Done()
			for iteration := 0; iteration < iterationsPerGoroutine; iteration++ {
				_ = capacity.ScaleFactor("modelX")
			}
		}()
	}

	wg.Wait()
	capacity.RemoveEscrow("escrowA")
}

func TestCapacityEscrowWeightOnUnobservedChainWeights(t *testing.T) {
	cases := []struct {
		name     string
		snapshot chain.PhaseSnapshot
		shares   map[string]float64
		want     float64
	}{
		{
			name:     "no weights observed at all falls back to the escrow's available share",
			snapshot: chain.PhaseSnapshot{LastUpdatedAt: testEpoch},
			shares:   map[string]float64{"hostA": 0.5, "hostB": 0.25},
			want:     0.75,
		},
		{
			name:     "an unavailable host is left out of the fallback share",
			snapshot: chain.PhaseSnapshot{LastUpdatedAt: testEpoch},
			shares:   map[string]float64{"hostA": 0.5, "hostUnavailable": 0.25},
			want:     0.5,
		},
		{
			// A stale poll republishes the weights it last saw, so the counts stay non-zero and the
			// escrow is judged on real data rather than on the fallback.
			name: "a stale snapshot keeps the weights it observed",
			snapshot: chain.PhaseSnapshot{
				CurrentWeights: map[string]float64{"hostA": 100},
				FullWeights:    map[string]float64{"hostA": 100},
				LastError:      "fetch participants: connection refused",
			},
			shares: map[string]float64{"hostA": 0.5},
			want:   50,
		},
		{
			// Every reported host weighing zero is an answer, not a missing one: capacity really is
			// zero and the escrow must score unusable.
			name: "weights observed as zero stay zero",
			snapshot: chain.PhaseSnapshot{
				CurrentWeights: map[string]float64{"hostA": 0, "hostB": 0},
				FullWeights:    map[string]float64{"hostA": 0, "hostB": 0},
				LastUpdatedAt:  testEpoch,
			},
			shares: map[string]float64{"hostA": 0.5, "hostB": 0.25},
			want:   0,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			capacity := NewCapacity(func(participant, _ string) bool { return participant != "hostUnavailable" })
			capacity.Update(testCase.snapshot)
			capacity.SetEscrowMembership("escrowA", testCase.shares)

			if got := capacity.EscrowWeight("escrowA", "modelX"); got != testCase.want {
				t.Errorf("EscrowWeight() = %v, want %v", got, testCase.want)
			}
		})
	}
}
