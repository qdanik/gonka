package escrow

import (
	"sync"
	"testing"
)

const suppressionSafetyBound = 10

func countSuppressedCalls(b *createBreaker, model, role string) int {
	count := 0
	for count <= suppressionSafetyBound {
		if !b.gated(model, role) {
			return count
		}
		count++
	}
	return count
}

func TestCreateBreakerFreshKeyNotGated(t *testing.T) {
	b := newCreateBreaker()

	if b.gated("model-a", "temp") {
		t.Fatal("gated() = true for a fresh key, want false")
	}
}

func TestCreateBreakerEscalationLadder(t *testing.T) {
	tests := []struct {
		name           string
		failures       int
		wantSuppressed int
	}{
		{name: "1 failure suppresses 1 call", failures: 1, wantSuppressed: 1},
		{name: "2 failures suppress 2 calls", failures: 2, wantSuppressed: 2},
		{name: "3 failures suppress 4 calls", failures: 3, wantSuppressed: 4},
		{name: "4 failures still cap at 4", failures: 4, wantSuppressed: 4},
		{name: "5 failures stay capped at 4", failures: 5, wantSuppressed: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newCreateBreaker()
			for range tt.failures {
				b.recordFailure("model-a", "temp")
			}

			got := countSuppressedCalls(b, "model-a", "temp")
			if got != tt.wantSuppressed {
				t.Fatalf("suppressed %d calls after %d failures, want %d", got, tt.failures, tt.wantSuppressed)
			}
		})
	}
}

func TestCreateBreakerKeysAreIndependent(t *testing.T) {
	b := newCreateBreaker()
	b.recordFailure("model-a", "temp")
	b.recordFailure("model-a", "temp") // cooldown=2: leaves a residual tick a colliding key would visibly steal

	if b.gated("model-a", "regular") {
		t.Fatal("gated(model-a, regular) = true, want a different role to stay ungated")
	}
	if b.gated("model-b", "temp") {
		t.Fatal("gated(model-b, temp) = true, want a different model to stay ungated")
	}

	got := countSuppressedCalls(b, "model-a", "temp")
	if got != 2 {
		t.Fatalf("suppressed %d calls for (model-a,temp), want 2 (the failures recorded above, untouched by the other keys)", got)
	}
}

func TestCreateBreakerResetClearsFailuresNotJustCooldown(t *testing.T) {
	b := newCreateBreaker()
	b.recordFailure("model-a", "temp")
	b.recordFailure("model-a", "temp")
	b.recordFailure("model-a", "temp") // 3 failures: next cooldown would be 4 if the ladder weren't wiped

	b.reset("model-a", "temp")

	if b.gated("model-a", "temp") {
		t.Fatal("gated() = true immediately after reset, want false")
	}

	b.recordFailure("model-a", "temp")
	got := countSuppressedCalls(b, "model-a", "temp")
	if got != 1 {
		t.Fatalf("suppressed %d calls after reset + 1 failure, want 1 (ladder must restart, not resume)", got)
	}
}

func TestCreateBreakerConcurrentAccess(t *testing.T) {
	b := newCreateBreaker()
	models := []string{"model-a", "model-b"}
	roles := []string{"temp", "regular"}

	var wg sync.WaitGroup
	for workerIndex := range 8 {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			model := models[workerIndex%len(models)]
			role := roles[workerIndex%len(roles)]
			for i := range 200 {
				switch i % 3 {
				case 0:
					b.recordFailure(model, role)
				case 1:
					b.gated(model, role)
				case 2:
					b.reset(model, role)
				}
			}
		}(workerIndex)
	}
	wg.Wait()
}
