package limits

import (
	"maps"
	"sync"

	"devshard/cmd/gateway/chain"
)

type Capacity struct {
	mu         sync.RWMutex
	snapshot   chain.PhaseSnapshot // Preserved is already applied to CurrentWeights by the observer; not re-filtered here.
	available  func(participant, model string) bool
	membership map[string]map[string]float64
}

func NewCapacity(available func(participant, model string) bool) *Capacity {
	return &Capacity{
		available:  available,
		membership: map[string]map[string]float64{},
	}
}

func (c *Capacity) Update(s chain.PhaseSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot = s
}

func (c *Capacity) SetEscrowMembership(escrowID string, share map[string]float64) {
	clean := make(map[string]float64, len(share))
	maps.Copy(clean, share)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.membership[escrowID] = clean
}

func (c *Capacity) RemoveEscrow(escrowID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.membership, escrowID)
}

// ScaleFactor is the availability-filtered W_tot(model)/W_ref(model) via scaleFactor(); 0 when RequestsBlocked.
func (c *Capacity) ScaleFactor(model string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot.RequestsBlocked {
		return 0
	}
	current := c.sumAvailableLocked(c.currentWeightsLocked(model), model)
	full := sumWeights(c.fullWeightsLocked(model))
	return scaleFactor(current, full)
}

// EscrowWeight is escrowWeight() over this escrow's membership share and the per-model current weights.
func (c *Capacity) EscrowWeight(escrowID, model string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var availableForModel func(string) bool
	if c.available != nil {
		availableForModel = func(host string) bool { return c.available(host, model) }
	}
	return escrowWeight(c.currentWeightsLocked(model), c.membership[escrowID], availableForModel)
}

// currentWeightsLocked/fullWeightsLocked fall back to the generic view independently per side, so a missing full-by-model entry doesn't suppress a present current-by-model one.
func (c *Capacity) currentWeightsLocked(model string) map[string]float64 {
	if weights, ok := c.snapshot.CurrentWeightsByModel[model]; ok {
		return weights
	}
	return c.snapshot.CurrentWeights
}

func (c *Capacity) fullWeightsLocked(model string) map[string]float64 {
	if weights, ok := c.snapshot.FullWeightsByModel[model]; ok {
		return weights
	}
	return c.snapshot.FullWeights
}

func (c *Capacity) sumAvailableLocked(weights map[string]float64, model string) float64 {
	var sum float64
	for host, weight := range weights {
		if c.available != nil && !c.available(host, model) {
			continue
		}
		sum += weight
	}
	return sum
}

func sumWeights(weights map[string]float64) float64 {
	var sum float64
	for _, weight := range weights {
		sum += weight
	}
	return sum
}
