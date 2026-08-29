package limits

import (
	"maps"
	"sync"

	"devshard/cmd/gateway/chain"
)

// Capacity scores escrows against the chain's weight views; the observer has already applied Preserved.
type Capacity struct {
	mu         sync.RWMutex
	snapshot   chain.PhaseSnapshot
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

// hostShares[h] is slots(h,escrowID)/totalSlots(h), so a shared participant is split, not counted twice.
func (c *Capacity) SetEscrowMembership(escrowID string, hostShares map[string]float64) {
	clean := make(map[string]float64, len(hostShares))
	maps.Copy(clean, hostShares)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.membership[escrowID] = clean
}

func (c *Capacity) RemoveEscrow(escrowID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.membership, escrowID)
}

// ScaleFactor takes the EFFECTIVE blocking state, never the chain's raw one. See README.md, "The capacity model".
func (c *Capacity) ScaleFactor(model string, blocked bool) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if blocked || !c.modelServedLocked(model) {
		return 0
	}
	current := c.sumAvailableLocked(c.currentWeightsLocked(model), model)
	full := sumWeights(c.fullWeightsLocked(model))
	return scaleFactor(current, full)
}

// EscrowWeight falls back to the membership share. See capacity.md, "Two fail-safes with opposite directions".
func (c *Capacity) EscrowWeight(escrowID, model string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.modelServedLocked(model) {
		return 0
	}
	var availableForModel func(string) bool
	if c.available != nil {
		availableForModel = func(host string) bool { return c.available(host, model) }
	}
	shares := c.membership[escrowID]
	if c.weightsUnobservedLocked(model) {
		return availableShare(shares, availableForModel)
	}
	return escrowWeight(c.currentWeightsLocked(model), shares, availableForModel)
}

// WeightsUnobserved makes the membership-share fallback visible, since it serves requests silently.
func (c *Capacity) WeightsUnobserved(model string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.modelServedLocked(model) && c.weightsUnobservedLocked(model)
}

// An empty view means the chain named nobody, not that everybody weighs nothing.
func (c *Capacity) weightsUnobservedLocked(model string) bool {
	return len(c.currentWeightsLocked(model)) == 0 && len(c.fullWeightsLocked(model)) == 0
}

// Weights is ScaleFactor's numerator and denominator for one model.
func (c *Capacity) Weights(model string) (current, baseline float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.modelServedLocked(model) {
		return 0, 0
	}
	return c.sumAvailableLocked(c.currentWeightsLocked(model), model), sumWeights(c.fullWeightsLocked(model))
}

// A model absent from a populated by-model view is served by nobody, so it must not inherit the generic view.
func (c *Capacity) modelServedLocked(model string) bool {
	if len(c.snapshot.CurrentWeightsByModel) == 0 && len(c.snapshot.FullWeightsByModel) == 0 {
		return true
	}
	if _, ok := c.snapshot.CurrentWeightsByModel[model]; ok {
		return true
	}
	_, ok := c.snapshot.FullWeightsByModel[model]
	return ok
}

// The two sides fall back independently, so a missing full-by-model entry can't suppress a present current one.
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
