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

// hostShares[h] is slots(h,escrowID)/totalSlots(h) across every escrow h serves, so a participant
// shared by several escrows is split rather than counted once per escrow.
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

// ScaleFactor is the availability-filtered W_tot(model)/W_ref(model) via scaleFactor(); 0 when RequestsBlocked.
func (c *Capacity) ScaleFactor(model string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot.RequestsBlocked || !c.modelServedLocked(model) {
		return 0
	}
	current := c.sumAvailableLocked(c.currentWeightsLocked(model), model)
	full := sumWeights(c.fullWeightsLocked(model))
	return scaleFactor(current, full)
}

// EscrowWeight is escrowWeight() over this escrow's membership share and the per-model current weights.
// With no weight observed for either view the share alone stands in: a chain that has reported nothing
// yet is missing data, not zero capacity, and scoring it unusable would reject every live request.
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

// A host the chain has reported is a key in the view whatever its weight, so an empty view means the
// chain named nobody rather than that everybody weighs nothing.
func (c *Capacity) weightsUnobservedLocked(model string) bool {
	return len(c.currentWeightsLocked(model)) == 0 && len(c.fullWeightsLocked(model)) == 0
}

// Weights is ScaleFactor's numerator and denominator: the availability-filtered current weight and
// the steady-state baseline weight for one model.
func (c *Capacity) Weights(model string) (current, baseline float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.modelServedLocked(model) {
		return 0, 0
	}
	return c.sumAvailableLocked(c.currentWeightsLocked(model), model), sumWeights(c.fullWeightsLocked(model))
}

// A model absent from a populated by-model view is served by nobody, so it gets zero capacity instead
// of inheriting the generic all-model view; with no by-model view at all that view applies to everything.
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
