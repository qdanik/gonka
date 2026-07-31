package registry

// membership is satisfied by *limits.Capacity.
type membership interface {
	SetEscrowMembership(escrowID string, hostShares map[string]float64)
	RemoveEscrow(escrowID string)
}

// exhaustion is satisfied by *escrow.Manager. It is called from the request path, once per spent
// candidate per pick, so it must mark and return rather than do I/O.
type exhaustion interface {
	OnBalanceExhausted(escrowID string)
}

// slotCounts counts the slots each participant occupies in one escrow: a validator holding several
// slots repeats in the per-slot key list.
func slotCounts(perSlotKeys []string) map[string]int {
	counts := make(map[string]int, len(perSlotKeys))
	for _, participant := range perSlotKeys {
		if participant == "" {
			continue
		}
		counts[participant]++
	}
	return counts
}

// hostShares is slots(participant, escrow)/totalSlots(participant), the per-host multiplier
// limits.Capacity expects. Raw slot counts would count a participant once per escrow it serves
// instead of splitting it, over-weighting every escrow that shares a host.
func hostShares(escrowSlots, totalSlots map[string]int) map[string]float64 {
	shares := make(map[string]float64, len(escrowSlots))
	for participant, count := range escrowSlots {
		total := totalSlots[participant]
		if count <= 0 || total <= 0 {
			continue
		}
		shares[participant] = float64(count) / float64(total)
	}
	return shares
}
