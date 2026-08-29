package registry

// membership is satisfied by *limits.Capacity.
type membership interface {
	SetEscrowMembership(escrowID string, hostShares map[string]float64)
	RemoveEscrow(escrowID string)
}

// exhaustion is satisfied by *escrow.Manager. Called from the request path, so it must mark and return, not do I/O.
type exhaustion interface {
	OnBalanceExhausted(escrowID, reason string)
}

// publications is called while the registry holds its lock, so an implementation must return without doing work.
type publications interface {
	EscrowPublished(escrowID, model string)
}

// slotCounts counts each participant's slots: one holding several repeats in the per-slot key list.
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

// hostShares is slots(participant, escrow)/totalSlots(participant). See routing.md, "Membership: what the capacity model is told".
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
