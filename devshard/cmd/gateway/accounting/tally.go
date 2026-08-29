package accounting

// The three levels are the same shape, so each is the sum of the rows below it. See README.md.
type nonceTotals struct {
	Assigned     uint64                 `json:"assigned_nonces"`
	Dispositions map[Disposition]uint64 `json:"dispositions"`
	ChainMissed  uint32                 `json:"protocol_misses"`
	ChainInvalid uint32                 `json:"protocol_invalid"`
	Pending      uint64                 `json:"pending_classification"`
	Unobserved   uint64                 `json:"unclassified"`
	Overcounted  uint64                 `json:"overclassified"`
}

func (t *nonceTotals) add(other nonceTotals) {
	t.Assigned += other.Assigned
	t.ChainMissed += other.ChainMissed
	t.ChainInvalid += other.ChainInvalid
	t.Pending += other.Pending
	t.Unobserved += other.Unobserved
	t.Overcounted += other.Overcounted
	t.Dispositions = addInto(t.Dispositions, other.Dispositions)
}

// What the host was doing rather than how its nonces came out.
type hostActivity struct {
	RequiredValidations  uint32 `json:"required_validations"`
	CompletedValidations uint32 `json:"completed_validations"`
	InFlight             uint64 `json:"in_flight"`
	InFlightRequests     uint64 `json:"in_flight_requests"`
	openRequests         map[string]struct{}
	UnresolvedChallenges uint64 `json:"unresolved_challenges"`
	ValidationsPerformed uint64 `json:"validations_performed"`
	TimeoutsApplied      uint64 `json:"timeouts_applied"`
}

// Open requests are unioned, not summed: one request spends several nonces at once.
func (a *hostActivity) add(other hostActivity) {
	a.RequiredValidations += other.RequiredValidations
	a.CompletedValidations += other.CompletedValidations
	a.InFlight += other.InFlight
	a.UnresolvedChallenges += other.UnresolvedChallenges
	a.ValidationsPerformed += other.ValidationsPerformed
	a.TimeoutsApplied += other.TimeoutsApplied
	for requestID := range other.openRequests {
		if a.openRequests == nil {
			a.openRequests = make(map[string]struct{}, len(other.openRequests))
		}
		a.openRequests[requestID] = struct{}{}
	}
	a.InFlightRequests = uint64(len(a.openRequests))
}

// Folded once on the owning slot and summed upwards, so two rows cannot classify one nonce differently.
type timeoutTally struct {
	TimeoutPending     uint64                    `json:"timeout_pending"`
	UnknownReasonTotal uint64                    `json:"unknown_reason_total"`
	TimeoutOutcomes    map[TimeoutOutcome]uint64 `json:"timeout_outcomes"`
}

func (t *timeoutTally) fold(key CounterKey, count uint64) {
	switch outcome, settled := timeoutOutcomeOf(key.TimeoutAction, key.TimeoutReason); {
	case settled:
		if t.TimeoutOutcomes == nil {
			t.TimeoutOutcomes = make(map[TimeoutOutcome]uint64)
		}
		t.TimeoutOutcomes[outcome] += count
	case awaitingTimeout(key):
		t.TimeoutPending += count
	}
	if namesNoReason(key) {
		t.UnknownReasonTotal += count
	}
}

func (t *timeoutTally) add(other timeoutTally) {
	t.TimeoutPending += other.TimeoutPending
	t.UnknownReasonTotal += other.UnknownReasonTotal
	t.TimeoutOutcomes = addInto(t.TimeoutOutcomes, other.TimeoutOutcomes)
}

// addInto returns target so a nil map can be filled without every caller repeating the check.
func addInto[Key comparable](target, source map[Key]uint64) map[Key]uint64 {
	for key, count := range source {
		if target == nil {
			target = make(map[Key]uint64, len(source))
		}
		target[key] += count
	}
	return target
}
