package accounting

// nonceTotals is the range of nonces a row was assigned and how those nonces came out. One slot of one
// escrow, one host across all of them, and one epoch across all hosts are the same shape, so each is
// the sum of the rows below it.
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

// hostActivity is what the host was doing rather than how its nonces came out: the duties the protocol
// handed it, the work still open, and the checks it performed.
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

// add unions the open requests instead of summing them: one request spends several nonces, so slots of
// the same escrow hold it at once and summing would count the client twice.
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

// timeoutTally is the timeout view of a set of counters, folded once per counter on the slot that owns
// it and summed upwards, so a host's row and its per-escrow rows cannot classify one nonce differently.
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
