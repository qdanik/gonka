package accounting

// HostShare is a host's row of the share ledger and the targets of host_load_mode, read at query time like the window half of capability. See docs/proposals/gateway-weight-proportional-load.md.
type HostShare struct {
	HostLoadMode   string  `json:"host_load_mode"`
	Weight         float64 `json:"weight"`
	WeightShare    float64 `json:"weight_share"`
	SlotShare      float64 `json:"slot_share"`
	TargetShare    float64 `json:"target_share"`
	OfferedShare   float64 `json:"offered_share"`
	Surplus        float64 `json:"surplus"`
	ServeRatio     float64 `json:"serve_ratio"`
	CapacityFactor float64 `json:"capacity_factor"`

	Assigned       uint64 `json:"assigned"`
	Offered        uint64 `json:"offered"`
	Served         uint64 `json:"served"`
	Failed         uint64 `json:"failed"`
	Passed         uint64 `json:"passed"`
	Excused        uint64 `json:"excused"`
	ProbesAnswered uint64 `json:"probes_answered"`
	ProbesMissed   uint64 `json:"probes_missed"`
	VotesOwed      uint64 `json:"votes_owed"`
	PassUnvoted    uint64 `json:"pass_unvoted"`
}

// ShareFunc answers false for a host the ledger holds nothing for.
type ShareFunc func(participant, model string) (HostShare, bool)

func attachShares(records []ParticipantRecord, lookup ShareFunc) {
	if lookup == nil {
		return
	}
	for i := range records {
		if row, held := lookup(records[i].Participant, records[i].Model); held {
			records[i].Share = &row
		}
	}
}
