package scheduler

import (
	"math"
	"sync"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/share"
)

// loadTableMaxAge is how long a model's table is kept while nothing it was built from changes. See docs/proposals/gateway-weight-proportional-load.md, "Cadence".
const loadTableMaxAge = time.Second

// loadTable is one model's targets, published whole: a pick reads it and never mutates it.
type loadTable struct {
	builtAt time.Time
	epoch   uint64
	escrows int
	balance bool
	hosts   map[string]share.Share
	slots   map[string]float64
}

// surplus is the host's offered load above its target in nonces; a host outside the table has none.
func (table *loadTable) surplus(participant string) float64 {
	if table == nil {
		return 0
	}
	return table.hosts[participant].Surplus
}

type loadTables struct {
	mu      sync.Mutex
	byModel map[string]*loadTable
}

// HostShare is one host's row of the share ledger and its targets, as reported.
type HostShare struct {
	HostLoadMode   string
	Weight         float64
	WeightShare    float64
	SlotShare      float64
	TargetShare    float64
	OfferedShare   float64
	Surplus        float64
	ServeRatio     float64
	CapacityFactor float64
	Totals         share.Totals
}

// HostShare reads the ledger row of one host; it reports the targets the mode in force would use, and the weight targets under off.
func (s *Scheduler) HostShare(participant, model string) (HostShare, bool) {
	mode := s.hostLoadMode(model)
	table := s.loadTableFor(model, mode == config.HostLoadModeBalance, s.escrows.Candidates(model), s.snapshots.Snapshot())
	row, known := table.hosts[participant]
	if !known {
		return HostShare{}, false
	}
	return HostShare{
		HostLoadMode: mode, Weight: s.weightOf(participant, model),
		WeightShare: row.WeightShare, SlotShare: table.slots[participant], TargetShare: row.TargetShare,
		OfferedShare: row.OfferedShare, Surplus: row.Surplus, ServeRatio: row.ServeRatio, CapacityFactor: row.CapacityFactor,
		Totals: s.shares.Totals(model, participant),
	}, true
}

// HostLoadMode is the mode in force for the model.
func (s *Scheduler) HostLoadMode(model string) string { return s.hostLoadMode(model) }

func (s *Scheduler) weightOf(participant, model string) float64 {
	if s.weights == nil {
		return 0
	}
	return s.weights(participant, model)
}

func (s *Scheduler) roomOf(participant, model string) float64 {
	if s.rooms == nil {
		return math.Inf(1)
	}
	return s.rooms(participant, model)
}

// loadTableFor rebuilds the model's table once a second and when its epoch, mode or set of accepting escrows changes.
func (s *Scheduler) loadTableFor(model string, balance bool, candidates []Escrow, snapshot chain.PhaseSnapshot) *loadTable {
	now := s.now()
	s.loads.mu.Lock()
	defer s.loads.mu.Unlock()
	if table := s.loads.byModel[model]; table != nil && table.balance == balance && table.epoch == snapshot.EpochIndex &&
		table.escrows == len(candidates) && now.Sub(table.builtAt) < loadTableMaxAge {
		return table
	}
	table := s.buildLoadTable(model, balance, candidates, snapshot, now)
	if s.loads.byModel == nil {
		s.loads.byModel = map[string]*loadTable{}
	}
	s.loads.byModel[model] = table
	return table
}

func (s *Scheduler) buildLoadTable(model string, balance bool, candidates []Escrow, snapshot chain.PhaseSnapshot, now time.Time) *loadTable {
	settings := s.settingsInForce()
	shadowed := shadowedOn(settings, model)
	slotCounts := map[string]float64{}
	var allSlots float64
	for _, candidate := range candidates {
		if candidate.Session == nil {
			continue
		}
		for _, slot := range candidate.Session.SlotParticipants() {
			allSlots++
			if shadowed == nil || !shadowed(slot) {
				slotCounts[slot]++
			}
		}
	}
	inputs := make(map[string]share.Input, len(slotCounts))
	for participant := range slotCounts {
		inputs[participant] = share.Input{
			Weight: s.weightOf(participant, model),
			Counts: s.shares.Decayed(model, participant),
			Room:   s.roomOf(participant, model),
		}
	}
	table := &loadTable{
		builtAt: now, epoch: snapshot.EpochIndex, escrows: len(candidates), balance: balance,
		hosts: share.Targets(balance, inputs, settings.For(model).Perf.FailureRateMinVolume),
		slots: make(map[string]float64, len(slotCounts)),
	}
	for participant, count := range slotCounts {
		table.slots[participant] = count / allSlots
	}
	return table
}
