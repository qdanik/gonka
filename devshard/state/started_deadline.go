package state

import (
	"time"

	"devshard/types"
)

// StartedInferencesPastDeadline returns the ids of records the chain still settles and no race has
// claimed: started and past their own execution deadline by grace, counted from the executor's stamp, or
// from the start when the executor stamped none. At most budget ids come back, and a scan that finds nothing
// allocates nothing.
func (sm *StateMachine) StartedInferencesPastDeadline(now time.Time, grace time.Duration, budget int) []uint64 {
	if budget <= 0 {
		return nil
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	window := time.Duration(sm.state.Config.ExecutionTimeout)*time.Second + grace
	var due []uint64
	for id, record := range sm.state.Inferences {
		if record.Status != types.StatusStarted {
			continue
		}
		anchor := record.ConfirmedAt
		if anchor <= 0 {
			anchor = record.StartedAt
		}
		if now.Before(time.Unix(anchor, 0).Add(window)) {
			continue
		}
		if due == nil {
			due = make([]uint64, 0, budget)
		}
		if due = append(due, id); len(due) == budget {
			break
		}
	}
	return due
}
