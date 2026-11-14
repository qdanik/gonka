package nonces

import "devshard/types"

// InvalidatedRecords is told once of each nonce whose record the chain resolved as invalidated.
type InvalidatedRecords interface {
	RecordInvalidated(escrowID string, nonce uint64)
}

// SetInvalidatedRecords wires who is told of an invalidated record. See README.md, "Boundaries".
func (n *Recorder) SetInvalidatedRecords(sink InvalidatedRecords) {
	if n != nil && sink != nil {
		n.invalidated.Store(&sink)
	}
}

func (n *Recorder) announceInvalidated(escrowID string, inferences map[uint64]*types.InferenceRecord) {
	sink := n.invalidated.Load()
	if sink == nil {
		return
	}
	announced := n.announcedInvalid[escrowID]
	for nonce, record := range inferences {
		if record == nil || record.Status != types.StatusInvalidated {
			continue
		}
		if _, already := announced[nonce]; already {
			continue
		}
		if announced == nil {
			announced = make(map[uint64]struct{})
			n.announcedInvalid[escrowID] = announced
		}
		announced[nonce] = struct{}{}
		(*sink).RecordInvalidated(escrowID, nonce)
	}
}
