package accounting

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

// Aggregation happens here rather than in storage: every total above the counters is derivable, so a
// stored total could only ever disagree with its own parts.
func (b *Book) Query(filter QueryFilter) []ParticipantRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()

	records := make(map[participantIdentity]*ParticipantRecord)
	for _, escrowID := range slices.Sorted(maps.Keys(b.escrows)) {
		escrow := b.escrows[escrowID]
		if !filter.admitsEscrow(escrowID, escrow.metadata) {
			continue
		}
		for _, slot := range escrow.slots(escrowID) {
			if filter.Participant != "" && slot.Participant != filter.Participant {
				continue
			}
			identity := escrow.identityOf(slot.Participant)
			record, known := records[identity]
			if !known {
				record = &ParticipantRecord{
					SchemaVersion: SchemaVersion,
					UpdatedAt:     b.updatedAt,
					EpochIndex:    identity.epoch,
					Participant:   identity.participant,
					Model:         identity.model,
					Dispositions:  make(map[Disposition]uint64),
				}
				records[identity] = record
			}
			record.absorb(slot)
		}
		for key, count := range escrow.counters {
			identity := escrow.identityOf(escrow.participantOf(key.SlotID))
			if record, known := records[identity]; known {
				record.Counters = append(record.Counters, CounterRecord{EscrowID: escrowID, CounterKey: key, Count: count})
			}
		}
	}

	aggregated := make([]ParticipantRecord, 0, len(records))
	for _, identity := range slices.SortedFunc(maps.Keys(records), compareParticipantIdentity) {
		record := records[identity]
		slices.SortFunc(record.Counters, compareCounterRecord)
		record.Findings = findingsFor(*record)
		aggregated = append(aggregated, *record)
	}
	return aggregated
}

func (f QueryFilter) admitsEscrow(escrowID string, metadata EscrowMetadata) bool {
	if f.EpochIndex != 0 && metadata.CreationEpoch != f.EpochIndex {
		return false
	}
	if f.Model != "" && metadata.Model != f.Model {
		return false
	}
	return len(f.EscrowIDs) == 0 || slices.Contains(f.EscrowIDs, escrowID)
}

func (b *Book) Epochs(filter QueryFilter) []EpochSummary {
	summaries := make(map[uint64]*EpochSummary)
	for _, record := range b.Query(filter) {
		summary, known := summaries[record.EpochIndex]
		if !known {
			summary = &EpochSummary{
				SchemaVersion: SchemaVersion,
				UpdatedAt:     record.UpdatedAt,
				EpochIndex:    record.EpochIndex,
				Dispositions:  make(map[Disposition]uint64),
			}
			summaries[record.EpochIndex] = summary
		}
		summary.absorb(record)
	}

	ordered := make([]EpochSummary, 0, len(summaries))
	for _, epoch := range slices.Sorted(maps.Keys(summaries)) {
		ordered = append(ordered, *summaries[epoch])
	}
	return ordered
}

func (s *EpochSummary) absorb(record ParticipantRecord) {
	s.Participants++
	s.Assigned += record.Assigned
	s.ChainMissed += record.ChainMissed
	s.ChainInvalid += record.ChainInvalid
	s.Pending += record.Pending
	s.Unobserved += record.Unobserved
	s.Overcounted += record.Overcounted
	for disposition, count := range record.Dispositions {
		s.Dispositions[disposition] += count
	}
}

func (b *Book) EscrowIDs() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return slices.Sorted(maps.Keys(b.escrows))
}

// Epoch is part of the key because a rotation leaves escrows of adjacent epochs live at once, and
// summing those would report one epoch's work under another's index.
type participantIdentity struct {
	participant string
	model       string
	epoch       uint64
}

func compareParticipantIdentity(left, right participantIdentity) int {
	if left.epoch != right.epoch {
		return cmp.Compare(left.epoch, right.epoch)
	}
	if byParticipant := strings.Compare(left.participant, right.participant); byParticipant != 0 {
		return byParticipant
	}
	return strings.Compare(left.model, right.model)
}

func (e *escrowLedger) identityOf(participant string) participantIdentity {
	return participantIdentity{
		participant: participant,
		model:       e.metadata.Model,
		epoch:       e.metadata.CreationEpoch,
	}
}

func compareCounterRecord(left, right CounterRecord) int {
	if byEscrow := strings.Compare(left.EscrowID, right.EscrowID); byEscrow != 0 {
		return byEscrow
	}
	if left.SlotID != right.SlotID {
		return int(left.SlotID) - int(right.SlotID)
	}
	return strings.Compare(string(left.Disposition), string(right.Disposition))
}

func (r *ParticipantRecord) absorb(slot SlotRecord) {
	r.Assigned += slot.Assigned
	r.ChainMissed += slot.ChainMissed
	r.ChainInvalid += slot.ChainInvalid
	r.Pending += slot.Pending
	r.Unobserved += slot.Unobserved
	r.Overcounted += slot.Overcounted
	for disposition, count := range slot.Dispositions {
		r.Dispositions[disposition] += count
	}
	r.Slots = append(r.Slots, slot)
}

func (e *escrowLedger) participantOf(slotID uint32) string {
	if int(slotID) >= len(e.metadata.Slots) {
		return ""
	}
	return e.metadata.Slots[slotID].ValidatorAddress
}

func (e *escrowLedger) slots(escrowID string) []SlotRecord {
	groupSize := uint32(len(e.metadata.Slots))
	counted := make(map[uint32]uint64, groupSize)
	dispositions := make(map[uint32]map[Disposition]uint64, groupSize)
	for key, count := range e.counters {
		counted[key.SlotID] += count
		if dispositions[key.SlotID] == nil {
			dispositions[key.SlotID] = make(map[Disposition]uint64)
		}
		dispositions[key.SlotID][key.Disposition] += count
	}
	pending := make(map[uint32]uint64, groupSize)
	for nonce, record := range e.nonces {
		if record.counted == nil {
			pending[e.slotOf(nonce)]++
		}
	}

	records := make([]SlotRecord, 0, groupSize)
	for slotID := uint32(0); slotID < groupSize; slotID++ {
		slot := SlotRecord{
			EscrowID:     escrowID,
			SlotID:       slotID,
			Participant:  e.metadata.Slots[slotID].ValidatorAddress,
			Assigned:     assignedForSlot(e.latest, groupSize, slotID),
			Dispositions: dispositions[slotID],
			Pending:      pending[slotID],
		}
		if slot.Dispositions == nil {
			slot.Dispositions = make(map[Disposition]uint64)
		}
		if stats, observed := e.hostStats[slotID]; observed {
			slot.ChainMissed, slot.ChainInvalid = stats.Missed, stats.Invalid
		}
		accounted := counted[slotID] + slot.Pending
		if accounted > slot.Assigned {
			slot.Overcounted = accounted - slot.Assigned
		} else {
			slot.Unobserved = slot.Assigned - accounted
		}
		records = append(records, slot)
	}
	return records
}
