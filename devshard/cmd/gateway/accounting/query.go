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
					nonceTotals:   nonceTotals{Dispositions: make(map[Disposition]uint64)},
				}
				records[identity] = record
			}
			record.absorb(slot)
			if last := len(record.LatestNonces); last == 0 || record.LatestNonces[last-1].EscrowID != escrowID {
				record.LatestNonces = append(record.LatestNonces, EscrowNonce{
					EscrowID:    escrowID,
					LatestNonce: escrow.latest,
					Retired:     escrow.retired,
				})
			}
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
				nonceTotals:   nonceTotals{Dispositions: make(map[Disposition]uint64)},
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
	s.nonceTotals.add(record.nonceTotals)
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
	r.nonceTotals.add(slot.nonceTotals)
	r.hostActivity.add(slot.hostActivity)
	r.timeoutTally.add(slot.timeoutTally)
	r.CrossChecks.TimeoutApplied += slot.TimeoutOutcomes[TimeoutApplied]
	r.CrossChecks.HostMissed += uint64(slot.ChainMissed)
	r.CrossChecks.HostInvalid += uint64(slot.ChainInvalid)
	r.CrossChecks.RecordedInvalid += slot.rejected
	// Per slot of one escrow: summing both sides first lets a surplus in one hide a shortfall in another.
	r.CrossChecks.ErrorCount += absDiff(slot.TimeoutsApplied, uint64(slot.ChainMissed)) +
		absDiff(slot.rejected, uint64(slot.ChainInvalid)) + slot.Overcounted
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
	tallies := make(map[uint32]timeoutTally, groupSize)
	for key, count := range e.counters {
		counted[key.SlotID] += count
		if dispositions[key.SlotID] == nil {
			dispositions[key.SlotID] = make(map[Disposition]uint64)
		}
		dispositions[key.SlotID][key.Disposition] += count
		tally := tallies[key.SlotID]
		tally.fold(key, count)
		tallies[key.SlotID] = tally
	}
	pending := make(map[uint32]uint64, groupSize)
	inFlight := make(map[uint32]uint64, groupSize)
	openRequests := make(map[uint32]map[string]struct{}, groupSize)
	for nonce, record := range e.nonces {
		switch {
		case record.counted != nil:
		case record.sent && !record.finished:
			slotID := e.slotOf(nonce)
			inFlight[slotID]++
			if record.requestID != "" {
				if openRequests[slotID] == nil {
					openRequests[slotID] = make(map[string]struct{})
				}
				openRequests[slotID][record.requestID] = struct{}{}
			}
		default:
			pending[e.slotOf(nonce)]++
		}
	}

	records := make([]SlotRecord, 0, groupSize)
	for slotID := range groupSize {
		slot := SlotRecord{
			EscrowID:    escrowID,
			SlotID:      slotID,
			Participant: e.metadata.Slots[slotID].ValidatorAddress,
			rejected:    e.rejected[slotID],
			nonceTotals: nonceTotals{
				Assigned:     assignedForSlot(e.latest, groupSize, slotID),
				Dispositions: dispositions[slotID],
				Pending:      pending[slotID],
			},
			hostActivity: hostActivity{
				InFlight:             inFlight[slotID],
				openRequests:         openRequests[slotID],
				InFlightRequests:     uint64(len(openRequests[slotID])),
				UnresolvedChallenges: e.challenged[slotID],
				ValidationsPerformed: e.validations[slotID],
				TimeoutsApplied:      e.timeouts[slotID],
			},
			timeoutTally: tallies[slotID],
		}
		if slot.Dispositions == nil {
			slot.Dispositions = make(map[Disposition]uint64)
		}
		if stats, observed := e.hostStats[slotID]; observed {
			slot.ChainMissed, slot.ChainInvalid = stats.Missed, stats.Invalid
			slot.RequiredValidations, slot.CompletedValidations = stats.RequiredValidations, stats.CompletedValidations
		}
		accounted := counted[slotID] + slot.Pending + slot.InFlight
		if accounted > slot.Assigned {
			slot.Overcounted = accounted - slot.Assigned
		} else {
			slot.Unobserved = slot.Assigned - accounted
		}
		records = append(records, slot)
	}
	return records
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
