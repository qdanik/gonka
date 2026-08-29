package accounting

import (
	"cmp"
	"maps"
	"slices"
	"strings"
	"time"
)

// maxEventsPerEscrow bounds the newest-wins ring one escrow keeps. A miss or an invalid is rare and an
// escrow dies with its epoch, so the bound caps a pathological run rather than trimming normal traffic.
const maxEventsPerEscrow = 256

type ProtocolKind string

const (
	ProtocolTimeoutApplied ProtocolKind = "timeout_applied"
	ProtocolInvalidated    ProtocolKind = "invalidated"
)

// protocolEvent is one chain-applied verdict against a slot, kept on the escrow that owns the nonce.
type protocolEvent struct {
	nonce     uint64
	slotID    uint32
	kind      ProtocolKind
	requestID string
	at        time.Time
}

// ProtocolEventRecord resolves one event against the escrow and participant it belongs to. The
// counters say how many verdicts a host took; this says which nonce and which client request took them.
type ProtocolEventRecord struct {
	EscrowID    string       `json:"escrow_id"`
	Participant string       `json:"participant"`
	Model       string       `json:"model"`
	Nonce       uint64       `json:"nonce"`
	SlotID      uint32       `json:"slot_id"`
	Kind        ProtocolKind `json:"kind"`
	RequestID   string       `json:"request_id,omitempty"`
	At          time.Time    `json:"at"`
}

func (e *escrowLedger) appendEvent(nonce uint64, kind ProtocolKind, at time.Time) {
	requestID := ""
	if record, known := e.nonces[nonce]; known {
		requestID = record.requestID
	}
	e.events = append(e.events, protocolEvent{
		nonce: nonce, slotID: e.slotOf(nonce), kind: kind, requestID: requestID, at: at.UTC(),
	})
	if overflow := len(e.events) - maxEventsPerEscrow; overflow > 0 {
		e.events = append(e.events[:0], e.events[overflow:]...)
	}
}

// Events returns the verdicts the filter admits, newest first.
func (b *Book) Events(filter QueryFilter) []ProtocolEventRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()

	records := make([]ProtocolEventRecord, 0)
	for _, escrowID := range slices.Sorted(maps.Keys(b.escrows)) {
		escrow := b.escrows[escrowID]
		if !filter.admitsEscrow(escrowID, escrow.metadata) {
			continue
		}
		for _, event := range escrow.events {
			participant := escrow.participantOf(event.slotID)
			if filter.Participant != "" && participant != filter.Participant {
				continue
			}
			records = append(records, ProtocolEventRecord{
				EscrowID:    escrowID,
				Participant: participant,
				Model:       escrow.metadata.Model,
				Nonce:       event.nonce,
				SlotID:      event.slotID,
				Kind:        event.kind,
				RequestID:   event.requestID,
				At:          event.at,
			})
		}
	}
	slices.SortStableFunc(records, compareProtocolEvent)
	return records
}

func compareProtocolEvent(first, second ProtocolEventRecord) int {
	if !first.At.Equal(second.At) {
		return second.At.Compare(first.At)
	}
	if first.EscrowID != second.EscrowID {
		return strings.Compare(first.EscrowID, second.EscrowID)
	}
	return cmp.Compare(second.Nonce, first.Nonce)
}
