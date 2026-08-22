package accounting

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"devshard/types"
)

type Snapshot struct {
	SchemaVersion int              `json:"schema_version"`
	UpdatedAt     time.Time        `json:"updated_at"`
	Escrows       []EscrowSnapshot `json:"escrows"`
}

type EscrowSnapshot struct {
	Metadata    EscrowMetadata             `json:"metadata"`
	LatestNonce uint64                     `json:"latest_nonce"`
	Retired     bool                       `json:"retired"`
	HostStats   map[uint32]types.HostStats `json:"host_stats,omitempty"`
	Counters    []PersistedCounter         `json:"counters,omitempty"`
	Nonces      []PersistedNonce           `json:"nonces,omitempty"`
}

type PersistedNonce struct {
	Nonce           uint64 `json:"nonce"`
	Sent            bool   `json:"sent"`
	Acknowledged    bool   `json:"acknowledged"`
	Usage           Usage  `json:"usage"`
	TimeoutKind     string `json:"timeout_kind,omitempty"`
	TimeoutAction   string `json:"timeout_action,omitempty"`
	TimeoutReason   string `json:"timeout_reason,omitempty"`
	Terminal        string `json:"terminal,omitempty"`
	Phase           Phase  `json:"phase,omitempty"`
	SlowReceipt     bool   `json:"slow_receipt,omitempty"`
	SlowChunk       bool   `json:"slow_chunk,omitempty"`
	ClockDrifted    bool   `json:"clock_drifted,omitempty"`
	SlowDecode      bool   `json:"slow_decode,omitempty"`
	LogprobsDecoded bool   `json:"logprobs_decoded,omitempty"`
}

type PersistedCounter struct {
	CounterKey
	Count uint64 `json:"count"`
}

func (b *Book) Snapshot() Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snapshot := Snapshot{
		SchemaVersion: SchemaVersion,
		UpdatedAt:     b.updatedAt,
		Escrows:       make([]EscrowSnapshot, 0, len(b.escrows)),
	}
	for _, escrowID := range slices.Sorted(maps.Keys(b.escrows)) {
		escrow := b.escrows[escrowID]
		stored := EscrowSnapshot{
			Metadata:    escrow.metadata,
			LatestNonce: escrow.latest,
			Retired:     escrow.retired,
			HostStats:   make(map[uint32]types.HostStats, len(escrow.hostStats)),
			Counters:    make([]PersistedCounter, 0, len(escrow.counters)),
		}
		maps.Copy(stored.HostStats, escrow.hostStats)
		for key, count := range escrow.counters {
			stored.Counters = append(stored.Counters, PersistedCounter{CounterKey: key, Count: count})
		}
		for _, nonce := range slices.Sorted(maps.Keys(escrow.nonces)) {
			record := escrow.nonces[nonce]
			if !revisable(record) {
				continue
			}
			stored.Nonces = append(stored.Nonces, PersistedNonce{
				Nonce:           nonce,
				Sent:            record.sent,
				Acknowledged:    record.acknowledged,
				Usage:           record.usage,
				TimeoutKind:     record.timeoutKind,
				TimeoutAction:   record.timeoutAction,
				TimeoutReason:   record.timeoutReason,
				Terminal:        record.terminal,
				Phase:           record.phase,
				SlowReceipt:     record.slowReceipt,
				SlowChunk:       record.slowChunk,
				ClockDrifted:    record.clockDrifted,
				SlowDecode:      record.slowDecode,
				LogprobsDecoded: record.logprobsDecoded,
			})
		}
		slices.SortFunc(stored.Counters, func(left, right PersistedCounter) int {
			return compareCounterRecord(
				CounterRecord{CounterKey: left.CounterKey},
				CounterRecord{CounterKey: right.CounterKey},
			)
		})
		snapshot.Escrows = append(snapshot.Escrows, stored)
	}
	return snapshot
}

// Restore replaces the ledger with a stored snapshot. A snapshot from another schema is refused
// rather than half-read: counters whose dimensions moved would be silently misfiled.
func (b *Book) Restore(snapshot Snapshot) error {
	if snapshot.SchemaVersion != SchemaVersion {
		return fmt.Errorf("accounting: snapshot schema %d is not %d", snapshot.SchemaVersion, SchemaVersion)
	}
	restored := make(map[string]*escrowLedger, len(snapshot.Escrows))
	for _, stored := range snapshot.Escrows {
		if stored.Metadata.EscrowID == "" || len(stored.Metadata.Slots) == 0 {
			return fmt.Errorf("accounting: snapshot holds an escrow without id or slots")
		}
		escrow := &escrowLedger{
			metadata:  stored.Metadata,
			latest:    stored.LatestNonce,
			retired:   stored.Retired,
			hostStats: make(map[uint32]types.HostStats, len(stored.HostStats)),
			counters:  make(map[CounterKey]uint64, len(stored.Counters)),
			nonces:    make(map[uint64]*nonceRecord),
		}
		maps.Copy(escrow.hostStats, stored.HostStats)
		for _, counter := range stored.Counters {
			escrow.counters[counter.CounterKey] += counter.Count
		}
		for _, stored := range stored.Nonces {
			record := &nonceRecord{
				sent:            stored.Sent,
				acknowledged:    stored.Acknowledged,
				usage:           stored.Usage,
				timeoutKind:     stored.TimeoutKind,
				timeoutAction:   stored.TimeoutAction,
				timeoutReason:   stored.TimeoutReason,
				terminal:        stored.Terminal,
				phase:           stored.Phase,
				slowReceipt:     stored.SlowReceipt,
				slowChunk:       stored.SlowChunk,
				clockDrifted:    stored.ClockDrifted,
				slowDecode:      stored.SlowDecode,
				logprobsDecoded: stored.LogprobsDecoded,
			}
			escrow.nonces[stored.Nonce] = record
			if key, settled := classify(escrow.slotOf(stored.Nonce), record); settled {
				counted := key
				record.counted = &counted
				continue
			}
			record.timeoutAction = TimeoutActionAbandoned
			escrow.reclassify(stored.Nonce, record)
		}
		restored[stored.Metadata.EscrowID] = escrow
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.escrows = restored
	b.updatedAt = snapshot.UpdatedAt
	return nil
}

// revisable says whether a nonce's disposition can still move. A burned or finished nonce cannot: no
// later fact lifts it. An unfinished one can, because the protocol may finish it after the race that
// gave up on it, and one still awaiting its timeout has no disposition yet.
func revisable(record *nonceRecord) bool {
	if record.counted == nil {
		return true
	}
	return record.counted.Disposition == DispositionUnfinishedRefused ||
		record.counted.Disposition == DispositionUnfinishedExecution
}
