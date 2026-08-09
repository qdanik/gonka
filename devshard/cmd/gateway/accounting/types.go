// Package accounting answers where every committed nonce went. Settlement credits a slot with
// assigned nonces minus recorded misses, so a nonce burned without work and a nonce sent but never
// finished both cost a participant's record. See gateway-operations.md, "Nonce accounting".
package accounting

import (
	"time"

	"devshard/types"
)

const SchemaVersion = 3

type Disposition string

const (
	DispositionGhost                Disposition = "ghost"
	DispositionFinishedUsed         Disposition = "finished_used"
	DispositionFinishedUnused       Disposition = "finished_unused"
	DispositionFinishedUsageUnknown Disposition = "finished_usage_unknown"
	DispositionUnfinishedRefused    Disposition = "unfinished_refused"
	DispositionUnfinishedExecution  Disposition = "unfinished_execution"
)

type Usage string

const (
	UsageWinner  Usage = "winner"
	UsageLoser   Usage = "loser"
	UsageUnknown Usage = "unknown"
)

type Phase string

const (
	PhaseNormal Phase = "normal"
	PhasePoC    Phase = "poc"
)

const (
	SlowReceipt  = 2500 * time.Millisecond
	SlowChunkGap = 5 * time.Second
	ClockDrift   = 5 * time.Second
	SlowDecode   = 40 * time.Millisecond
)

type EscrowMetadata struct {
	EscrowID      string
	Model         string
	CreationEpoch uint64
	Slots         []types.SlotAssignment
}

// Attempt is one nonce's share of a race outcome. Sent is false for a nonce the race committed but
// never dispatched, which is a stranded nonce rather than a ghost: a ghost is a deliberate burn.
type Attempt struct {
	Nonce        uint64
	Sent         bool
	Finished     bool
	Acknowledged bool
	Usage        Usage
	Terminal     string
	Phase        Phase
	SlowReceipt  bool
	SlowChunk    bool
	ClockDrifted bool
	SlowDecode   bool
}

// CounterKey is one bucket of classified nonces. Only observed combinations exist and every dimension
// is bounded by its producer, so an escrow holds a few hundred buckets rather than a row per nonce.
type CounterKey struct {
	SlotID      uint32      `json:"slot_id"`
	Disposition Disposition `json:"disposition"`

	GhostReason   string `json:"ghost_reason,omitempty"`
	TimeoutKind   string `json:"timeout_kind,omitempty"`
	TimeoutAction string `json:"timeout_action,omitempty"`
	TimeoutReason string `json:"timeout_reason,omitempty"`
	Terminal      string `json:"terminal,omitempty"`
	Phase         Phase  `json:"phase,omitempty"`
	SlowReceipt   bool   `json:"slow_receipt,omitempty"`
	SlowChunk     bool   `json:"slow_chunk,omitempty"`
	ClockDrifted  bool   `json:"clock_drifted,omitempty"`
	SlowDecode    bool   `json:"slow_decode,omitempty"`
}

// Pending is seen but not yet classifiable, Unobserved is the assigned range this gateway never saw,
// and Overcounted is the impossible case: more classified than the chain assigned.
type SlotRecord struct {
	EscrowID     string                 `json:"escrow_id"`
	SlotID       uint32                 `json:"slot_id"`
	Participant  string                 `json:"participant"`
	Assigned     uint64                 `json:"assigned_nonces"`
	Dispositions map[Disposition]uint64 `json:"dispositions"`
	ChainMissed  uint32                 `json:"chain_missed"`
	ChainInvalid uint32                 `json:"chain_invalid"`
	Pending      uint64                 `json:"pending"`
	Unobserved   uint64                 `json:"unobserved"`
	Overcounted  uint64                 `json:"overcounted"`
}

type ParticipantRecord struct {
	SchemaVersion int       `json:"schema_version"`
	UpdatedAt     time.Time `json:"updated_at"`
	EpochIndex    uint64    `json:"epoch_index"`
	Participant   string    `json:"participant"`
	Model         string    `json:"model"`

	Assigned     uint64                 `json:"assigned_nonces"`
	Dispositions map[Disposition]uint64 `json:"dispositions"`
	ChainMissed  uint32                 `json:"chain_missed"`
	ChainInvalid uint32                 `json:"chain_invalid"`
	Pending      uint64                 `json:"pending"`
	Unobserved   uint64                 `json:"unobserved"`
	Overcounted  uint64                 `json:"overcounted"`

	Counters []CounterRecord `json:"counters"`
	Slots    []SlotRecord    `json:"slots"`
	Findings []Finding       `json:"findings"`
}

type EpochSummary struct {
	SchemaVersion int       `json:"schema_version"`
	UpdatedAt     time.Time `json:"updated_at"`
	EpochIndex    uint64    `json:"epoch_index"`
	Participants  int       `json:"participants"`

	Assigned     uint64                 `json:"assigned_nonces"`
	Dispositions map[Disposition]uint64 `json:"dispositions"`
	ChainMissed  uint32                 `json:"chain_missed"`
	ChainInvalid uint32                 `json:"chain_invalid"`
	Pending      uint64                 `json:"pending"`
	Unobserved   uint64                 `json:"unobserved"`
	Overcounted  uint64                 `json:"overcounted"`
}

// A zero field constrains nothing. Epoch zero is unconstrained rather than selectable, matching the
// rest of the gateway, which reads a zero epoch index as "not known yet".
type QueryFilter struct {
	EpochIndex  uint64
	Model       string
	Participant string
	EscrowIDs   []string
}

type CounterRecord struct {
	EscrowID string `json:"escrow_id"`
	CounterKey
	Count uint64 `json:"count"`
}
