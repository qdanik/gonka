// Package accounting answers where every committed nonce went. Settlement credits a slot with
// assigned nonces minus recorded misses, so a nonce burned without work and a nonce sent but never
// finished both cost a participant's record. See gateway-operations.md, "Nonce accounting".
package accounting

import (
	"time"

	"devshard/types"
)

const SchemaVersion = 4

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
	Nonce           uint64
	RequestID       string
	Sent            bool
	Finished        bool
	Acknowledged    bool
	Usage           Usage
	Terminal        string
	Phase           Phase
	SlowReceipt     bool
	SlowChunk       bool
	ClockDrifted    bool
	SlowDecode      bool
	LogprobsDecoded bool
}

// CounterKey is one bucket of classified nonces. Only observed combinations exist and every dimension
// is bounded by its producer, so an escrow holds a few hundred buckets rather than a row per nonce.
type CounterKey struct {
	SlotID      uint32      `json:"slot_id"`
	Disposition Disposition `json:"disposition"`

	GhostReason     string `json:"ghost_reason,omitempty"`
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

// Pending is seen but not yet classifiable, Unobserved is the assigned range this gateway never saw,
// and Overcounted is the impossible case: more classified than the chain assigned.
type SlotRecord struct {
	EscrowID     string                 `json:"escrow_id"`
	SlotID       uint32                 `json:"slot_id"`
	Participant  string                 `json:"participant"`
	Assigned     uint64                 `json:"assigned_nonces"`
	Dispositions map[Disposition]uint64 `json:"dispositions"`
	ChainMissed  uint32                 `json:"protocol_misses"`
	ChainInvalid uint32                 `json:"protocol_invalid"`
	Pending      uint64                 `json:"pending_classification"`
	Unobserved   uint64                 `json:"unclassified"`
	Overcounted  uint64                 `json:"overclassified"`

	RequiredValidations  uint32 `json:"required_validations"`
	CompletedValidations uint32 `json:"completed_validations"`

	InFlight             uint64 `json:"in_flight"`
	InFlightRequests     uint64 `json:"in_flight_requests"`
	openRequests         map[string]struct{}
	UnresolvedChallenges uint64 `json:"unresolved_challenges"`
	ValidationsPerformed uint64 `json:"validations_performed"`
	TimeoutsApplied      uint64 `json:"timeouts_applied"`
	TimeoutPending       uint64 `json:"timeout_pending"`
	UnknownReasonTotal   uint64 `json:"unknown_reason_total"`
	CrossCheckError      uint64 `json:"-"`

	TimeoutOutcomes map[TimeoutOutcome]uint64 `json:"timeout_outcomes"`
}

// EscrowNonce is the furthest nonce one escrow has reached, so a reader can tell a participant that
// stopped receiving work from one whose escrow stopped advancing.
type EscrowNonce struct {
	EscrowID    string `json:"escrow_id"`
	LatestNonce uint64 `json:"latest_nonce"`
	Retired     bool   `json:"retired,omitempty"`
}

// CrossChecks holds both sides of every count the chain also keeps, and the size of their disagreement.
type CrossChecks struct {
	TimeoutApplied  uint64 `json:"timeout_applied"`
	HostMissed      uint64 `json:"host_missed"`
	RecordedInvalid uint64 `json:"recorded_invalid_transitions"`
	HostInvalid     uint64 `json:"host_invalid"`
	ErrorCount      uint64 `json:"error_count"`
}

type ParticipantRecord struct {
	SchemaVersion int       `json:"schema_version"`
	UpdatedAt     time.Time `json:"updated_at"`
	EpochIndex    uint64    `json:"epoch_index"`
	Participant   string    `json:"participant"`
	Model         string    `json:"model"`

	Assigned     uint64                 `json:"assigned_nonces"`
	Dispositions map[Disposition]uint64 `json:"dispositions"`
	ChainMissed  uint32                 `json:"protocol_misses"`
	ChainInvalid uint32                 `json:"protocol_invalid"`
	Pending      uint64                 `json:"pending_classification"`
	Unobserved   uint64                 `json:"unclassified"`
	Overcounted  uint64                 `json:"overclassified"`

	RequiredValidations  uint32                    `json:"required_validations"`
	CompletedValidations uint32                    `json:"completed_validations"`
	TimeoutOutcomes      map[TimeoutOutcome]uint64 `json:"timeout_outcomes"`

	LatestNonces         []EscrowNonce `json:"latest_nonces"`
	InFlight             uint64        `json:"in_flight"`
	InFlightRequests     uint64        `json:"in_flight_requests"`
	openRequests         map[string]struct{}
	UnresolvedChallenges uint64          `json:"unresolved_challenges"`
	ValidationsPerformed uint64          `json:"validations_performed"`
	TimeoutsApplied      uint64          `json:"timeouts_applied"`
	TimeoutPending       uint64          `json:"timeout_pending"`
	UnknownReasonTotal   uint64          `json:"unknown_reason_total"`
	CrossChecks          CrossChecks     `json:"cross_checks"`
	Capability           *HostCapability `json:"capability,omitempty"`

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
	ChainMissed  uint32                 `json:"protocol_misses"`
	ChainInvalid uint32                 `json:"protocol_invalid"`
	Pending      uint64                 `json:"pending_classification"`
	Unobserved   uint64                 `json:"unclassified"`
	Overcounted  uint64                 `json:"overclassified"`
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
