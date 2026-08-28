// Package logkey is the gateway's log vocabulary: every key a log line may carry, declared once and
// referenced by name, so a rename reaches every emitter through the compiler.
package logkey

const (
	Request = "request"
	Escrow  = "escrow"
	Nonce   = "nonce"
	Model   = "model"
	Host    = "host"
	Slot    = "slot"
	Role    = "role"
	Reason  = "reason"
	Outcome = "outcome"
	Action  = "action"
	Rewound = "rewound"
	Error   = "error"
)

const (
	ReceiptTimeoutMS       = "receipt_timeout_ms"
	FirstTokenFloorMS      = "first_token_floor_ms"
	FirstTokenCeilingMS    = "first_token_ceiling_ms"
	InterChunkStallMS      = "inter_chunk_stall_ms"
	LoserGraceMS           = "loser_grace_ms"
	MaxSpeculativeAttempts = "max_speculative_attempts"
)

const hostLabelLength = 8

func ShortHost(address string) string {
	if len(address) <= hostLabelLength {
		return address
	}
	return address[len(address)-hostLabelLength:]
}

// What an attempt delivered and how long each stage took.
const (
	Terminal       = "terminal"
	NonceFinished  = "nonce_finished"
	StateDivergent = "state_divergent"
	PhaseAborted   = "phase_aborted"
	ContentChunks  = "content_chunks"
	StreamChunks   = "stream_chunks"
	OutputBytes    = "output_bytes"
	UsageTokens    = "usage_tokens"
	DroppedEvents  = "dropped_events"
	MaxGapMS       = "max_gap_ms"
	UpstreamStatus = "upstream_status"
	UpstreamBody   = "upstream_body"
	MaxGapAtChunk  = "max_gap_at_chunk"
	MeanGapMS      = "mean_gap_ms"
	ReceiptMS      = "receipt_ms"
	FirstTokenMS   = "first_token_ms"
	FirstContentMS = "first_content_ms"
	AttemptMS      = "attempt_ms"
)

// What the client asked for and what it got.
const (
	Stream            = "stream"
	InputTokens       = "input_tokens"
	OutputTokens      = "output_tokens"
	Bytes             = "bytes"
	Terminated        = "terminated"
	DurationMS        = "duration_ms"
	DeliverError      = "deliver_error"
	CatchUpError      = "catch_up_error"
	Served            = "served"
	HostClockOffsetMS = "host_clock_offset_ms"
	HostReceiptMS     = "host_receipt_ms"
)

// The chain and the escrow set behind the request.
const (
	Epoch          = "epoch"
	Height         = "height"
	SwitchHeight   = "switch_height"
	Phase          = "phase"
	Tx             = "tx"
	Settler        = "settler"
	Created        = "created"
	Retired        = "retired"
	Promoted       = "promoted"
	Recorded       = "recorded"
	Used           = "used"
	InFlight       = "in_flight"
	Attempts       = "attempts"
	EscrowBuilders = "escrow_builders"
)

// Process and transport.
const (
	Port        = "port"
	Addr        = "addr"
	StorageDir  = "storage_dir"
	Version     = "version"
	RoutePrefix = "route_prefix"
	Subsystem   = "subsystem"
	Route       = "route"
	Method      = "method"
	Status      = "status"
)
