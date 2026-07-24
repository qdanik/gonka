package chain

import "time"

// Chain wire strings, compared verbatim — never normalize.
type EpochPhase string

const (
	EpochPhaseInference           EpochPhase = "Inference"
	EpochPhasePoCGenerate         EpochPhase = "PoCGenerate"
	EpochPhasePoCGenerateWindDown EpochPhase = "PoCGenerateWindDown"
	EpochPhasePoCValidate         EpochPhase = "PoCValidate"
	EpochPhasePoCValidateWindDown EpochPhase = "PoCValidateWindDown"
)

type ConfirmationPoCPhase string

const (
	ConfirmationPoCInactive    ConfirmationPoCPhase = "CONFIRMATION_POC_INACTIVE"
	ConfirmationPoCGracePeriod ConfirmationPoCPhase = "CONFIRMATION_POC_GRACE_PERIOD"
	ConfirmationPoCGeneration  ConfirmationPoCPhase = "CONFIRMATION_POC_GENERATION"
	ConfirmationPoCValidation  ConfirmationPoCPhase = "CONFIRMATION_POC_VALIDATION"
	ConfirmationPoCCompleted   ConfirmationPoCPhase = "CONFIRMATION_POC_COMPLETED"
)

type BlockReason string

const (
	BlockReasonNone            BlockReason = ""
	BlockReasonPoC             BlockReason = "poc"
	BlockReasonConfirmationPoC BlockReason = "confirmation_poc"
)

// PhaseSnapshot is an immutable, published view of chain phase and participant
// state. The observer derives and publishes raw inputs only; subscribers derive
// scale, admission, and policy from them.
type PhaseSnapshot struct {
	BlockHeight            int64
	EpochSwitchBlockHeight int64
	EpochIndex             uint64
	EpochPhase             EpochPhase
	ConfirmationPoCPhase   ConfirmationPoCPhase
	// RequestsBlocked mirrors rawPoCBlockingState as-is; relaxed-mode overrides
	// are applied at the admission boundary, not by this observer.
	RequestsBlocked bool
	BlockReason     BlockReason

	CurrentWeights        map[string]float64 // participant addr -> weight; preservation-filtered, validation-capable-merged during PoC validation
	FullWeights           map[string]float64 // participant addr -> steady-state weight
	CurrentWeightsByModel map[string]map[string]float64
	FullWeightsByModel    map[string]map[string]float64
	Preserved             []string // PoC-preserved participant addrs; nil = not loaded, treat all preserved
	PreservedByModel      map[string][]string
	InferenceURLs         map[string]string // participant addr -> dapi base URL

	LastUpdatedAt time.Time
	LastError     string
}

// rawPoCBlockingState reports whether the raw chain phase blocks new requests
// and why; epoch-phase PoC states take precedence over confirmation-PoC states.
func rawPoCBlockingState(epochPhase EpochPhase, confirmationPhase ConfirmationPoCPhase) (bool, BlockReason) {
	switch epochPhase {
	case EpochPhasePoCGenerate, EpochPhasePoCGenerateWindDown, EpochPhasePoCValidate, EpochPhasePoCValidateWindDown:
		return true, BlockReasonPoC
	}
	switch confirmationPhase {
	case ConfirmationPoCGracePeriod, ConfirmationPoCGeneration, ConfirmationPoCValidation:
		return true, BlockReasonConfirmationPoC
	}
	return false, BlockReasonNone
}

// rawPoCValidationState reports whether the raw chain phase is a PoC validation stage.
func rawPoCValidationState(epochPhase EpochPhase, confirmationPhase ConfirmationPoCPhase) bool {
	switch epochPhase {
	case EpochPhasePoCValidate, EpochPhasePoCValidateWindDown:
		return true
	}
	return confirmationPhase == ConfirmationPoCValidation
}
