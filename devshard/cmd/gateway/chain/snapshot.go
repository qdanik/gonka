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

// PhaseSnapshot is an immutable, published view of chain phase and participant state. The observer folds
// raw inputs only: RequestsBlocked mirrors rawPoCBlockingState as-is, and relaxed mode overrides it at the
// admission boundary. A nil Preserved means not loaded, so everyone counts as preserved; a 0 MaxNonce means
// not yet fetched, so the nonce cap is disabled rather than escrows deactivated on missing data.
type PhaseSnapshot struct {
	BlockHeight            int64
	EpochSwitchBlockHeight int64
	EpochIndex             uint64
	EpochPhase             EpochPhase
	ConfirmationPoCPhase   ConfirmationPoCPhase
	RequestsBlocked        bool
	BlockReason            BlockReason

	CurrentWeights        map[string]float64
	FullWeights           map[string]float64
	CurrentWeightsByModel map[string]map[string]float64
	FullWeightsByModel    map[string]map[string]float64
	Preserved             []string
	PreservedByModel      map[string][]string
	InferenceURLs         map[string]string

	MaxNonce uint64

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

func rawPoCValidationState(epochPhase EpochPhase, confirmationPhase ConfirmationPoCPhase) bool {
	switch epochPhase {
	case EpochPhasePoCValidate, EpochPhasePoCValidateWindDown:
		return true
	}
	return confirmationPhase == ConfirmationPoCValidation
}
