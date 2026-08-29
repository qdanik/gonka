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

// AllEpochPhases lists every phase, so a reader that must enumerate them (a per-phase metric series)
// stays beside the const block instead of restating it in another package.
func AllEpochPhases() []EpochPhase {
	return []EpochPhase{
		EpochPhaseInference,
		EpochPhasePoCGenerate,
		EpochPhasePoCGenerateWindDown,
		EpochPhasePoCValidate,
		EpochPhasePoCValidateWindDown,
	}
}

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

// AllBlockReasons lists every block reason, for the same reason AllEpochPhases exists.
func AllBlockReasons() []BlockReason {
	return []BlockReason{BlockReasonNone, BlockReasonPoC, BlockReasonConfirmationPoC}
}

// PhaseSnapshot is an immutable, published view of chain phase and participant state, folded from raw
// chain inputs only. The absent-value semantics of RequestsBlocked, Preserved and MaxNonce are
// load-bearing: see README.md, "What the chain observer provides".
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
