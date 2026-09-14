package journal

import (
	"devshard/cmd/gateway/accounting"
	"devshard/types"
)

// DiffFactKind and DiffFact live in accounting, so the ledger takes them without importing journal.
type (
	DiffFactKind = accounting.DiffFactKind
	DiffFact     = accounting.DiffFact
)

const (
	DiffFactValidation     = accounting.DiffFactValidation
	DiffFactInvalidVerdict = accounting.DiffFactInvalidVerdict
	DiffFactAppliedTimeout = accounting.DiffFactAppliedTimeout
)

// diffFacts runs under the session lock, so a diff with no ledger fact allocates nothing. See README.md, "Order and the locks it takes".
func diffFacts(diff *types.Diff) []DiffFact {
	factCount := 0
	for _, tx := range diff.Txs {
		if validation := tx.GetValidation(); validation != nil {
			factCount++
			if !validation.Valid {
				factCount++
			}
		} else if tx.GetTimeoutInference() != nil {
			factCount++
		}
	}
	if factCount == 0 {
		return nil
	}
	facts := make([]DiffFact, 0, factCount)
	for _, tx := range diff.Txs {
		switch validation, timeout := tx.GetValidation(), tx.GetTimeoutInference(); {
		case validation != nil:
			facts = append(facts, DiffFact{Kind: DiffFactValidation, Nonce: validation.InferenceId, ValidatorSlot: validation.ValidatorSlot})
			if !validation.Valid {
				facts = append(facts, DiffFact{Kind: DiffFactInvalidVerdict, Nonce: validation.InferenceId, ValidatorSlot: validation.ValidatorSlot})
			}
		case timeout != nil:
			facts = append(facts, DiffFact{Kind: DiffFactAppliedTimeout, Nonce: timeout.InferenceId})
		}
	}
	return facts
}
