package timeoutclaim

import (
	"devshard/host"
	"devshard/types"
)

// Payload builds the timeout vote payload of nonce from its live record and the prompt it was dispatched with, and returns nil when lookUpRecord finds no live record.
func Payload(lookUpRecord func(nonce uint64) (types.InferenceRecord, bool), nonce uint64, prompt []byte) *host.InferencePayload {
	record, live := lookUpRecord(nonce)
	if !live {
		return nil
	}
	return &host.InferencePayload{
		Prompt:      prompt,
		Model:       record.Model,
		InputLength: record.InputLength,
		MaxTokens:   record.MaxTokens,
		StartedAt:   record.StartedAt,
	}
}
