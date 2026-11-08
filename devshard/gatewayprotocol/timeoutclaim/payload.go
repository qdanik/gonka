package timeoutclaim

import (
	"devshard/host"
	"devshard/types"
	"devshard/user"
)

// PayloadFromParams builds the timeout vote payload from the params the nonce was dispatched with.
func PayloadFromParams(params user.InferenceParams) *host.InferencePayload {
	return &host.InferencePayload{
		Prompt:      params.Prompt,
		Model:       params.Model,
		InputLength: params.InputLength,
		MaxTokens:   params.MaxTokens,
		StartedAt:   params.StartedAt,
	}
}

// PayloadFromRecord builds the timeout vote payload from the nonce's committed record and the prompt it was dispatched with.
func PayloadFromRecord(record types.InferenceRecord, prompt []byte) *host.InferencePayload {
	return &host.InferencePayload{
		Prompt:      prompt,
		Model:       record.Model,
		InputLength: record.InputLength,
		MaxTokens:   record.MaxTokens,
		StartedAt:   record.StartedAt,
	}
}
