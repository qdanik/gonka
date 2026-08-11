package registry

import (
	"testing"

	"common/completionapi"

	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/scheduler"
	"devshard/user"
)

// The bug this pins was a join, not a rule: the filters produced one budget, the chain demanded
// another, and nothing exercised the two together. Every path that commits a nonce runs here.
func TestWhatTheFiltersProduceTheChainAccepts(t *testing.T) {
	for _, body := range []string{
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":1}`,
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":8}`,
		`{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":8}`,
		`{"messages":[{"role":"user","content":"x"}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			normalized, err := filters.NormalizeRequest([]byte(body), filters.Options{
				RoutedModel:      "qwen",
				DefaultMaxTokens: 3072,
				MaxTokensCap:     4096,
			})
			if err != nil {
				t.Fatalf("NormalizeRequest() = %v, want nil", err)
			}
			if normalized.MaxTokens < completionapi.MinTokensFloor {
				t.Fatalf("MaxTokens = %d, under the floor %d before the chain ever sees it", normalized.MaxTokens, completionapi.MinTokensFloor)
			}

			stream, _, _ := newLiveStream(t, 1, 1, 1)
			prepared, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
				return scheduler.NonceIntent{Commit: true, Params: user.InferenceParams{
					Model:       "qwen",
					Prompt:      normalized.Body,
					InputLength: uint64(len(normalized.Body)),
					MaxTokens:   normalized.MaxTokens,
				}}
			})
			if err != nil {
				t.Fatalf("committing what the filters produced = %v, want nil", err)
			}
			if prepared == nil {
				t.Fatal("no nonce was committed")
			}
		})
	}
}

func TestAGhostBurnStillCommits(t *testing.T) {
	stream, _, _ := newLiveStream(t, 1, 1, 1)

	prepared, err := stream.Advance(func(scheduler.HostBinding) scheduler.NonceIntent {
		return scheduler.NonceIntent{Commit: true, Params: stream.ghostParams()}
	})

	if err != nil {
		t.Fatalf("burning a nonce = %v, want nil", err)
	}
	if prepared == nil {
		t.Fatal("no nonce was burned")
	}
}
