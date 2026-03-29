package observability

import "context"

type InferenceHookStart struct {
	RequestID        string
	TraceID          string
	InferenceID      string
	RequesterAddress string
	Model            string
	Path             string
}

type InferenceHookFinish struct {
	RequestID        string
	TraceID          string
	InferenceID      string
	RequesterAddress string
	Model            string
	ResponseHash     string
	PromptTokens     uint64
	CompletionTokens uint64
	Path             string
}

type InferenceHookError struct {
	RequestID        string
	TraceID          string
	InferenceID      string
	RequesterAddress string
	Model            string
	Path             string
	Err              error
}

type InferenceHookService interface {
	OnStart(ctx context.Context, event InferenceHookStart)
	OnFinish(ctx context.Context, event InferenceHookFinish)
	OnError(ctx context.Context, event InferenceHookError)
}

type NoopInferenceHookService struct{}

func (NoopInferenceHookService) OnStart(context.Context, InferenceHookStart) {}

func (NoopInferenceHookService) OnFinish(context.Context, InferenceHookFinish) {}

func (NoopInferenceHookService) OnError(context.Context, InferenceHookError) {}
