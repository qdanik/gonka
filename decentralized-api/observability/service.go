package observability

import (
	"context"
	"net/http"
)

var Default = NewService(nil)
var Inference = Default.Inference()
var Chain = Default.Chain()

type Service struct {
	inference *InferenceService
	chain     *ChainService
}

type InferenceService struct {
	trace InferenceTraceService
	hooks InferenceHookService
}

type ChainService struct {
	trace ChainTraceService
}

func NewService(hooks InferenceHookService) *Service {
	if hooks == nil {
		hooks = NoopInferenceHookService{}
	}

	return &Service{
		inference: &InferenceService{
			trace: NewInferenceTraceService(),
			hooks: hooks,
		},
		chain: &ChainService{
			trace: NewChainTraceService(),
		},
	}
}

func (s *Service) Inference() *InferenceService {
	if s == nil {
		return NewService(nil).Inference()
	}
	return s.inference
}

func (s *Service) Chain() *ChainService {
	if s == nil {
		return NewService(nil).Chain()
	}
	return s.chain
}

func (s *InferenceService) Trace() InferenceTraceService {
	if s == nil {
		return NewInferenceTraceService()
	}
	return s.trace
}

func (s *InferenceService) Hooks() InferenceHookService {
	if s == nil {
		return NoopInferenceHookService{}
	}
	return s.hooks
}

func (s *InferenceService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return s.Trace().ExtractRequestContext(ctx, headers)
}

func (s *InferenceService) InjectRequestContext(ctx context.Context, headers http.Header) {
	s.Trace().InjectRequestContext(ctx, headers)
}

func (s *InferenceService) StartRequest(ctx context.Context, method string) (context.Context, *Operation) {
	return s.Trace().StartRequest(ctx, method)
}

func (s *InferenceService) SetRequestIdentity(op *Operation, model string, requester string) {
	s.Trace().SetRequestIdentity(op, model, requester)
}

func (s *InferenceService) MarkTransferPath(op *Operation) {
	s.Trace().MarkTransferPath(op)
}

func (s *InferenceService) MarkExecutorPath(op *Operation, inferenceID string) {
	s.Trace().MarkExecutorPath(op, inferenceID)
}

func (s *InferenceService) StartTransfer(ctx context.Context, model string, requester string) (context.Context, *Operation) {
	return s.Trace().StartTransfer(ctx, model, requester)
}

func (s *InferenceService) StartForwardExecutor(ctx context.Context, model string, executorAddress string, executorURL string) (context.Context, *Operation) {
	return s.Trace().StartForwardExecutor(ctx, model, executorAddress, executorURL)
}

func (s *InferenceService) StartExecutor(ctx context.Context, inferenceID string, model string, requester string, transferAddress string) (context.Context, *Operation) {
	return s.Trace().StartExecutor(ctx, inferenceID, model, requester, transferAddress)
}

func (s *InferenceService) StartMLNodeExecution(ctx context.Context, inferenceID string, model string) (context.Context, *Operation) {
	return s.Trace().StartMLNodeExecution(ctx, inferenceID, model)
}

func (s *InferenceService) SetMLNodeTarget(op *Operation, nodeID string, nodeURL string) {
	s.Trace().SetMLNodeTarget(op, nodeID, nodeURL)
}

func (s *InferenceService) StartFinishSubmission(ctx context.Context, inferenceID string, executorAddress string, model string) (context.Context, *Operation) {
	return s.Trace().StartFinishSubmission(ctx, inferenceID, executorAddress, model)
}

func (s *InferenceService) SetModel(op *Operation, model string) {
	s.Trace().SetModel(op, model)
}

func (s *InferenceService) SetResponseHash(op *Operation, responseHash string) {
	s.Trace().SetResponseHash(op, responseHash)
}

func (s *InferenceService) StartValidationEvent(ctx context.Context, inferenceCount int) (context.Context, *Operation) {
	return s.Trace().StartValidationEvent(ctx, inferenceCount)
}

func (s *InferenceService) StartValidationSample(ctx context.Context, candidateCount int) (context.Context, *Operation) {
	return s.Trace().StartValidationSample(ctx, candidateCount)
}

func (s *InferenceService) SetSampledCount(op *Operation, sampledCount int) {
	s.Trace().SetSampledCount(op, sampledCount)
}

func (s *InferenceService) StartValidationExecution(ctx context.Context, inferenceID string, model string, epochID int64, revalidation bool) (context.Context, *Operation) {
	return s.Trace().StartValidationExecution(ctx, inferenceID, model, epochID, revalidation)
}

func (s *InferenceService) AddValidationRetry(op *Operation, attempt int, err error) {
	s.Trace().AddValidationRetry(op, attempt, err)
}

func (s *InferenceService) SetValidationResult(op *Operation, result any) {
	s.Trace().SetValidationResult(op, result)
}

func (s *InferenceService) StartPayloadRetrieval(ctx context.Context, inferenceID string, executorAddress string, epochID int64) (context.Context, *Operation) {
	return s.Trace().StartPayloadRetrieval(ctx, inferenceID, executorAddress, epochID)
}

func (s *InferenceService) AddPayloadAttempt(op *Operation, attempt int) {
	s.Trace().AddPayloadAttempt(op, attempt)
}

func (s *InferenceService) StartPayloadFetch(ctx context.Context, requestURL string, validatorAddress string, epochID int64) (context.Context, *Operation) {
	return s.Trace().StartPayloadFetch(ctx, requestURL, validatorAddress, epochID)
}

func (s *InferenceService) StartValidationMLNode(ctx context.Context, inferenceID string, model string, nodeID string) (context.Context, *Operation) {
	return s.Trace().StartValidationMLNode(ctx, inferenceID, model, nodeID)
}

func (s *InferenceService) StartCompareLogits(ctx context.Context, inferenceID string) (context.Context, *Operation) {
	return s.Trace().StartCompareLogits(ctx, inferenceID)
}

func (s *InferenceService) SetSimilarity(op *Operation, similarity float64) {
	s.Trace().SetSimilarity(op, similarity)
}

func (s *InferenceService) SetHTTPStatus(op *Operation, statusCode int) {
	s.Trace().SetHTTPStatus(op, statusCode)
}

func (s *ChainService) Trace() ChainTraceService {
	if s == nil {
		return NewChainTraceService()
	}
	return s.trace
}

func (s *ChainService) StartTxBroadcast(ctx context.Context, msgType string, batchSize int) (context.Context, *Operation) {
	return s.Trace().StartTxBroadcast(ctx, msgType, batchSize)
}

func (s *ChainService) SetTxHash(op *Operation, txHash string) {
	s.Trace().SetTxHash(op, txHash)
}

func (s *ChainService) SetTxResult(op *Operation, txHash string, code uint32) {
	s.Trace().SetTxResult(op, txHash, code)
}

func (s *ChainService) StartTxConfirmation(ctx context.Context, txHash string) (context.Context, *Operation) {
	return s.Trace().StartTxConfirmation(ctx, txHash)
}

func (s *ChainService) StartStoreQuery(ctx context.Context, storeKey string, withProof bool, height int64) (context.Context, *Operation) {
	return s.Trace().StartStoreQuery(ctx, storeKey, withProof, height)
}
