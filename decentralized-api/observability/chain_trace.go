package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ChainTraceService interface {
	StartTxBroadcast(ctx context.Context, msgType string, batchSize int) (context.Context, *Operation)
	SetTxHash(op *Operation, txHash string)
	SetTxResult(op *Operation, txHash string, code uint32)
	StartTxConfirmation(ctx context.Context, txHash string) (context.Context, *Operation)
	StartStoreQuery(ctx context.Context, storeKey string, withProof bool, height int64) (context.Context, *Operation)
}

type otelChainTraceService struct{}

func NewChainTraceService() ChainTraceService {
	return otelChainTraceService{}
}

func (otelChainTraceService) StartTxBroadcast(ctx context.Context, msgType string, batchSize int) (context.Context, *Operation) {
	attrs := []attribute.KeyValue{
		attribute.String("blockchain.system", "cosmos"),
		attribute.String("tx.msg_type", msgType),
	}
	if batchSize > 0 {
		attrs = append(attrs, attribute.Int("tx.batch_size", batchSize))
	}
	return StartOperation(
		ctx,
		"decentralized-api.chain",
		"chain.tx.broadcast",
		trace.SpanKindClient,
		attrs,
		nil,
	)
}

func (otelChainTraceService) SetTxHash(op *Operation, txHash string) {
	if op == nil || txHash == "" {
		return
	}
	op.Span().SetAttributes(attribute.String("tx.hash", txHash))
}

func (otelChainTraceService) SetTxResult(op *Operation, txHash string, code uint32) {
	if op == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.Int64("tx.result_code", int64(code))}
	if txHash != "" {
		attrs = append(attrs, attribute.String("tx.hash", txHash))
	}
	op.Span().SetAttributes(attrs...)
}

func (otelChainTraceService) StartTxConfirmation(ctx context.Context, txHash string) (context.Context, *Operation) {
	attrs := []attribute.KeyValue{attribute.String("blockchain.system", "cosmos")}
	if txHash != "" {
		attrs = append(attrs, attribute.String("tx.hash", txHash))
	}
	return StartOperation(
		ctx,
		"decentralized-api.chain",
		"chain.tx.confirmation",
		trace.SpanKindClient,
		attrs,
		nil,
	)
}

func (otelChainTraceService) StartStoreQuery(ctx context.Context, storeKey string, withProof bool, height int64) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.chain",
		"chain.store.query",
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("blockchain.system", "cosmos"),
			attribute.String("store.key", storeKey),
			attribute.Bool("query.with_proof", withProof),
			attribute.Int64("query.height", height),
		},
		nil,
	)
}
