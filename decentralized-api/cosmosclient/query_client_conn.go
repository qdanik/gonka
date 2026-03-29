package cosmosclient

import (
	"context"
	"decentralized-api/observability"
	"strings"

	"github.com/cosmos/cosmos-sdk/client"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type observedQueryClientConn struct {
	client.Context
}

func newObservedQueryClientConn(ctx client.Context) observedQueryClientConn {
	return observedQueryClientConn{Context: ctx}
}

func (c observedQueryClientConn) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) (err error) {
	service, rpcMethod := splitGRPCMethod(method)
	queryCtx, queryOp := observability.Chain.StartGRPCQuery(ctx, service, rpcMethod)
	defer func() {
		observability.Chain.SetRPCStatus(queryOp, status.Code(err).String())
		queryOp.FinishErr(&err)
	}()
	queryCtx = injectGRPCTraceContext(queryCtx)
	err = c.Context.Invoke(queryCtx, method, args, reply, opts...)
	return err
}

func (c observedQueryClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.Context.NewStream(ctx, desc, method, opts...)
}

func splitGRPCMethod(fullMethod string) (string, string) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	if trimmed == "" {
		return "unknown", "unknown"
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return trimmed, "unknown"
	}
	return parts[0], parts[1]
}

type grpcMetadataCarrier struct {
	metadata.MD
}

func (c grpcMetadataCarrier) Get(key string) string {
	values := c.MD.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c grpcMetadataCarrier) Set(key string, value string) {
	c.MD.Set(key, value)
}

func (c grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c.MD))
	for key := range c.MD {
		keys = append(keys, key)
	}
	return keys
}

func injectGRPCTraceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	otel.GetTextMapPropagator().Inject(ctx, grpcMetadataCarrier{MD: md})
	return metadata.NewOutgoingContext(ctx, md)
}
