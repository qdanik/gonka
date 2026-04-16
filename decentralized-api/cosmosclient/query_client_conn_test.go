package cosmosclient

import (
	"context"
	"net"
	"testing"

	sdkclient "github.com/cosmos/cosmos-sdk/client"
	infertypes "github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type observedQueryServer struct {
	infertypes.UnimplementedQueryServer
	traceparent string
}

func (s *observedQueryServer) Params(ctx context.Context, _ *infertypes.QueryParamsRequest) (*infertypes.QueryParamsResponse, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		values := md.Get("traceparent")
		if len(values) > 0 {
			s.traceparent = values[0]
		}
	}
	return nil, grpcstatus.Error(grpccodes.NotFound, "key not found")
}

func setupQueryTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider()
	provider.RegisterSpanProcessor(recorder)
	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func startObservedQueryTestServer(t *testing.T, srv infertypes.QueryServer) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	infertypes.RegisterQueryServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	require.NoError(t, err)
	cleanup := func() {
		server.Stop()
		_ = listener.Close()
		_ = conn.Close()
	}
	return conn, cleanup
}

func TestObservedQueryClientConn_InvokeReturnsOriginalErrorAndFormatsSpan(t *testing.T) {
	recorder := setupQueryTraceRecorder(t)
	server := &observedQueryServer{}
	conn, cleanup := startObservedQueryTestServer(t, server)
	defer cleanup()

	ctx := sdkclient.Context{}.WithGRPCClient(conn)
	queryClient := infertypes.NewQueryClient(observedQueryClientConn{Context: ctx})

	_, err := queryClient.Params(context.Background(), &infertypes.QueryParamsRequest{})
	require.Error(t, err)
	require.Equal(t, grpccodes.NotFound, grpcstatus.Code(err))
	require.EqualError(t, err, "rpc error: code = NotFound desc = key not found")
	require.NotContains(t, err.Error(), "rpc error: code = NotFound desc = rpc error")
	require.NotEmpty(t, server.traceparent)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "chain.grpc.query", spans[0].Name())
	require.Equal(t, otelcodes.Error, spans[0].Status().Code)
	require.Contains(t, spans[0].Status().Description, "grpc query: service=inference.inference.Query, method=Params")
	require.Contains(t, spans[0].Status().Description, "rpc error: code = NotFound desc = key not found")

	attributes := spans[0].Attributes()
	var rpcStatus string
	for _, attr := range attributes {
		if string(attr.Key) == "rpc.grpc.status_code" {
			rpcStatus = attr.Value.AsString()
		}
	}
	require.Equal(t, "NotFound", rpcStatus)
}