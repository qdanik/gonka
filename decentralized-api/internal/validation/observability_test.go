package validation

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"decentralized-api/apiconfig"
	"decentralized-api/broker"
	"decentralized-api/chainphase"
	"decentralized-api/cosmosclient"
	"decentralized-api/cosmosclient/tx_manager"
	"decentralized-api/mlnodeclient"
	"decentralized-api/participant"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	sdkclient "github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	infertypes "github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type validationTestTxManager struct {
	clientContext sdkclient.Context
}

func (m *validationTestTxManager) SendTransactionAsyncWithRetry(sdk.Msg, ...int64) (*sdk.TxResponse, error) {
	return &sdk.TxResponse{}, nil
}

func (m *validationTestTxManager) SendTransactionAsyncNoRetry(sdk.Msg) (*sdk.TxResponse, error) {
	return &sdk.TxResponse{}, nil
}

func (m *validationTestTxManager) SendBatchAsyncWithRetry([]sdk.Msg, ...int64) error {
	return nil
}

func (m *validationTestTxManager) SendTransactionSyncNoRetry(proto.Message) (*ctypes.ResultTx, error) {
	return nil, nil
}

func (m *validationTestTxManager) BroadcastMessages(string, ...sdk.Msg) (*sdk.TxResponse, time.Time, error) {
	return &sdk.TxResponse{}, time.Time{}, nil
}

func (m *validationTestTxManager) GetClientContext() sdkclient.Context {
	return m.clientContext
}

func (m *validationTestTxManager) GetKeyring() *keyring.Keyring {
	return nil
}

func (m *validationTestTxManager) GetApiAccount() apiconfig.ApiAccount {
	return apiconfig.ApiAccount{}
}

func (m *validationTestTxManager) Status(context.Context) (*ctypes.ResultStatus, error) {
	return nil, nil
}

func (m *validationTestTxManager) BankBalances(context.Context, string) ([]sdk.Coin, error) {
	return nil, nil
}

func (m *validationTestTxManager) GetJetStream() nats.JetStreamContext {
	return nil
}

type validationQueryCall struct {
	method  string
	traceID trace.TraceID
	spanID  trace.SpanID
}

type validationQueryServer struct {
	infertypes.UnimplementedQueryServer

	mu       sync.Mutex
	calls    []validationQueryCall
	infCh    chan struct{}
	modelID  string
}

func (s *validationQueryServer) GetInferenceValidationParameters(ctx context.Context, req *infertypes.QueryGetInferenceValidationParametersRequest) (*infertypes.QueryGetInferenceValidationParametersResponse, error) {
	s.recordCall(ctx, "GetInferenceValidationParameters")
	return &infertypes.QueryGetInferenceValidationParametersResponse{
		ValidatorPowers: []*infertypes.ValidatorPower{{
			Power:      100,
			EpochIndex: 1,
		}},
		Details: []*infertypes.InferenceValidationDetails{{
			EpochId:        1,
			InferenceId:    req.Ids[0],
			ExecutorId:     "executor-address",
			ExecutorPower:  1,
			Model:          s.modelID,
			TotalPower:     101,
		}},
	}, nil
}

func (s *validationQueryServer) Params(ctx context.Context, req *infertypes.QueryParamsRequest) (*infertypes.QueryParamsResponse, error) {
	s.recordCall(ctx, "Params")
	validationParams := infertypes.DefaultValidationParams()
	validationParams.MinValidationAverage = infertypes.DecimalFromFloat(1.0)
	validationParams.MaxValidationAverage = infertypes.DecimalFromFloat(1.0)
	return &infertypes.QueryParamsResponse{
		Params: infertypes.Params{
			ValidationParams: validationParams,
		},
	}, nil
}

func (s *validationQueryServer) Inference(ctx context.Context, req *infertypes.QueryGetInferenceRequest) (*infertypes.QueryGetInferenceResponse, error) {
	s.recordCall(ctx, "Inference")
	select {
	case s.infCh <- struct{}{}:
	default:
	}
	return nil, grpcStatus.Error(grpcCodes.Internal, "stop after capture")
}

func (s *validationQueryServer) Calls() []validationQueryCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]validationQueryCall, len(s.calls))
	copy(result, s.calls)
	return result
}

func (s *validationQueryServer) recordCall(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	traceID, spanID := parseTraceparent(md)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, validationQueryCall{method: method, traceID: traceID, spanID: spanID})
}

type validationBrokerChainBridge struct {
	participantAddress string
	modelID            string
	nodeID             string
}

func (b *validationBrokerChainBridge) GetHardwareNodes() (*infertypes.QueryHardwareNodesResponse, error) {
	return &infertypes.QueryHardwareNodesResponse{}, nil
}

func (b *validationBrokerChainBridge) SubmitHardwareDiff(diff *infertypes.MsgSubmitHardwareDiff) error {
	return nil
}

func (b *validationBrokerChainBridge) GetBlockHash(height int64) (string, error) {
	return "", nil
}

func (b *validationBrokerChainBridge) GetGovernanceModels() (*infertypes.QueryModelsAllResponse, error) {
	return &infertypes.QueryModelsAllResponse{
		Model: []infertypes.Model{{Id: b.modelID}},
	}, nil
}

func (b *validationBrokerChainBridge) GetCurrentEpochGroupData() (*infertypes.QueryCurrentEpochGroupDataResponse, error) {
	return nil, nil
}

func (b *validationBrokerChainBridge) GetEpochGroupDataByModelId(epochIndex uint64, modelID string) (*infertypes.QueryGetEpochGroupDataResponse, error) {
	if modelID == "" {
		return &infertypes.QueryGetEpochGroupDataResponse{
			EpochGroupData: infertypes.EpochGroupData{
				EpochIndex:     epochIndex,
				SubGroupModels: []string{b.modelID},
			},
		}, nil
	}

	if modelID != b.modelID {
		return nil, errors.New("unexpected model id")
	}

	return &infertypes.QueryGetEpochGroupDataResponse{
		EpochGroupData: infertypes.EpochGroupData{
			EpochIndex:    epochIndex,
			ModelId:       modelID,
			ModelSnapshot: &infertypes.Model{Id: modelID},
			ValidationWeights: []*infertypes.ValidationWeight{{
				MemberAddress: b.participantAddress,
				MlNodes: []*infertypes.MLNodeInfo{{
					NodeId: b.nodeID,
				}},
			}},
		},
	}, nil
}

func (b *validationBrokerChainBridge) GetParams() (*infertypes.QueryParamsResponse, error) {
	return &infertypes.QueryParamsResponse{}, nil
}

func setupValidationTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func startValidationTestGRPCServer(t *testing.T, srv infertypes.QueryServer) (*grpc.ClientConn, func()) {
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

func newValidationTestRecorder(t *testing.T, conn *grpc.ClientConn, address string) cosmosclient.InferenceCosmosClient {
	t.Helper()
	manager := &validationTestTxManager{
		clientContext: sdkclient.Context{}.WithGRPCClient(conn),
	}
	client := cosmosclient.InferenceCosmosClient{Address: address}
	setUnexportedField(t, &client, "ctx", context.Background())
	setUnexportedField(t, &client, "manager", tx_manager.TxManager(manager))
	return client
}

func setUnexportedField(t *testing.T, target interface{}, fieldName string, value interface{}) {
	t.Helper()
	v := reflectValueOfPointer(target, fieldName)
	reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func reflectValueOfPointer(target interface{}, fieldName string) reflect.Value {
	value := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	return value
}

func parseTraceparent(md metadata.MD) (trace.TraceID, trace.SpanID) {
	values := md.Get("traceparent")
	if len(values) == 0 {
		return trace.TraceID{}, trace.SpanID{}
	}
	parts := strings.Split(values[0], "-")
	if len(parts) != 4 {
		return trace.TraceID{}, trace.SpanID{}
	}
	traceID, _ := trace.TraceIDFromHex(parts[1])
	spanID, _ := trace.SpanIDFromHex(parts[2])
	return traceID, spanID
}

func TestSampleInferenceToValidate_UsesSampleContextForChainQueries(t *testing.T) {
	recorder := setupValidationTraceRecorder(t)
	t.Setenv("ENFORCED_MODEL_ID", "disabled")
	const (
		participantAddress = "gonka1validator"
		modelID            = "model-a"
		nodeID             = "node-1"
		inferenceID        = "inf-1"
	)

	queryServer := &validationQueryServer{
		infCh:   make(chan struct{}, 1),
		modelID: modelID,
	}
	conn, cleanup := startValidationTestGRPCServer(t, queryServer)
	defer cleanup()

	configManager := &apiconfig.ConfigManager{}
	require.NoError(t, configManager.SetCurrentSeed(apiconfig.SeedInfo{Seed: 7, EpochIndex: 1}))

	phaseTracker := &chainphase.ChainPhaseTracker{}
	phaseTracker.Update(
		chainphase.BlockInfo{Height: 1},
		&infertypes.Epoch{Index: 1, PocStartBlockHeight: 100},
		infertypes.DefaultEpochParams(),
		true,
		nil,
	)

	brokerChainBridge := &validationBrokerChainBridge{
		participantAddress: participantAddress,
		modelID:            modelID,
		nodeID:             nodeID,
	}
	nodeBroker := broker.NewBroker(
		brokerChainBridge,
		phaseTracker,
		participant.CosmosInfo{Address: participantAddress},
		"",
		mlnodeclient.NewMockClientFactory(),
		configManager,
	)

	responseCh := nodeBroker.LoadNodeToBroker(&apiconfig.InferenceNodeConfig{
		Host:             "127.0.0.1",
		InferenceSegment: "/v1/chat/completions",
		InferencePort:    8080,
		PoCSegment:       "/poc",
		PoCPort:          8081,
		Models: map[string]apiconfig.ModelConfig{
			modelID: {},
		},
		Id:            nodeID,
		MaxConcurrent: 1,
	})
	require.NotNil(t, responseCh)
	response := <-responseCh
	require.NoError(t, response.Error)

	txRecorder := newValidationTestRecorder(t, conn, participantAddress)
	validator := NewInferenceValidator(nodeBroker, configManager, nil, phaseTracker)

	validator.SampleInferenceToValidate(context.Background(), []string{inferenceID}, txRecorder)

	require.Eventually(t, func() bool {
		return len(queryServer.Calls()) == 3
	}, time.Second, 10*time.Millisecond)

	select {
	case <-queryServer.infCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for inference query")
	}

	ended := recorder.Ended()
	var sampleSpan sdktrace.ReadOnlySpan
	querySpans := make([]sdktrace.ReadOnlySpan, 0, 3)
	for _, span := range ended {
		switch span.Name() {
		case "inference.validation.sample":
			sampleSpan = span
		case "chain.grpc.query":
			querySpans = append(querySpans, span)
		}
	}

	require.NotNil(t, sampleSpan)
	require.Len(t, querySpans, 3)

	for _, span := range querySpans {
		require.Equal(t, sampleSpan.SpanContext().TraceID(), span.SpanContext().TraceID())
		require.Equal(t, sampleSpan.SpanContext().SpanID(), span.Parent().SpanID())
	}

	for _, call := range queryServer.Calls() {
		require.Equal(t, sampleSpan.SpanContext().TraceID(), call.traceID)
		require.True(t, call.spanID.IsValid())
	}
	}