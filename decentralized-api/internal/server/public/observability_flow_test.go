package public

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"decentralized-api/apiconfig"
	"decentralized-api/cosmosclient"
	"decentralized-api/internal"
	"decentralized-api/utils"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/labstack/echo/v4"
	"github.com/productscience/inference/x/inference/calculations"
	"github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type forwardFlowQueryServer struct {
	fakePricingQueryServer
	executorAddress string
	executorURL     string
}

func (f *forwardFlowQueryServer) GetRandomExecutor(ctx context.Context, req *types.QueryGetRandomExecutorRequest) (*types.QueryGetRandomExecutorResponse, error) {
	return &types.QueryGetRandomExecutorResponse{
		Executor: types.Participant{
			Address:      f.executorAddress,
			InferenceUrl: f.executorURL,
		},
	}, nil
}

func setupPublicTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func setupPublicTestCodec() codec.Codec {
	registry := codectypes.NewInterfaceRegistry()
	registry.RegisterInterface("cosmos.crypto.PubKey", (*cryptotypes.PubKey)(nil))
	registry.RegisterInterface("cosmos.crypto.PrivKey", (*cryptotypes.PrivKey)(nil))
	registry.RegisterImplementations((*cryptotypes.PubKey)(nil), &secp256k1.PubKey{})
	registry.RegisterImplementations((*cryptotypes.PrivKey)(nil), &secp256k1.PrivKey{})
	return codec.NewProtoCodec(registry)
}

func newTestSignerKeyring(t *testing.T) (*keyring.Keyring, string) {
	t.Helper()
	kr := keyring.NewInMemory(setupPublicTestCodec())
	record, _, err := kr.NewMnemonic("signer", keyring.English, sdk.FullFundraiserPath, "", hd.Secp256k1)
	require.NoError(t, err)
	addr, err := record.GetAddress()
	require.NoError(t, err)
	return &kr, addr.String()
}

func assertSpanRecorded(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return
		}
	}
	t.Fatalf("expected span %q to be recorded", name)
}

func TestPostChat_OversizedBody_RecordsRequestSpan(t *testing.T) {
	recorder := setupPublicTraceRecorder(t)
	e := echo.New()
	s := &Server{e: e}

	oversizedBody := bytes.Repeat([]byte("a"), MaxRequestBodySize+1)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(oversizedBody))
	req.Header.Set("Content-Type", "application/json")
	ctx := e.NewContext(req, httptest.NewRecorder())

	err := s.postChat(ctx)
	require.Error(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assertSpanRecorded(t, spans, "inference.request")
	require.Equal(t, codes.Error, spans[0].Status().Code)
}

func TestPostCompletions_OversizedBody_RecordsRequestSpan(t *testing.T) {
	recorder := setupPublicTraceRecorder(t)
	e := echo.New()
	s := &Server{e: e}

	oversizedBody := bytes.Repeat([]byte("a"), MaxRequestBodySize+1)
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", bytes.NewReader(oversizedBody))
	req.Header.Set("Content-Type", "application/json")
	ctx := e.NewContext(req, httptest.NewRecorder())

	err := s.postCompletions(ctx)
	require.Error(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assertSpanRecorded(t, spans, "inference.request")
	require.Equal(t, codes.Error, spans[0].Status().Code)
}

func TestHandleTransferRequest_ForwardRequestUsesTraceContext(t *testing.T) {
	setupPublicTraceRecorder(t)
	e := echo.New()
	configManager := newTestConfigManager(t)
	configManager.SetTransferAgentAccessCache(apiconfig.TransferAgentAccessCache{IsEnabled: false})
	configManager.SetBandwidthParams(apiconfig.BandwidthParamsCache{
		EstimatedLimitsPerBlockKb: 1_000_000,
		KbPerInputToken:           1,
		KbPerOutputToken:          1,
	})

	status := &coretypes.ResultStatus{
		SyncInfo: coretypes.SyncInfo{
			LatestBlockHeight: 1,
			LatestBlockTime:   time.Now(),
		},
	}
	timestamp := status.SyncInfo.LatestBlockTime.UnixNano()
	body := `{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`
	transferAddress := "ta1"
	devKey := newTestKey()
	components := calculations.SignatureComponents{
		Payload:         utils.GenerateSHA256Hash(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
	}
	signature, err := calculations.Sign(devKey, components, calculations.Developer)
	require.NoError(t, err)

	queryServer := &forwardFlowQueryServer{
		fakePricingQueryServer: fakePricingQueryServer{
			price:   1,
			found:   true,
			pubkey:  devKey.GetPubKeyBase64(),
			balance: 100,
		},
		executorAddress: "executor1",
		executorURL:     "http://executor",
	}
	conn, cleanup := startTestGRPCServer(t, queryServer)
	t.Cleanup(cleanup)

	kr, signerAddress := newTestSignerKeyring(t)
	mockCosmos := &cosmosclient.MockCosmosMessageClient{}
	mockCosmos.On("NewInferenceQueryClient").Return(types.NewQueryClient(conn))
	mockCosmos.On("Status", mock.MatchedBy(func(ctx context.Context) bool {
		return trace.SpanFromContext(ctx).SpanContext().IsValid()
	})).Return(status, nil)
	mockCosmos.On("StartInference", mock.Anything).Return(nil).Maybe()
	mockCosmos.On("GetSignerAddress").Return(signerAddress)
	mockCosmos.On("GetKeyring").Return(kr)

	var sawTraceContext bool
	httpClient := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		sawTraceContext = trace.SpanFromContext(req.Context()).SpanContext().IsValid() && req.Header.Get("traceparent") != ""
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
		}, nil
	})}

	s := &Server{
		e:                e,
		recorder:         mockCosmos,
		configManager:    configManager,
		bandwidthLimiter: internal.NewBandwidthLimiterFromConfig(configManager, nil, nil),
		httpClient:       httpClient,
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	ctx := e.NewContext(req, httptest.NewRecorder())
	request := &ChatRequest{
		Body:             []byte(body),
		ForwardPath:      chatCompletionsPath,
		ForwardBody:      []byte(body),
		Request:          req,
		OpenAiRequest:    OpenAiRequest{Model: "test-model", MaxTokens: 1, Messages: []Message{{Role: "user", Content: textMessageContent("hello")}}},
		AuthKey:          signature,
		Timestamp:        timestamp,
		TransferAddress:  transferAddress,
		RequesterAddress: "dev1",
		SignBodyHash:     utils.GenerateSHA256Hash(body),
	}

	err = s.handleTransferRequest(ctx, request)
	require.NoError(t, err)
	require.True(t, sawTraceContext)
	mockCosmos.AssertExpectations(t)
}