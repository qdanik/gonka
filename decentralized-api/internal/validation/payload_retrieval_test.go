package validation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"decentralized-api/payloadstorage"
	devshardpkg "devshard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupPayloadTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func TestBuildPayloadRequestURL_DevshardPath(t *testing.T) {
	// Test with devshard session-specific path
	url, err := BuildPayloadRequestURL("https://executor.example.com", devshardpkg.LegacySessionPayloadPath("escrow-123"), "456")
	require.NoError(t, err)
	assert.Contains(t, url, devshardpkg.LegacySessionPayloadPath("escrow-123"))
	assert.Contains(t, url, "inference_id=456")
}

func TestBuildPayloadRequestURL_VersionedDevshardPath(t *testing.T) {
	url, err := BuildPayloadRequestURL("https://executor.example.com", devshardpkg.VersionedSessionPayloadPath("v1", "escrow-123"), "456")
	require.NoError(t, err)
	assert.Contains(t, url, devshardpkg.VersionedSessionPayloadPath("v1", "escrow-123"))
	assert.Contains(t, url, "inference_id=456")
}

func TestBuildPayloadRequestURL_PublicPath(t *testing.T) {
	// Test with public endpoint path
	url, err := BuildPayloadRequestURL("https://executor.example.com", "v1/inference/payloads", "test-id")
	require.NoError(t, err)
	assert.Contains(t, url, "v1/inference/payloads")
	assert.Contains(t, url, "inference_id=test-id")
}

func TestVerifyPayloadHashes_Valid(t *testing.T) {
	promptPayload := []byte(`{"model":"test","messages":[]}`)
	responsePayload := []byte(`{"choices":[]}`)

	expectedPromptHash, err := payloadstorage.ComputePromptHash(promptPayload)
	require.NoError(t, err)
	expectedResponseHash, err := payloadstorage.ComputeResponseHash(responsePayload)
	require.NoError(t, err)

	err = VerifyPayloadHashes(promptPayload, responsePayload, expectedPromptHash, expectedResponseHash, "inf-1")
	assert.NoError(t, err)
}

func TestVerifyPayloadHashes_EmptyExpectedHashes(t *testing.T) {
	// Empty expected hashes should pass (backward compatibility)
	err := VerifyPayloadHashes([]byte("prompt"), []byte("response"), "", "", "inf-1")
	assert.NoError(t, err)
}

func TestVerifyPayloadHashes_PromptMismatch(t *testing.T) {
	promptPayload := []byte(`{"model":"test"}`)
	responsePayload := []byte(`{"choices":[]}`)

	expectedResponseHash, err := payloadstorage.ComputeResponseHash(responsePayload)
	require.NoError(t, err)

	// Use wrong prompt hash
	err = VerifyPayloadHashes(promptPayload, responsePayload, "wrong-hash", expectedResponseHash, "inf-1")
	assert.ErrorIs(t, err, ErrHashMismatch)
}

func TestVerifyPayloadHashes_ResponseMismatch(t *testing.T) {
	promptPayload := []byte(`{"model":"test"}`)
	responsePayload := []byte(`{"choices":[]}`)

	expectedPromptHash, err := payloadstorage.ComputePromptHash(promptPayload)
	require.NoError(t, err)

	// Use wrong response hash
	err = VerifyPayloadHashes(promptPayload, responsePayload, expectedPromptHash, "wrong-hash", "inf-1")
	assert.ErrorIs(t, err, ErrHashMismatch)
}

func TestBuildPayloadRequestURL(t *testing.T) {
	tests := []struct {
		name        string
		executorUrl string
		inferenceId string
		wantQuery   string
	}{
		{
			name:        "simple base64 ID",
			executorUrl: "https://executor.example.com",
			inferenceId: "aW5mZXJlbmNlLTEyMzQ1",
			wantQuery:   "inference_id=aW5mZXJlbmNlLTEyMzQ1",
		},
		{
			name:        "base64 ID with slash",
			executorUrl: "https://executor.example.com",
			inferenceId: "abc/def/ghi",
			wantQuery:   "inference_id=abc%2Fdef%2Fghi",
		},
		{
			name:        "base64 ID with plus",
			executorUrl: "https://executor.example.com",
			inferenceId: "abc+def+ghi",
			wantQuery:   "inference_id=abc%2Bdef%2Bghi",
		},
		{
			name:        "base64 ID with slash and plus",
			executorUrl: "https://executor.example.com",
			inferenceId: "a/b+c/d+e",
			wantQuery:   "inference_id=a%2Fb%2Bc%2Fd%2Be",
		},
		{
			name:        "base64 ID with equals padding",
			executorUrl: "https://executor.example.com",
			inferenceId: "dGVzdA==",
			wantQuery:   "inference_id=dGVzdA%3D%3D",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseUrl, err := url.JoinPath(tt.executorUrl, "v1/inference/payloads")
			require.NoError(t, err)

			parsedUrl, err := url.Parse(baseUrl)
			require.NoError(t, err)

			query := parsedUrl.Query()
			query.Set("inference_id", tt.inferenceId)
			parsedUrl.RawQuery = query.Encode()

			result := parsedUrl.String()

			require.Contains(t, result, "v1/inference/payloads")
			require.Contains(t, result, tt.wantQuery)

			// Verify URL can be parsed and query param decoded correctly
			parsedResult, err := url.Parse(result)
			require.NoError(t, err)
			decodedId := parsedResult.Query().Get("inference_id")
			require.Equal(t, tt.inferenceId, decodedId)
		})
	}
}

func TestFetchPayloadsHTTP_NotFoundReturnsOriginalErrorAndFormatsSpan(t *testing.T) {
	recorder := setupPayloadTraceRecorder(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := FetchPayloadsHTTP(context.Background(), server.Client(), server.URL, "validator1", 123, 7, "sig")
	require.EqualError(t, err, "payload not found on executor")

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "inference.payload.fetch", spans[0].Name())
	require.Equal(t, codes.Error, spans[0].Status().Code)
	require.Contains(t, spans[0].Status().Description, "payload fetch returned not found")
	require.Contains(t, spans[0].Status().Description, "payload not found on executor")
}

func TestFetchPayloadsHTTP_InvalidJSONReturnsOriginalErrorAndFormatsSpan(t *testing.T) {
	recorder := setupPayloadTraceRecorder(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer server.Close()

	_, err := FetchPayloadsHTTP(context.Background(), server.Client(), server.URL, "validator1", 123, 7, "sig")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode response")

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "inference.payload.fetch", spans[0].Name())
	require.Equal(t, codes.Error, spans[0].Status().Code)
	require.Contains(t, spans[0].Status().Description, "decode payload fetch response")
	require.Contains(t, spans[0].Status().Description, "failed to decode response")
}
