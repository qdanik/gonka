package transport

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type endlessLine struct{ served int }

func (e *endlessLine) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'A'
	}
	e.served += len(p)
	return len(p), nil
}

func TestParseSSE_EndlessLineIsRefusedRatherThanAccumulated(t *testing.T) {
	const limit = 1 << 20
	client := &HTTPClient{config: ClientConfig{MaxSSELineBytes: limit}}
	host := &endlessLine{}

	result, err := client.parseSSEResponse(context.Background(), io.MultiReader(strings.NewReader("data: "), host), nil, nil)

	require.ErrorIs(t, err, ErrSSELineTooLarge, "a host that never sends a newline must be refused by name")
	require.NotNil(t, result)
	require.Less(t, host.served, 4*limit, "the reader must stop near the cap, not keep pulling")
}

func TestParseSSE_LargeLineUnderTheCapArrivesWhole(t *testing.T) {
	const limit = 1 << 20
	client := &HTTPClient{config: ClientConfig{MaxSSELineBytes: limit}}
	forwarded := &strings.Builder{}

	payload := strings.Repeat("x", limit-64)
	stream := "data: " + payload + "\n" + "data: [DONE]\n"

	result, err := client.parseSSEResponse(context.Background(), strings.NewReader(stream), forwarded, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, forwarded.String(), payload, "a line under the cap must reach the client unchanged")
}

func TestParseSSE_ZeroConfigFallsBackToTheDefaultCap(t *testing.T) {
	client := &HTTPClient{config: ClientConfig{}}
	forwarded := &strings.Builder{}

	_, err := client.parseSSEResponse(context.Background(), strings.NewReader("data: hello\ndata: [DONE]\n"), forwarded, nil)

	require.NoError(t, err)
	require.Contains(t, forwarded.String(), "hello", "an unset cap must not read as a zero-byte one")
}

func TestDefaultClientConfigCarriesTheCap(t *testing.T) {
	require.Equal(t, DefaultMaxSSELineBytes, DefaultClientConfig().MaxSSELineBytes)
}
