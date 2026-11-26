package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/types"
)

// Test flow:
//  1. Build a nonce stream whose clock is fixed at a burned-at instant.
//  2. Read the stream's ghost params.
//  3. Assert StartedAt equals the burned-at instant in Unix seconds.
//  4. Assert that once the default refusal timeout has elapsed, nowSeconds-StartedAt meets or exceeds the timeout, so a verifier can accept the refusal deadline.
func TestABurnedNonceCanReachItsRefusalDeadline(t *testing.T) {
	burnedAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	stream := nonceStream{model: "test-model", now: func() time.Time { return burnedAt }}

	params := stream.ghostParams()

	require.Equal(t, burnedAt.Unix(), params.StartedAt)
	waited := burnedAt.Add(types.DefaultRefusalTimeoutSeconds*time.Second + time.Second).Unix()
	require.GreaterOrEqual(t, waited-params.StartedAt, int64(types.DefaultRefusalTimeoutSeconds),
		"a verifier must be able to accept the timeout once the refusal window has passed")
}

// Test flow:
//  1. Read the placeholder params for a pass with a given second, and the stream's own ghost params at a later clock.
//  2. Assert the pass params carry the given second and the same prompt and token budget as a ghost's.
func TestAPlaceholderIsDatedAsTheScheduleCommittedIt(t *testing.T) {
	committedAt := int64(1_700_000_000)
	stream := nonceStream{model: "test-model", now: func() time.Time { return time.Unix(committedAt+90, 0) }}

	pass := PlaceholderParams("test-model", committedAt)
	ghost := stream.ghostParams()

	require.Equal(t, committedAt, pass.StartedAt)
	require.Equal(t, committedAt+90, ghost.StartedAt)
	pass.StartedAt = ghost.StartedAt
	require.Equal(t, ghost, pass)
}
