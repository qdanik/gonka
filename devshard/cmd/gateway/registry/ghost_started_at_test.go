package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/types"
)

// A burned nonce is only ever settled by a refusal timeout, and a verifier gates that on
// nowSeconds-StartedAt >= RefusalTimeout (host/timeout.go). A millisecond stamp keeps the difference
// around -1.8e12 forever, so the vote is rejected no matter how long anyone waits.
func TestABurnedNonceCanReachItsRefusalDeadline(t *testing.T) {
	burnedAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	stream := nonceStream{model: "test-model", now: func() time.Time { return burnedAt }}

	params := stream.ghostParams()

	require.Equal(t, burnedAt.Unix(), params.StartedAt)
	waited := burnedAt.Add(types.DefaultRefusalTimeoutSeconds*time.Second + time.Second).Unix()
	require.GreaterOrEqual(t, waited-params.StartedAt, int64(types.DefaultRefusalTimeoutSeconds),
		"a verifier must be able to accept the timeout once the refusal window has passed")
}
