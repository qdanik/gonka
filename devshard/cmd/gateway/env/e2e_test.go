package env

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/internal/e2econfig"
)

// Without these a stand cannot reach the execution-timeout path: the chain's deadline is half an hour.
func TestLoadE2EReadsTheProtocolDeadlines(t *testing.T) {
	t.Setenv("DEVSHARD_E2E", "1")
	t.Setenv(e2econfig.RefusalTimeoutSecondsEnv, "5")
	t.Setenv(e2econfig.ExecutionTimeoutSecondsEnv, "10")

	loaded := LoadE2E()

	require.NotNil(t, loaded.SessionTimeouts.RefusalTimeoutSeconds)
	require.Equal(t, int64(5), *loaded.SessionTimeouts.RefusalTimeoutSeconds)
	require.NotNil(t, loaded.SessionTimeouts.ExecutionTimeoutSeconds)
	require.Equal(t, int64(10), *loaded.SessionTimeouts.ExecutionTimeoutSeconds)
}

// A stand that declares nothing is production, and production must read no override at all.
func TestLoadE2EWithoutADeclarationReadsNoDeadline(t *testing.T) {
	t.Setenv(e2econfig.ExecutionTimeoutSecondsEnv, "10")

	loaded := LoadE2E()

	require.Nil(t, loaded.SessionTimeouts.ExecutionTimeoutSeconds)
}
