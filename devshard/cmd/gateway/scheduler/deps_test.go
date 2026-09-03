package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A missing dependency used to be a panic on the first request rather than a refusal at boot.
func TestNewSchedulerNamesTheDependencyItWasNotGiven(t *testing.T) {
	_, err := NewScheduler(Deps{})

	require.ErrorContains(t, err, "Config")
}
