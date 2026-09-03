package escrow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A missing dependency used to be a panic on the first tick rather than a refusal at boot.
func TestNewManagerNamesTheDependencyItWasNotGiven(t *testing.T) {
	_, err := NewManager(Deps{})

	require.ErrorContains(t, err, "Config")
}
