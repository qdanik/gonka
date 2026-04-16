package observability

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorFmt_NilErrorReturnsNil(t *testing.T) {
	require.NoError(t, Error.Fmt(nil, "anything"))
}

func TestErrorFmt_WrapsAndPreservesCause(t *testing.T) {
	root := errors.New("root cause")
	err := Error.Fmt(root, "context %d", 7)
	require.Error(t, err)
	require.EqualError(t, err, "context 7. root cause")
	require.ErrorIs(t, err, root)
}

func TestErrorFmt_EmptyFormatReturnsOriginalError(t *testing.T) {
	root := errors.New("root cause")
	err := Error.Fmt(root, "")
	require.Same(t, root, err)
}