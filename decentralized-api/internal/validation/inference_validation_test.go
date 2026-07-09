package validation

import (
	"testing"

	"decentralized-api/completionapi"

	"github.com/stretchr/testify/require"
)

// TestCustomDistanceIgnoresExecutorTopLogprobsPadding pins the H1 #3853145 fix:
// positionDistance sums over the validator's tokens, so the denominator must be
// normalized by the validator's top_logprobs width — not the executor's. The
// executor is untrusted; before the fix it could pad its top_logprobs width to
// inflate the denominator and shrink the distance below the validation
// threshold. Widening the executor's top_logprobs (entries the validator never
// scores against) must therefore leave the distance unchanged.
func TestCustomDistanceIgnoresExecutorTopLogprobsPadding(t *testing.T) {
	// Validator (trusted): a fixed 2-wide top_logprobs at one position.
	validator := []completionapi.Logprob{{
		Token: "x",
		TopLogprobs: []completionapi.TopLogprobs{
			{Token: "a", Logprob: -0.5},
			{Token: "b", Logprob: -1.5},
		},
	}}

	// Executor (untrusted), narrow: exactly the tokens the validator scores.
	narrowExecutor := []completionapi.Logprob{{
		Token: "x",
		TopLogprobs: []completionapi.TopLogprobs{
			{Token: "a", Logprob: -2.0},
			{Token: "b", Logprob: -2.0},
		},
	}}

	// Executor, padded: identical logprobs for the validator's tokens plus extra
	// entries the validator never has — so positionDistance is unchanged and only
	// the executor's top_logprobs *width* differs.
	paddedExecutor := []completionapi.Logprob{{
		Token: "x",
		TopLogprobs: []completionapi.TopLogprobs{
			{Token: "a", Logprob: -2.0},
			{Token: "b", Logprob: -2.0},
			{Token: "c", Logprob: -50.0},
			{Token: "d", Logprob: -50.0},
			{Token: "e", Logprob: -50.0},
		},
	}}

	narrow, err := customDistance(narrowExecutor, validator)
	require.NoError(t, err)
	padded, err := customDistance(paddedExecutor, validator)
	require.NoError(t, err)

	require.Greater(t, narrow, 0.0, "sanity: distance is non-zero so the invariance is meaningful")
	require.Equal(t, narrow, padded, "executor top_logprobs padding must not change the distance (H1 #3853145)")
}
