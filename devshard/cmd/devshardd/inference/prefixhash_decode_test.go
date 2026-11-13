package inference

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// promptPrefixHashes hashes a request body through the same decode and hashing the engine uses.
func promptPrefixHashes(body []byte) []uint64 {
	var requestMap map[string]interface{}
	if err := json.Unmarshal(body, &requestMap); err != nil {
		return nil
	}
	return messagesPrefixHashes(requestMap)
}

// Test flow:
//  1. Put hashes in a context and read them back.
//  2. Expect the same hashes, and none from a context that carries none or carries an empty list.
func TestPrefixHashesTravelInTheContext(t *testing.T) {
	require.Equal(t, []uint64{1, 2}, prefixHashesFrom(withPrefixHashes(context.Background(), []uint64{1, 2})))
	require.Empty(t, prefixHashesFrom(context.Background()))
	require.Empty(t, prefixHashesFrom(withPrefixHashes(context.Background(), nil)))
}
