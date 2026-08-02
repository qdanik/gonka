package chain

import (
	"context"
	"testing"
)

// A configured chain id is the operator's decision and is not checked against the node: a mismatch
// invalidates every signature, so the value that signs must be the one that was configured.
func TestAConfiguredChainIDIsUsedWithoutAskingTheNode(t *testing.T) {
	grpcChain := NewGRPCChain(nil, "  gonka-mainnet  ")

	chainID, err := grpcChain.ChainID(context.Background())

	if err != nil {
		t.Fatalf("ChainID: %v", err)
	}
	if chainID != "gonka-mainnet" {
		t.Fatalf("chain id = %q, want the configured one", chainID)
	}
}
