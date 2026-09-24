package chain

import (
	"context"
	"testing"
)

// Test flow:
//  1. Create a GRPCChain configured with a padded chain id and no gRPC connection.
//  2. Call ChainID.
//  3. Assert it returns the configured id trimmed, without asking the node.
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
