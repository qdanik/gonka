package chain

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// EscrowInfo is the escrow as the gateway reads it: the id it was asked about and the balance that
// decides whether it can still pay for work.
type EscrowInfo struct {
	EscrowID string
	Balance  uint64
}

// GetEscrow reports found=false only when the chain says the escrow is absent. A read that failed is
// returned as an error and never as absence: absence retires an escrow, and retiring one that still
// holds funds strands them.
func (c *TxClient) GetEscrow(ctx context.Context, escrowID string) (EscrowInfo, bool, error) {
	numericID, err := strconv.ParseUint(strings.TrimSpace(escrowID), 10, 64)
	if err != nil {
		return EscrowInfo{}, false, fmt.Errorf("escrow id %q is not a number: %w", escrowID, err)
	}
	return c.transport.Escrow(ctx, numericID)
}
