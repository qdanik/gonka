package chain

import (
	"context"
	"fmt"
	"net/url"
)

const devshardEscrowPath = "/productscience/inference/inference/devshard_escrow/"

type EscrowInfo struct {
	EscrowID string
	Balance  uint64
}

type devshardEscrowResponse struct {
	Escrow struct {
		Amount jsonUint64 `json:"amount"`
	} `json:"escrow"`
	Found bool `json:"found"`
}

// found=false only when every reachable endpoint agrees the escrow is absent; any per-endpoint
// error is returned as-is, never read as absence.
func (c *TxClient) GetEscrow(ctx context.Context, escrowID string) (EscrowInfo, bool, error) {
	var lastErr error
	sawNotFound := false
	for _, baseURL := range c.txQueryURLs {
		var payload devshardEscrowResponse
		err := c.getJSONFromBaseURL(ctx, baseURL, devshardEscrowPath+url.PathEscape(escrowID), &payload)
		if err != nil {
			if isNotFoundError(err) {
				sawNotFound = true
				continue
			}
			lastErr = fmt.Errorf("%s: %w", baseURL, err)
			continue
		}
		if !payload.Found {
			sawNotFound = true
			continue
		}
		return EscrowInfo{EscrowID: escrowID, Balance: uint64(payload.Escrow.Amount)}, true, nil
	}
	if lastErr == nil && sawNotFound {
		return EscrowInfo{}, false, nil
	}
	return EscrowInfo{}, false, lastErr
}
