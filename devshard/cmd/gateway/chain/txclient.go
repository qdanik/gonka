// Package chain builds/signs/broadcasts gonka escrow transactions and
// observes chain phase state.
package chain

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"devshard/logging"
	"devshard/signing"
)

// Defaults applied by NewTxClient for zero-value fields, and the single source config.Defaults reads.
const (
	DefaultFeeDenom     = "ngonka"
	DefaultFeeAmount    = uint64(1_000_000)
	DefaultGasLimit     = uint64(500_000)
	DefaultPollInterval = 2 * time.Second
	DefaultPollTimeout  = 45 * time.Second
	// UnorderedTxTTL is how far past "now" a built tx's timeout_timestamp is set.
	UnorderedTxTTL = 9 * time.Minute
)

// ErrTxNotFound marks a tx absent from every reachable query endpoint —
// distinct from a tx that committed but failed on chain. Not terminal: the
// tx may still land until its unordered TTL elapses.
var ErrTxNotFound = errors.New("tx not found on chain")

// TxClient builds, signs, broadcasts, and queries gonka escrow transactions over the chain's gRPC
// service. It owns the transaction envelope and the intent hook; the transport underneath is
// interchangeable, which is what keeps its tests off a connection.
type TxClient struct {
	transport    Transport
	feeDenom     string
	feeAmount    uint64
	gasLimit     uint64
	pollInterval time.Duration
	pollTimeout  time.Duration
	now          func() time.Time
}

// Config configures a TxClient. Zero-value fee/gas/poll/clock fields take package defaults; Transport
// is required.
type Config struct {
	Transport    Transport
	FeeDenom     string
	FeeAmount    uint64
	GasLimit     uint64
	PollInterval time.Duration
	PollTimeout  time.Duration
	Now          func() time.Time
}

// The tags are what the operator API returns: without them the two results are the only responses in
// the gateway rendered with Go field names.
type CreateEscrowResult struct {
	EscrowID uint64 `json:"escrow_id"`
	TxHash   string `json:"tx_hash"`
	Creator  string `json:"creator"`
}

type SettleEscrowResult struct {
	EscrowID uint64 `json:"escrow_id"`
	TxHash   string `json:"tx_hash"`
	Settler  string `json:"settler"`
}

// NewTxClient validates cfg and applies defaults for unset fields; it errors only when no transport
// is given.
func NewTxClient(cfg Config) (*TxClient, error) {
	if cfg.Transport == nil {
		return nil, fmt.Errorf("chain transport is required")
	}
	feeDenom := strings.TrimSpace(cfg.FeeDenom)
	if feeDenom == "" {
		feeDenom = DefaultFeeDenom
	}
	feeAmount := cfg.FeeAmount
	if feeAmount == 0 {
		feeAmount = DefaultFeeAmount
	}
	gasLimit := cfg.GasLimit
	if gasLimit == 0 {
		gasLimit = DefaultGasLimit
	}
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	pollTimeout := cfg.PollTimeout
	if pollTimeout <= 0 {
		pollTimeout = DefaultPollTimeout
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &TxClient{
		transport:    cfg.Transport,
		feeDenom:     feeDenom,
		feeAmount:    feeAmount,
		gasLimit:     gasLimit,
		pollInterval: pollInterval,
		pollTimeout:  pollTimeout,
		now:          now,
	}, nil
}

// CreateEscrow builds, signs, and broadcasts a MsgCreateDevshardEscrow tx; onPrepared records the
// precomputed tx hash before the irreversible broadcast, and an error from it aborts the broadcast.
// See gateway-escrow-lifecycle.md, "Creating an escrow, and surviving a crash mid-creation".
func (c *TxClient) CreateEscrow(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(txHash string) error) (CreateEscrowResult, error) {
	if signer == nil {
		return CreateEscrowResult{}, fmt.Errorf("signer is required")
	}
	modelID = strings.TrimSpace(modelID)
	if modelID == "" {
		return CreateEscrowResult{}, fmt.Errorf("model_id is required")
	}
	if amount == 0 {
		return CreateEscrowResult{}, fmt.Errorf("amount is required")
	}
	creator := signer.Address()
	chainID, err := c.resolveChainID(ctx)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	account, err := c.transport.Account(ctx, creator)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	ttl := c.now().Add(UnorderedTxTTL)
	txBytes, err := buildCreateEscrowTx(signer, chainID, account.Number, c.feeDenom, c.feeAmount, c.gasLimit, amount, modelID, ttl)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	txHash := txHashFromBytes(txBytes)
	if onPrepared != nil {
		if err := onPrepared(txHash); err != nil {
			return CreateEscrowResult{}, fmt.Errorf("record escrow create intent before broadcast: %w", err)
		}
	}
	nodeHash, err := c.transport.Broadcast(ctx, txBytes)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	if !strings.EqualFold(nodeHash, txHash) {
		return CreateEscrowResult{}, fmt.Errorf("tx hash mismatch: precomputed %s, node returned %s", txHash, nodeHash)
	}
	escrowID, err := c.waitForCreatedEscrowID(ctx, txHash)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	return CreateEscrowResult{EscrowID: escrowID, TxHash: txHash, Creator: creator}, nil
}

// SettleEscrow builds, signs, broadcasts and confirms a MsgSettleDevshardEscrow tx; unlike CreateEscrow
// it takes no intent hook, and it waits for the commit rather than CheckTx. See
// gateway-escrow-lifecycle.md, "Transaction encoding".
// SettleEscrow takes onPrepared for the same reason CreateEscrow does: the hash has to be durable
// before the broadcast, or a settle that commits while the gateway is not looking is money moved under
// a name nothing recorded.
func (c *TxClient) SettleEscrow(ctx context.Context, signer *signing.Secp256k1Signer, input SettlementInput, onPrepared func(txHash string) error) (SettleEscrowResult, error) {
	if signer == nil {
		return SettleEscrowResult{}, fmt.Errorf("signer is required")
	}
	if input.EscrowID == 0 {
		return SettleEscrowResult{}, fmt.Errorf("escrow_id is required")
	}
	settler := signer.Address()
	chainID, err := c.resolveChainID(ctx)
	if err != nil {
		return SettleEscrowResult{}, err
	}
	account, err := c.transport.Account(ctx, settler)
	if err != nil {
		return SettleEscrowResult{}, err
	}
	ttl := c.now().Add(UnorderedTxTTL)
	txBytes, err := buildSettleEscrowTx(signer, chainID, account.Number, c.feeDenom, c.feeAmount, c.gasLimit, settler, input, ttl)
	if err != nil {
		return SettleEscrowResult{}, err
	}
	txHash := txHashFromBytes(txBytes)
	if onPrepared != nil {
		if err := onPrepared(txHash); err != nil {
			return SettleEscrowResult{}, fmt.Errorf("record settle intent before broadcast: %w", err)
		}
	}
	nodeHash, err := c.transport.Broadcast(ctx, txBytes)
	if err != nil {
		return SettleEscrowResult{}, err
	}
	if !strings.EqualFold(nodeHash, txHash) {
		return SettleEscrowResult{}, fmt.Errorf("tx hash mismatch: precomputed %s, node returned %s", txHash, nodeHash)
	}
	logging.Info("settle tx broadcast", "escrow", input.EscrowID, "tx", txHash, "settler", settler)
	// Waiting is what makes the result mean "settled": the caller destroys the means to retry. See
	// gateway-escrow-lifecycle.md, "Transaction encoding".
	if err := c.waitForCommit(ctx, txHash); err != nil {
		return SettleEscrowResult{}, fmt.Errorf("awaiting settlement commit for tx %s: %w", txHash, err)
	}
	return SettleEscrowResult{EscrowID: input.EscrowID, TxHash: txHash, Settler: settler}, nil
}

func (c *TxClient) resolveChainID(ctx context.Context) (string, error) {
	return c.transport.ChainID(ctx)
}

// GetTxEscrowID reports found=false when the transaction committed and failed, and ErrTxNotFound when
// the chain does not have it; a transport failure is neither. See gateway-escrow-lifecycle.md,
// "Creating an escrow, and surviving a crash mid-creation".
func (c *TxClient) GetTxEscrowID(ctx context.Context, txHash string) (uint64, bool, error) {
	result, found, err := c.transport.Tx(ctx, txHash)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, ErrTxNotFound
	}
	if result.Code != 0 {
		return 0, false, nil // tx committed but failed -> no escrow created
	}
	if escrowID, held := result.createdEscrowID(); held {
		return escrowID, true, nil
	}
	return 0, false, fmt.Errorf("tx %s committed but escrow_id event was not found", txHash)
}

// TxCommitted reports whether a transaction reached the chain and succeeded there. A transport failure
// is returned as one: reading it as absence would rebuild and rebroadcast a settlement that already
// moved the money.
func (c *TxClient) TxCommitted(ctx context.Context, txHash string) (succeeded bool, err error) {
	result, found, err := c.transport.Tx(ctx, txHash)
	if err != nil {
		return false, err
	}
	if !found {
		return false, ErrTxNotFound
	}
	return result.Code == 0, nil
}

// awaitTx polls the chain until a committed transaction satisfies ready, pollTimeout elapses, or ctx is
// done; a 404 is "not indexed yet" rather than the terminal not-found GetTxEscrowID reports. A non-zero
// code is returned as an error, and it is the only way to learn a DeliverTx failure: broadcasting in
// BROADCAST_MODE_SYNC reports CheckTx alone, so a transaction can be accepted and still never execute.
func (c *TxClient) awaitTx(ctx context.Context, txHash string, ready func(TxResult) bool) (TxResult, error) {
	deadline := c.now().Add(c.pollTimeout)
	var lastErr error
	for {
		result, found, err := c.transport.Tx(ctx, txHash)
		switch {
		case err != nil:
			lastErr = err
		case !found:
			lastErr = fmt.Errorf("tx %s is not on chain yet", txHash)
		case result.Code != 0:
			return TxResult{}, fmt.Errorf("tx %s failed code=%d codespace=%s raw_log=%s", txHash, result.Code, result.Codespace, result.RawLog)
		case ready(result):
			return result, nil
		default:
			lastErr = fmt.Errorf("tx %s committed but is not yet complete", txHash)
		}
		if c.now().After(deadline) {
			if lastErr != nil {
				return TxResult{}, fmt.Errorf("wait for tx %s: %w", txHash, lastErr)
			}
			return TxResult{}, fmt.Errorf("wait for tx %s timed out", txHash)
		}
		select {
		case <-ctx.Done():
			return TxResult{}, ctx.Err()
		case <-time.After(c.pollInterval):
		}
	}
}

func (c *TxClient) waitForCreatedEscrowID(ctx context.Context, txHash string) (uint64, error) {
	committed, err := c.awaitTx(ctx, txHash, func(response TxResult) bool {
		_, found := response.createdEscrowID()
		return found
	})
	if err != nil {
		return 0, err
	}
	escrowID, _ := committed.createdEscrowID()
	return escrowID, nil
}

// waitForCommit reports that a transaction executed, not merely that it was accepted.
func (c *TxClient) waitForCommit(ctx context.Context, txHash string) error {
	_, err := c.awaitTx(ctx, txHash, func(TxResult) bool { return true })
	return err
}

func (r TxResult) createdEscrowID() (uint64, bool) {
	if id, ok := createdEscrowIDFromEvents(r.Events); ok {
		return id, true
	}
	return 0, false
}

func createdEscrowIDFromEvents(events []TxEvent) (uint64, bool) {
	for _, event := range events {
		if event.Type != "devshard_escrow_created" {
			continue
		}
		for _, attr := range event.Attributes {
			if attr.Key != "escrow_id" {
				continue
			}
			id, err := strconv.ParseUint(attr.Value, 10, 64)
			if err == nil && id > 0 {
				return id, true
			}
		}
	}
	return 0, false
}
