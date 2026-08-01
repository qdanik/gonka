// Package chain builds/signs/broadcasts gonka escrow transactions and
// observes chain phase state.
package chain

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"devshard/logging"
	"devshard/signing"

	json "github.com/goccy/go-json"
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

// TxClient builds, signs, broadcasts, and queries gonka escrow transactions
// over the Cosmos REST API.
type TxClient struct {
	baseURL      string
	txQueryURLs  []string
	chainID      string
	feeDenom     string
	feeAmount    uint64
	gasLimit     uint64
	pollInterval time.Duration
	pollTimeout  time.Duration
	client       *http.Client
	now          func() time.Time
}

// Config configures a TxClient. Zero-value fee/gas/poll/client/clock fields
// take package defaults; RESTBaseURL is required.
type Config struct {
	RESTBaseURL         string
	TxQueryFallbackURLs []string
	ChainID             string
	FeeDenom            string
	FeeAmount           uint64
	GasLimit            uint64
	PollInterval        time.Duration
	PollTimeout         time.Duration
	HTTPClient          *http.Client
	Now                 func() time.Time
}

type CreateEscrowResult struct {
	EscrowID uint64
	TxHash   string
	Creator  string
}

type SettleEscrowResult struct {
	EscrowID uint64
	TxHash   string
	Settler  string
}

// NewTxClient validates cfg and applies defaults for unset fields; it errors
// only when RESTBaseURL is blank.
func NewTxClient(cfg Config) (*TxClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.RESTBaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("chain REST URL is required")
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
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &TxClient{
		baseURL:      baseURL,
		txQueryURLs:  buildTxQueryURLs(baseURL, cfg.TxQueryFallbackURLs),
		chainID:      strings.TrimSpace(cfg.ChainID),
		feeDenom:     feeDenom,
		feeAmount:    feeAmount,
		gasLimit:     gasLimit,
		pollInterval: pollInterval,
		pollTimeout:  pollTimeout,
		client:       client,
		now:          now,
	}, nil
}

// buildTxQueryURLs dedupes fallbacks against baseURL, keeping baseURL first.
func buildTxQueryURLs(baseURL string, fallbacks []string) []string {
	urls := make([]string, 0, len(fallbacks)+1)
	seen := make(map[string]bool, len(fallbacks)+1)
	add := func(raw string) {
		trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
		if trimmed == "" || seen[trimmed] {
			return
		}
		seen[trimmed] = true
		urls = append(urls, trimmed)
	}
	add(baseURL)
	for _, fallback := range fallbacks {
		add(fallback)
	}
	return urls
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
	account, err := c.fetchAccount(ctx, creator)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	ttl := c.now().Add(UnorderedTxTTL)
	txBytes, err := buildCreateEscrowTx(signer, chainID, account.AccountNumber, c.feeDenom, c.feeAmount, c.gasLimit, amount, modelID, ttl)
	if err != nil {
		return CreateEscrowResult{}, err
	}
	txHash := txHashFromBytes(txBytes)
	if onPrepared != nil {
		if err := onPrepared(txHash); err != nil {
			return CreateEscrowResult{}, fmt.Errorf("record escrow create intent before broadcast: %w", err)
		}
	}
	nodeHash, err := c.broadcastTx(ctx, txBytes)
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
	account, err := c.fetchAccount(ctx, settler)
	if err != nil {
		return SettleEscrowResult{}, err
	}
	ttl := c.now().Add(UnorderedTxTTL)
	txBytes, err := buildSettleEscrowTx(signer, chainID, account.AccountNumber, c.feeDenom, c.feeAmount, c.gasLimit, settler, input, ttl)
	if err != nil {
		return SettleEscrowResult{}, err
	}
	txHash := txHashFromBytes(txBytes)
	if onPrepared != nil {
		if err := onPrepared(txHash); err != nil {
			return SettleEscrowResult{}, fmt.Errorf("record settle intent before broadcast: %w", err)
		}
	}
	nodeHash, err := c.broadcastTx(ctx, txBytes)
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
	if c.chainID != "" {
		return c.chainID, nil
	}
	return c.fetchChainID(ctx)
}

// GetTxEscrowID reports found=false only when every reachable endpoint agrees the tx is absent or
// committed-but-failed; a per-endpoint error is never read as absence. See
// gateway-escrow-lifecycle.md, "Creating an escrow, and surviving a crash mid-creation".
func (c *TxClient) GetTxEscrowID(ctx context.Context, txHash string) (uint64, bool, error) {
	var lastErr error
	sawNotFound := false
	for _, baseURL := range c.txQueryURLs {
		var payload txResponseEnvelope
		err := c.getJSONFromBaseURL(ctx, baseURL, "/cosmos/tx/v1beta1/txs/"+url.PathEscape(txHash), &payload)
		if err != nil {
			if isNotFoundError(err) {
				sawNotFound = true // this endpoint lacks it; try the fallback before deciding
				continue
			}
			lastErr = fmt.Errorf("%s: %w", baseURL, err)
			continue
		}
		if payload.TxResponse.Code != 0 {
			return 0, false, nil // tx committed but failed -> no escrow created
		}
		if escrowID, ok := payload.TxResponse.createdEscrowID(); ok {
			return escrowID, true, nil
		}
		lastErr = fmt.Errorf("tx %s committed via %s but escrow_id event was not found", txHash, baseURL)
	}
	// Only conclude "not on chain" when every reachable endpoint agreed on 404.
	if lastErr == nil && sawNotFound {
		return 0, false, ErrTxNotFound
	}
	return 0, false, lastErr
}

// TxCommitted reports whether a transaction reached the chain and succeeded there. It keeps
// GetTxEscrowID's discipline: a per-endpoint error is never read as absence, and only agreement across
// every reachable endpoint concludes the transaction is not on chain.
func (c *TxClient) TxCommitted(ctx context.Context, txHash string) (succeeded bool, err error) {
	var lastErr error
	sawNotFound := false
	for _, baseURL := range c.txQueryURLs {
		var payload txResponseEnvelope
		fetchErr := c.getJSONFromBaseURL(ctx, baseURL, "/cosmos/tx/v1beta1/txs/"+url.PathEscape(txHash), &payload)
		if fetchErr != nil {
			if isNotFoundError(fetchErr) {
				sawNotFound = true
				continue
			}
			lastErr = fmt.Errorf("%s: %w", baseURL, fetchErr)
			continue
		}
		return payload.TxResponse.Code == 0, nil
	}
	if lastErr == nil && sawNotFound {
		return false, ErrTxNotFound
	}
	return false, lastErr
}

func (c *TxClient) fetchChainID(ctx context.Context) (string, error) {
	var payload any
	if err := c.getJSONFromBaseURL(ctx, c.baseURL, "/cosmos/base/tendermint/v1beta1/node_info", &payload); err != nil {
		return "", fmt.Errorf("fetch chain id: %w", err)
	}
	chainID := findStringField(payload, "network")
	if chainID == "" {
		return "", fmt.Errorf("chain id not found in node_info response")
	}
	return chainID, nil
}

type chainAccount struct {
	AccountNumber uint64
	Sequence      uint64
}

// fetchAccount looks up account_number/sequence, walking nested JSON shapes
// (e.g. vesting accounts wrap the base account several levels deep).
func (c *TxClient) fetchAccount(ctx context.Context, address string) (chainAccount, error) {
	var payload any
	if err := c.getJSONFromBaseURL(ctx, c.baseURL, "/cosmos/auth/v1beta1/accounts/"+url.PathEscape(address), &payload); err != nil {
		return chainAccount{}, fmt.Errorf("fetch account %s: %w", address, err)
	}
	accountNumber, ok := findUintField(payload, "account_number")
	if !ok {
		return chainAccount{}, fmt.Errorf("account_number not found for %s", address)
	}
	sequence, ok := findUintField(payload, "sequence")
	if !ok {
		return chainAccount{}, fmt.Errorf("sequence not found for %s", address)
	}
	return chainAccount{AccountNumber: accountNumber, Sequence: sequence}, nil
}

// broadcastTx submits txBytes in sync mode and returns the node-assigned hash.
func (c *TxClient) broadcastTx(ctx context.Context, txBytes []byte) (string, error) {
	reqBody := map[string]string{
		"tx_bytes": base64.StdEncoding.EncodeToString(txBytes),
		"mode":     "BROADCAST_MODE_SYNC",
	}
	var payload txResponseEnvelope
	if err := c.postJSON(ctx, "/cosmos/tx/v1beta1/txs", reqBody, &payload); err != nil {
		return "", fmt.Errorf("broadcast tx: %w", err)
	}
	if payload.TxResponse.Code != 0 {
		return "", fmt.Errorf("broadcast tx failed code=%d codespace=%s raw_log=%s", payload.TxResponse.Code, payload.TxResponse.Codespace, payload.TxResponse.RawLog)
	}
	txHash := strings.TrimSpace(payload.TxResponse.TxHash)
	if txHash == "" {
		return "", fmt.Errorf("broadcast response missing txhash")
	}
	return txHash, nil
}

// awaitTx polls txQueryURLs until a committed response satisfies ready, pollTimeout elapses, or ctx is
// done; a 404 is "not indexed yet" rather than the terminal not-found GetTxEscrowID reports. A non-zero
// code is returned as an error, and it is the only way to learn a DeliverTx failure: broadcasting in
// BROADCAST_MODE_SYNC reports CheckTx alone, so a transaction can be accepted and still never execute.
func (c *TxClient) awaitTx(ctx context.Context, txHash string, ready func(txResponse) bool) (txResponse, error) {
	deadline := c.now().Add(c.pollTimeout)
	var lastErr error
	for {
		for _, baseURL := range c.txQueryURLs {
			var payload txResponseEnvelope
			err := c.getJSONFromBaseURL(ctx, baseURL, "/cosmos/tx/v1beta1/txs/"+url.PathEscape(txHash), &payload)
			if err != nil {
				lastErr = fmt.Errorf("%s: %w", baseURL, err)
				continue
			}
			if payload.TxResponse.Code != 0 {
				return txResponse{}, fmt.Errorf("tx %s failed code=%d codespace=%s raw_log=%s", txHash, payload.TxResponse.Code, payload.TxResponse.Codespace, payload.TxResponse.RawLog)
			}
			if ready(payload.TxResponse) {
				return payload.TxResponse, nil
			}
			lastErr = fmt.Errorf("tx %s committed via %s but is not yet complete", txHash, baseURL)
		}
		if c.now().After(deadline) {
			if lastErr != nil {
				return txResponse{}, fmt.Errorf("wait for tx %s: %w", txHash, lastErr)
			}
			return txResponse{}, fmt.Errorf("wait for tx %s timed out", txHash)
		}
		select {
		case <-ctx.Done():
			return txResponse{}, ctx.Err()
		case <-time.After(c.pollInterval):
		}
	}
}

func (c *TxClient) waitForCreatedEscrowID(ctx context.Context, txHash string) (uint64, error) {
	committed, err := c.awaitTx(ctx, txHash, func(response txResponse) bool {
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
	_, err := c.awaitTx(ctx, txHash, func(txResponse) bool { return true })
	return err
}

type chainHTTPError struct {
	method string
	path   string
	status int
	body   string
}

func (e *chainHTTPError) Error() string {
	return fmt.Sprintf("%s %s status %d: %s", e.method, e.path, e.status, e.body)
}

func (e *chainHTTPError) StatusCode() int {
	return e.status
}

func isNotFoundError(err error) bool {
	var httpErr *chainHTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode() == http.StatusNotFound
}

func (c *TxClient) getJSONFromBaseURL(ctx context.Context, baseURL, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &chainHTTPError{method: http.MethodGet, path: path, status: resp.StatusCode, body: strings.TrimSpace(string(body))}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *TxClient) postJSON(ctx context.Context, path string, in, out any) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("POST %s status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type txResponseEnvelope struct {
	TxResponse txResponse `json:"tx_response"`
}

type txResponse struct {
	Code      uint32      `json:"code"`
	Codespace string      `json:"codespace"`
	TxHash    string      `json:"txhash"`
	RawLog    string      `json:"raw_log"`
	Events    []txEvent   `json:"events"`
	Logs      []txLogItem `json:"logs"`
}

type txLogItem struct {
	Events []txEvent `json:"events"`
}

type txEvent struct {
	Type       string        `json:"type"`
	Attributes []txAttribute `json:"attributes"`
}

type txAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// createdEscrowID checks top-level events first, then falls back to
// per-message logs (older node responses nest events under logs only).
func (r txResponse) createdEscrowID() (uint64, bool) {
	if id, ok := createdEscrowIDFromEvents(r.Events); ok {
		return id, true
	}
	for _, logItem := range r.Logs {
		if id, ok := createdEscrowIDFromEvents(logItem.Events); ok {
			return id, true
		}
	}
	return 0, false
}

func createdEscrowIDFromEvents(events []txEvent) (uint64, bool) {
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

func findStringField(node any, key string) string {
	switch typed := node.(type) {
	case map[string]any:
		if raw, ok := typed[key]; ok {
			if value, ok := raw.(string); ok {
				return value
			}
		}
		for _, child := range typed {
			if found := findStringField(child, key); found != "" {
				return found
			}
		}
	case []any:
		for _, child := range typed {
			if found := findStringField(child, key); found != "" {
				return found
			}
		}
	}
	return ""
}

func findUintField(node any, key string) (uint64, bool) {
	switch typed := node.(type) {
	case map[string]any:
		if raw, ok := typed[key]; ok {
			if parsed, ok := parseJSONUint(raw); ok {
				return parsed, true
			}
		}
		for _, child := range typed {
			if found, ok := findUintField(child, key); ok {
				return found, true
			}
		}
	case []any:
		for _, child := range typed {
			if found, ok := findUintField(child, key); ok {
				return found, true
			}
		}
	}
	return 0, false
}

func parseJSONUint(raw any) (uint64, bool) {
	switch value := raw.(type) {
	case string:
		parsed, err := strconv.ParseUint(value, 10, 64)
		return parsed, err == nil
	case float64:
		if value < 0 || value != float64(uint64(value)) {
			return 0, false
		}
		return uint64(value), true
	default:
		return 0, false
	}
}
