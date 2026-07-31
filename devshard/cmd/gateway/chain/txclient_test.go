package chain

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"devshard/signing"
)

func writeJSONResponse(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encode stub response: %v", err)
	}
}

// newUnreachableTxClient returns a client pointed at a closed server, so any
// call that reaches the network fails fast — used to prove input-validation
// guards short-circuit before a request is attempted.
func newUnreachableTxClient(t *testing.T) *TxClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	client, err := NewTxClient(Config{RESTBaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}
	return client
}

func TestNewTxClientRequiresRESTBaseURL(t *testing.T) {
	_, err := NewTxClient(Config{})
	if err == nil {
		t.Fatal("want error for empty RESTBaseURL, got nil")
	}
}

func TestNewTxClientAppliesDefaultsForZeroFields(t *testing.T) {
	client, err := NewTxClient(Config{RESTBaseURL: "http://chain.example.invalid"})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}
	if client.feeDenom != DefaultFeeDenom {
		t.Errorf("feeDenom = %q, want %q", client.feeDenom, DefaultFeeDenom)
	}
	if client.feeAmount != DefaultFeeAmount {
		t.Errorf("feeAmount = %d, want %d", client.feeAmount, DefaultFeeAmount)
	}
	if client.gasLimit != DefaultGasLimit {
		t.Errorf("gasLimit = %d, want %d", client.gasLimit, DefaultGasLimit)
	}
	if client.pollInterval != DefaultPollInterval {
		t.Errorf("pollInterval = %v, want %v", client.pollInterval, DefaultPollInterval)
	}
	if client.pollTimeout != DefaultPollTimeout {
		t.Errorf("pollTimeout = %v, want %v", client.pollTimeout, DefaultPollTimeout)
	}
	if client.client == nil {
		t.Error("HTTP client default not applied")
	}
	if client.now == nil {
		t.Error("now default not applied")
	}
}

func TestBuildTxQueryURLsDedupesWithBaseURLFirst(t *testing.T) {
	got := buildTxQueryURLs("http://primary/", []string{"http://fallback/", "http://primary", "", "http://fallback/"})
	want := []string{"http://primary", "http://fallback"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildTxQueryURLs = %v, want %v", got, want)
	}
}

// TestCreateEscrowValidatesInput asserts the guards reject before any network call.
func TestCreateEscrowValidatesInput(t *testing.T) {
	client := newUnreachableTxClient(t)
	validSigner := fixedSigner(t)

	tests := []struct {
		name    string
		signer  *signing.Secp256k1Signer
		amount  uint64
		modelID string
	}{
		{"nil signer", nil, 1_000_000, fixedModelID},
		{"zero amount", validSigner, 0, fixedModelID},
		{"blank model id", validSigner, 1_000_000, "   "},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := client.CreateEscrow(t.Context(), testCase.signer, testCase.amount, testCase.modelID, nil)
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
		})
	}
}

// TestCreateEscrowHappyPath drives the full node_info+account+broadcast+tx-query
// flow and asserts onPrepared runs strictly before broadcast is ever attempted.
func TestCreateEscrowHappyPath(t *testing.T) {
	signer := fixedSigner(t)

	var onPreparedCalled atomic.Bool
	var broadcastCalled atomic.Bool
	var broadcastSawOnPreparedDone atomic.Bool
	var broadcastTxHash atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cosmos/base/tendermint/v1beta1/node_info":
			writeJSONResponse(t, w, map[string]any{
				"default_node_info": map[string]any{"network": "gonka-test"},
			})
		case r.URL.Path == "/cosmos/auth/v1beta1/accounts/"+signer.Address():
			writeJSONResponse(t, w, map[string]any{
				"account": map[string]any{"account_number": "7", "sequence": "0"},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/cosmos/tx/v1beta1/txs":
			broadcastCalled.Store(true)
			broadcastSawOnPreparedDone.Store(onPreparedCalled.Load())
			var req struct {
				TxBytes string `json:"tx_bytes"`
				Mode    string `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode broadcast request: %v", err)
			}
			if req.Mode != "BROADCAST_MODE_SYNC" {
				t.Errorf("mode = %q, want BROADCAST_MODE_SYNC", req.Mode)
			}
			txBytes, err := base64.StdEncoding.DecodeString(req.TxBytes)
			if err != nil {
				t.Errorf("decode tx_bytes: %v", err)
			}
			hash := txHashFromBytes(txBytes)
			broadcastTxHash.Store(hash)
			writeJSONResponse(t, w, map[string]any{"tx_response": map[string]any{"code": 0, "txhash": hash}})
		case strings.HasPrefix(r.URL.Path, "/cosmos/tx/v1beta1/txs/"):
			writeJSONResponse(t, w, map[string]any{
				"tx_response": map[string]any{
					"code": 0,
					"events": []map[string]any{{
						"type":       "devshard_escrow_created",
						"attributes": []map[string]string{{"key": "escrow_id", "value": "42"}},
					}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewTxClient(Config{
		RESTBaseURL:  server.URL,
		PollInterval: time.Millisecond,
		PollTimeout:  time.Second,
		Now:          func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	onPrepared := func(txHash string) error {
		if txHash == "" {
			t.Error("onPrepared called with empty tx hash")
		}
		onPreparedCalled.Store(true)
		return nil
	}

	result, err := client.CreateEscrow(t.Context(), signer, 1_000_000, fixedModelID, onPrepared)
	if err != nil {
		t.Fatalf("CreateEscrow: %v", err)
	}
	if !onPreparedCalled.Load() {
		t.Fatal("onPrepared was never called")
	}
	if !broadcastCalled.Load() {
		t.Fatal("broadcast was never called")
	}
	if !broadcastSawOnPreparedDone.Load() {
		t.Fatal("broadcast ran before onPrepared completed")
	}
	if result.EscrowID != 42 {
		t.Errorf("EscrowID = %d, want 42", result.EscrowID)
	}
	if result.Creator != signer.Address() {
		t.Errorf("Creator = %q, want %q", result.Creator, signer.Address())
	}
	wantHash, _ := broadcastTxHash.Load().(string)
	if result.TxHash != wantHash {
		t.Errorf("TxHash = %q, want %q", result.TxHash, wantHash)
	}
}

// TestCreateEscrowOnPreparedErrorAbortsBeforeBroadcast pins the crash-recovery
// ordering: a failed intent write must prevent the irreversible broadcast.
func TestCreateEscrowOnPreparedErrorAbortsBeforeBroadcast(t *testing.T) {
	signer := fixedSigner(t)
	var broadcastCalled atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cosmos/base/tendermint/v1beta1/node_info":
			writeJSONResponse(t, w, map[string]any{"default_node_info": map[string]any{"network": "gonka-test"}})
		case r.URL.Path == "/cosmos/auth/v1beta1/accounts/"+signer.Address():
			writeJSONResponse(t, w, map[string]any{"account": map[string]any{"account_number": "7", "sequence": "0"}})
		case r.Method == http.MethodPost && r.URL.Path == "/cosmos/tx/v1beta1/txs":
			broadcastCalled.Store(true)
			writeJSONResponse(t, w, map[string]any{"tx_response": map[string]any{"code": 0, "txhash": "UNUSED"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewTxClient(Config{RESTBaseURL: server.URL, PollInterval: time.Millisecond, PollTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	wantErr := errors.New("store failed")
	onPrepared := func(string) error { return wantErr }

	_, err = client.CreateEscrow(t.Context(), signer, 1_000_000, fixedModelID, onPrepared)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapping %v", err, wantErr)
	}
	const wantPrefix = "record escrow create intent before broadcast:"
	if !strings.Contains(err.Error(), wantPrefix) {
		t.Fatalf("err = %q, want to contain %q", err.Error(), wantPrefix)
	}
	if broadcastCalled.Load() {
		t.Fatal("broadcast must not be called when onPrepared fails")
	}
}

func TestCreateEscrowHashMismatchErrors(t *testing.T) {
	signer := fixedSigner(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cosmos/base/tendermint/v1beta1/node_info":
			writeJSONResponse(t, w, map[string]any{"default_node_info": map[string]any{"network": "gonka-test"}})
		case r.URL.Path == "/cosmos/auth/v1beta1/accounts/"+signer.Address():
			writeJSONResponse(t, w, map[string]any{"account": map[string]any{"account_number": "7", "sequence": "0"}})
		case r.Method == http.MethodPost && r.URL.Path == "/cosmos/tx/v1beta1/txs":
			// Node echoes back an unrelated hash instead of the precomputed one.
			writeJSONResponse(t, w, map[string]any{"tx_response": map[string]any{"code": 0, "txhash": strings.Repeat("0", 64)}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewTxClient(Config{RESTBaseURL: server.URL, PollInterval: time.Millisecond, PollTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	_, err = client.CreateEscrow(t.Context(), signer, 1_000_000, fixedModelID, nil)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "tx hash mismatch") {
		t.Fatalf("err = %q, want to contain %q", err.Error(), "tx hash mismatch")
	}
}

// TestSettleEscrowValidatesInput asserts the guards reject before any network call.
func TestSettleEscrowValidatesInput(t *testing.T) {
	client := newUnreachableTxClient(t)
	validSigner := fixedSigner(t)
	validInput := fixedSettlementFull()

	tests := []struct {
		name   string
		signer *signing.Secp256k1Signer
		input  SettlementInput
	}{
		{"nil signer", nil, validInput},
		{"zero escrow id", validSigner, SettlementInput{EscrowID: 0}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := client.SettleEscrow(t.Context(), testCase.signer, testCase.input)
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
		})
	}
}

// TestSettleEscrowReturnsNodeHashWithoutPolling pins the create/settle
// asymmetry: settlement has no intent hook and never polls tx-query.
func TestSettleEscrowReturnsNodeHashWithoutPolling(t *testing.T) {
	signer := fixedSigner(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cosmos/auth/v1beta1/accounts/"+signer.Address():
			writeJSONResponse(t, w, map[string]any{"account": map[string]any{"account_number": "9", "sequence": "0"}})
		case r.Method == http.MethodPost && r.URL.Path == "/cosmos/tx/v1beta1/txs":
			writeJSONResponse(t, w, map[string]any{"tx_response": map[string]any{"code": 0, "txhash": "DEF456"}})
		case strings.HasPrefix(r.URL.Path, "/cosmos/tx/v1beta1/txs/"):
			t.Errorf("settlement must not poll the tx-query endpoint: %s", r.URL.Path)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewTxClient(Config{RESTBaseURL: server.URL, ChainID: "gonka-test", PollInterval: time.Millisecond, PollTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	input := fixedSettlementFull()
	result, err := client.SettleEscrow(t.Context(), signer, input)
	if err != nil {
		t.Fatalf("SettleEscrow: %v", err)
	}
	if result.TxHash != "DEF456" {
		t.Errorf("TxHash = %q, want %q", result.TxHash, "DEF456")
	}
	if result.EscrowID != input.EscrowID {
		t.Errorf("EscrowID = %d, want %d", result.EscrowID, input.EscrowID)
	}
	if result.Settler != signer.Address() {
		t.Errorf("Settler = %q, want %q", result.Settler, signer.Address())
	}
}

// TestFetchAccountFindsNestedAccountFields covers vesting/wrapped account
// shapes where account_number/sequence sit several levels deep.
func TestFetchAccountFindsNestedAccountFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{
			"account": map[string]any{
				"base_vesting_account": map[string]any{
					"base_account": map[string]any{
						"account_number": "9",
						"sequence":       "13",
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewTxClient(Config{RESTBaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	account, err := client.fetchAccount(t.Context(), "gonka1nested")
	if err != nil {
		t.Fatalf("fetchAccount: %v", err)
	}
	if account.AccountNumber != 9 {
		t.Errorf("AccountNumber = %d, want 9", account.AccountNumber)
	}
	if account.Sequence != 13 {
		t.Errorf("Sequence = %d, want 13", account.Sequence)
	}
}

// TestGetTxEscrowIDThreeWaySemantics pins the multi-endpoint resolution rules:
// ErrTxNotFound only when every endpoint agrees the tx is absent; a genuine
// error from any endpoint is never conflated with "not found".
func TestGetTxEscrowIDThreeWaySemantics(t *testing.T) {
	const stubTxHash = "ABC123"
	notFoundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	transientErrorHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "transaction indexing is disabled", http.StatusInternalServerError)
	})

	tests := []struct {
		name         string
		primary      http.HandlerFunc
		fallback     http.HandlerFunc
		wantEscrowID uint64
		wantFound    bool
		checkErr     func(t *testing.T, err error)
	}{
		{
			name:     "all endpoints 404 -> ErrTxNotFound",
			primary:  notFoundHandler.ServeHTTP,
			fallback: notFoundHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrTxNotFound) {
					t.Fatalf("err = %v, want ErrTxNotFound", err)
				}
			},
		},
		{
			name: "committed but failed -> not found, no error",
			primary: func(w http.ResponseWriter, r *http.Request) {
				writeJSONResponse(t, w, map[string]any{"tx_response": map[string]any{"code": 5}})
			},
			fallback: notFoundHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name: "found on primary -> escrow id",
			primary: func(w http.ResponseWriter, r *http.Request) {
				writeJSONResponse(t, w, map[string]any{
					"tx_response": map[string]any{
						"code": 0,
						"events": []map[string]any{{
							"type":       "devshard_escrow_created",
							"attributes": []map[string]string{{"key": "escrow_id", "value": "77"}},
						}},
					},
				})
			},
			fallback:     notFoundHandler.ServeHTTP,
			wantEscrowID: 77,
			wantFound:    true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name:    "found on fallback after primary 404 -> escrow id",
			primary: notFoundHandler.ServeHTTP,
			fallback: func(w http.ResponseWriter, r *http.Request) {
				writeJSONResponse(t, w, map[string]any{
					"tx_response": map[string]any{
						"code": 0,
						"events": []map[string]any{{
							"type":       "devshard_escrow_created",
							"attributes": []map[string]string{{"key": "escrow_id", "value": "99"}},
						}},
					},
				})
			},
			wantEscrowID: 99,
			wantFound:    true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name:     "one 404 one transient -> inconclusive, not ErrTxNotFound",
			primary:  notFoundHandler.ServeHTTP,
			fallback: transientErrorHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("want non-nil error")
				}
				if errors.Is(err, ErrTxNotFound) {
					t.Fatalf("err = %v, must not be ErrTxNotFound (inconclusive)", err)
				}
			},
		},
		{
			name:    "one 404 one 500-with-not-found-body -> transient error, not ErrTxNotFound",
			primary: notFoundHandler.ServeHTTP,
			fallback: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"message":"tx not found"}`, http.StatusInternalServerError)
			},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("want non-nil error")
				}
				if errors.Is(err, ErrTxNotFound) {
					t.Fatalf("err = %v, must not be ErrTxNotFound (status-only 404 check)", err)
				}
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			primaryServer := httptest.NewServer(testCase.primary)
			defer primaryServer.Close()
			fallbackServer := httptest.NewServer(testCase.fallback)
			defer fallbackServer.Close()

			client, err := NewTxClient(Config{
				RESTBaseURL:         primaryServer.URL,
				TxQueryFallbackURLs: []string{fallbackServer.URL},
			})
			if err != nil {
				t.Fatalf("NewTxClient: %v", err)
			}

			escrowID, found, err := client.GetTxEscrowID(t.Context(), stubTxHash)
			testCase.checkErr(t, err)
			if found != testCase.wantFound {
				t.Errorf("found = %v, want %v", found, testCase.wantFound)
			}
			if escrowID != testCase.wantEscrowID {
				t.Errorf("escrowID = %d, want %d", escrowID, testCase.wantEscrowID)
			}
		})
	}
}

// TestGetTxEscrowIDParsesOlderNodeLogsEventsShape pins support for older-node
// responses that nest the escrow-created event under logs[].events[] instead
// of the top-level events[].
func TestGetTxEscrowIDParsesOlderNodeLogsEventsShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{
			"tx_response": map[string]any{
				"code": 0,
				"logs": []map[string]any{{
					"events": []map[string]any{{
						"type":       "devshard_escrow_created",
						"attributes": []map[string]string{{"key": "escrow_id", "value": "55"}},
					}},
				}},
			},
		})
	}))
	defer server.Close()

	client, err := NewTxClient(Config{RESTBaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	escrowID, found, err := client.GetTxEscrowID(t.Context(), "ABC123")
	if err != nil {
		t.Fatalf("GetTxEscrowID: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if escrowID != 55 {
		t.Errorf("escrowID = %d, want 55", escrowID)
	}
}

// TestBroadcastTxCodeNonzeroReturnsError pins the broadcast failure message shape.
func TestBroadcastTxCodeNonzeroReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{
			"tx_response": map[string]any{"code": 5, "codespace": "sdk", "raw_log": "insufficient funds"},
		})
	}))
	defer server.Close()

	client, err := NewTxClient(Config{RESTBaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	_, err = client.broadcastTx(t.Context(), []byte("tx-bytes"))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	const wantMsg = "broadcast tx failed code=5 codespace=sdk raw_log=insufficient funds"
	if !strings.Contains(err.Error(), wantMsg) {
		t.Fatalf("err = %q, want to contain %q", err.Error(), wantMsg)
	}
}

func TestWaitForCreatedEscrowIDTimesOutWhenNeverFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r) // tx never gets indexed
	}))
	defer server.Close()

	client, err := NewTxClient(Config{
		RESTBaseURL:  server.URL,
		PollInterval: time.Millisecond,
		PollTimeout:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	_, err = client.waitForCreatedEscrowID(t.Context(), "ABC123")
	if err == nil {
		t.Fatal("want timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "wait for tx ABC123") {
		t.Fatalf("err = %q, want to contain %q", err.Error(), "wait for tx ABC123")
	}
}

// TestWaitForCreatedEscrowIDReturnsOnContextCancel ends the poll loop as soon
// as ctx is done, independent of PollTimeout — proven with a long PollTimeout
// so a timeout-path bug would fail slow rather than pass by accident.
func TestWaitForCreatedEscrowIDReturnsOnContextCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := NewTxClient(Config{
		RESTBaseURL:  server.URL,
		PollInterval: time.Second,
		PollTimeout:  time.Minute,
	})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = client.waitForCreatedEscrowID(ctx, "ABC123")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestWaitForCreatedEscrowIDSucceedsViaFallbackURL pins the poll loop's use of
// every configured query URL, not just the first: the primary never indexes
// the tx, but the fallback does on the very first poll.
func TestWaitForCreatedEscrowIDSucceedsViaFallbackURL(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r) // primary never indexes the tx
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{
			"tx_response": map[string]any{
				"code": 0,
				"events": []map[string]any{{
					"type":       "devshard_escrow_created",
					"attributes": []map[string]string{{"key": "escrow_id", "value": "88"}},
				}},
			},
		})
	}))
	defer fallback.Close()

	client, err := NewTxClient(Config{
		RESTBaseURL:         primary.URL,
		TxQueryFallbackURLs: []string{fallback.URL},
		PollInterval:        time.Millisecond,
		PollTimeout:         time.Second,
	})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	escrowID, err := client.waitForCreatedEscrowID(t.Context(), "ABC123")
	if err != nil {
		t.Fatalf("waitForCreatedEscrowID: %v", err)
	}
	if escrowID != 88 {
		t.Errorf("escrowID = %d, want 88", escrowID)
	}
}
