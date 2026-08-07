package chain

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"devshard/signing"
)

// newFakeTxClient wires a client to a transport that answers in process. Nothing here reaches a
// connection, so a test that expects no call can prove it by the transport's own records.
func newFakeTxClient(t *testing.T, transport *fakeTransport) *TxClient {
	t.Helper()
	client, err := NewTxClient(Config{
		Transport:    transport,
		PollInterval: time.Millisecond,
		PollTimeout:  time.Second,
		Now:          func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}
	return client
}

func TestNewTxClientRequiresATransport(t *testing.T) {
	_, err := NewTxClient(Config{})
	if err == nil {
		t.Fatal("want an error when no transport is given, got nil")
	}
}

func TestNewTxClientAppliesDefaultsForZeroFields(t *testing.T) {
	client, err := NewTxClient(Config{Transport: newFakeTransport()})
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
	if client.now == nil {
		t.Error("now default not applied")
	}
}

// The guards reject before anything is signed or sent, which the transport's empty record proves.
func TestCreateEscrowValidatesInput(t *testing.T) {
	validSigner := fixedSigner(t)

	testCases := []struct {
		name    string
		signer  *signing.Secp256k1Signer
		amount  uint64
		modelID string
	}{
		{name: "nil_signer", amount: 1_000_000, modelID: fixedModelID},
		{name: "zero_amount", signer: validSigner, modelID: fixedModelID},
		{name: "blank_model_id", signer: validSigner, amount: 1_000_000, modelID: "   "},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			transport := newFakeTransport()
			client := newFakeTxClient(t, transport)

			_, err := client.CreateEscrow(t.Context(), testCase.signer, testCase.amount, testCase.modelID, nil)

			if err == nil {
				t.Fatal("want a validation error, got nil")
			}
			if len(transport.broadcasts()) != 0 {
				t.Fatal("a rejected request still reached the chain")
			}
		})
	}
}

// The intent is recorded before the broadcast, because the broadcast cannot be taken back: a crash
// between the two leaves an escrow on chain that the gateway can still find by its hash.
func TestCreateEscrowRecordsTheIntentBeforeItBroadcasts(t *testing.T) {
	signer := fixedSigner(t)
	transport := newFakeTransport()
	transport.account = Account{Number: 7}
	client := newFakeTxClient(t, transport)

	var recordedHash string
	var recordedBeforeBroadcast bool
	onPrepared := func(txHash string) error {
		recordedHash = txHash
		recordedBeforeBroadcast = len(transport.broadcasts()) == 0
		transport.setTx(txHash, escrowCreatedResult("42"))
		return nil
	}

	result, err := client.CreateEscrow(t.Context(), signer, 1_000_000, fixedModelID, onPrepared)

	if err != nil {
		t.Fatalf("CreateEscrow: %v", err)
	}
	if !recordedBeforeBroadcast {
		t.Fatal("the intent was recorded after the broadcast, so a crash between them loses the escrow")
	}
	if result.TxHash != recordedHash {
		t.Fatalf("TxHash = %q, want the hash the intent was recorded under %q", result.TxHash, recordedHash)
	}
	if result.EscrowID != 42 {
		t.Fatalf("EscrowID = %d, want the id from the commit event", result.EscrowID)
	}
	if result.Creator != signer.Address() {
		t.Fatalf("Creator = %q, want %q", result.Creator, signer.Address())
	}
}

func TestCreateEscrowOnPreparedErrorAbortsBeforeBroadcast(t *testing.T) {
	transport := newFakeTransport()
	client := newFakeTxClient(t, transport)

	_, err := client.CreateEscrow(t.Context(), fixedSigner(t), 1_000_000, fixedModelID, func(string) error {
		return errors.New("store unavailable")
	})

	if err == nil {
		t.Fatal("want the intent failure to abort the create, got nil")
	}
	if len(transport.broadcasts()) != 0 {
		t.Fatal("an escrow was created on chain after the intent write failed")
	}
}

// The hash is computed before the broadcast and must match what the node acknowledges: the intent is
// filed under the local one, so a divergence means recovery would look for a transaction nobody has.
func TestCreateEscrowHashMismatchErrors(t *testing.T) {
	transport := newFakeTransport()
	transport.broadcastHash = "0000000000000000000000000000000000000000000000000000000000000000"
	client := newFakeTxClient(t, transport)

	_, err := client.CreateEscrow(t.Context(), fixedSigner(t), 1_000_000, fixedModelID, nil)

	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("CreateEscrow = %v, want a hash mismatch", err)
	}
}

func TestSettleEscrowValidatesInput(t *testing.T) {
	validSigner := fixedSigner(t)

	testCases := []struct {
		name   string
		signer *signing.Secp256k1Signer
		input  SettlementInput
	}{
		{name: "nil_signer", input: fixedSettlementFull()},
		{name: "zero_escrow_id", signer: validSigner, input: SettlementInput{}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			transport := newFakeTransport()
			client := newFakeTxClient(t, transport)

			_, err := client.SettleEscrow(t.Context(), testCase.signer, testCase.input, nil)

			if err == nil {
				t.Fatal("want a validation error, got nil")
			}
			if len(transport.broadcasts()) != 0 {
				t.Fatal("a rejected settlement still reached the chain")
			}
		})
	}
}

// Settlement waits for the transaction to execute, not merely to be accepted. Sync broadcast reports
// only that the transaction entered the queue, and the caller destroys the key that could retry, so
// returning early would strand the deposit of every settle the chain later rejects.
func TestSettleEscrowWaitsForTheTransactionToCommit(t *testing.T) {
	signer := fixedSigner(t)
	transport := newFakeTransport()
	transport.account = Account{Number: 9}
	// The transaction appears only on the second poll, so a client that did not wait would miss it.
	var polls atomic.Int64
	transport.onTx = func(call int) {
		polls.Store(int64(call))
		if call >= 2 {
			for _, sent := range transport.broadcasts() {
				transport.setTx(txHashFromBytes(sent), TxResult{})
			}
		}
	}
	client := newFakeTxClient(t, transport)
	input := fixedSettlementFull()

	result, err := client.SettleEscrow(t.Context(), signer, input, nil)

	if err != nil {
		t.Fatalf("SettleEscrow: %v", err)
	}
	if polls.Load() < 2 {
		t.Fatalf("polls = %d, want the settle to wait past the first answer", polls.Load())
	}
	if result.EscrowID != input.EscrowID {
		t.Fatalf("EscrowID = %d, want %d", result.EscrowID, input.EscrowID)
	}
	if result.Settler != signer.Address() {
		t.Fatalf("Settler = %q, want %q", result.Settler, signer.Address())
	}
}

// A settlement the chain accepted and then rejected must reach the caller as a failure: it is the only
// signal that the deposit is still there and the row must not be cleared.
func TestSettleEscrowFailsWhenTheChainRejectsTheCommittedTransaction(t *testing.T) {
	transport := newFakeTransport()
	transport.onTx = func(int) {
		for _, sent := range transport.broadcasts() {
			transport.setTx(txHashFromBytes(sent), TxResult{Code: 11, Codespace: "inference", RawLog: "settlement window closed"})
		}
	}
	client := newFakeTxClient(t, transport)

	_, err := client.SettleEscrow(t.Context(), fixedSigner(t), fixedSettlementFull(), nil)

	if err == nil || !strings.Contains(err.Error(), "settlement window closed") {
		t.Fatalf("SettleEscrow = %v, want the chain's rejection", err)
	}
}

// The three answers a caller must tell apart: the escrow id of a create that worked, "no escrow" for a
// transaction that committed and failed, and "not on chain" for one the chain does not have.
func TestGetTxEscrowIDThreeWaySemantics(t *testing.T) {
	testCases := []struct {
		name       string
		result     TxResult
		present    bool
		wantID     uint64
		wantFound  bool
		wantErr    error
		wantNoErr  bool
		transports func(*fakeTransport)
	}{
		{name: "created", result: escrowCreatedResult("42"), present: true, wantID: 42, wantFound: true, wantNoErr: true},
		{name: "committed_but_failed", result: TxResult{Code: 5}, present: true, wantNoErr: true},
		{name: "not_on_chain", wantErr: ErrTxNotFound},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			transport := newFakeTransport()
			if testCase.present {
				transport.setTx("HASH", testCase.result)
			}
			client := newFakeTxClient(t, transport)

			escrowID, found, err := client.GetTxEscrowID(t.Context(), "HASH")

			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("err = %v, want %v", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetTxEscrowID: %v", err)
			}
			if escrowID != testCase.wantID || found != testCase.wantFound {
				t.Fatalf("got (%d, %v), want (%d, %v)", escrowID, found, testCase.wantID, testCase.wantFound)
			}
		})
	}
}

// A read that failed is not an absent transaction. Reading it as absence would rebuild and rebroadcast
// a settlement that already moved the money.
func TestATransportFailureIsNeverReadAsAbsence(t *testing.T) {
	transport := newFakeTransport()
	transport.txErr = errTransportRefused
	client := newFakeTxClient(t, transport)

	if _, _, err := client.GetTxEscrowID(t.Context(), "HASH"); !errors.Is(err, errTransportRefused) {
		t.Fatalf("GetTxEscrowID = %v, want the transport failure", err)
	}
	if _, err := client.TxCommitted(t.Context(), "HASH"); !errors.Is(err, errTransportRefused) {
		t.Fatalf("TxCommitted = %v, want the transport failure", err)
	}
}

func TestTxCommittedReportsExecution(t *testing.T) {
	testCases := []struct {
		name          string
		result        TxResult
		present       bool
		wantSucceeded bool
		wantErr       error
	}{
		{name: "executed", present: true, wantSucceeded: true},
		{name: "committed_but_failed", result: TxResult{Code: 3}, present: true},
		{name: "not_on_chain", wantErr: ErrTxNotFound},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			transport := newFakeTransport()
			if testCase.present {
				transport.setTx("HASH", testCase.result)
			}
			client := newFakeTxClient(t, transport)

			succeeded, err := client.TxCommitted(t.Context(), "HASH")

			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("err = %v, want %v", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("TxCommitted: %v", err)
			}
			if succeeded != testCase.wantSucceeded {
				t.Fatalf("succeeded = %v, want %v", succeeded, testCase.wantSucceeded)
			}
		})
	}
}

// The wait is bounded: a create whose transaction never lands must fail rather than hold the caller.
func TestWaitForCreatedEscrowIDTimesOutWhenNeverFound(t *testing.T) {
	transport := newFakeTransport()
	client, err := NewTxClient(Config{Transport: transport, PollInterval: time.Millisecond, PollTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}

	_, err = client.waitForCreatedEscrowID(t.Context(), "HASH")

	// The wording matters as much as the giving up: an operator who reads this as "the transaction
	// failed" creates a second escrow while the first is still landing.
	if err == nil || !strings.Contains(err.Error(), "not confirmed within") {
		t.Fatalf("waitForCreatedEscrowID = %v, want a bounded wait that reports the broadcast", err)
	}
	if !strings.Contains(err.Error(), "do not create another") {
		t.Fatalf("waitForCreatedEscrowID = %v, want the caller told not to retry the creation", err)
	}
}

func TestWaitForCreatedEscrowIDReturnsOnContextCancel(t *testing.T) {
	transport := newFakeTransport()
	client, err := NewTxClient(Config{Transport: transport, PollInterval: time.Hour, PollTimeout: time.Hour})
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := client.waitForCreatedEscrowID(ctx, "HASH"); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForCreatedEscrowID = %v, want context.Canceled", err)
	}
}
