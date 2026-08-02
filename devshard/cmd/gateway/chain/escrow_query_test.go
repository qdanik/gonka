package chain

import (
	"errors"
	"testing"
)

// found=false retires an escrow, so it must mean the chain said the escrow is absent and nothing else.
// A read that failed is returned as an error: retiring an escrow that still holds funds strands them.
func TestGetEscrowSeparatesAbsenceFromFailure(t *testing.T) {
	testCases := []struct {
		name      string
		escrow    EscrowInfo
		present   bool
		failure   error
		wantFound bool
		wantErr   bool
	}{
		{name: "present", escrow: EscrowInfo{EscrowID: "7", Balance: 500}, present: true, wantFound: true},
		{name: "absent"},
		{name: "read_failed", failure: errTransportRefused, wantErr: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			transport := newFakeTransport()
			transport.escrow = testCase.escrow
			transport.escrowRaw = testCase.present
			transport.escrowErr = testCase.failure
			client := newFakeTxClient(t, transport)

			info, found, err := client.GetEscrow(t.Context(), "7")

			if testCase.wantErr {
				if !errors.Is(err, errTransportRefused) {
					t.Fatalf("err = %v, want the transport failure", err)
				}
				if found {
					t.Fatal("a failed read reported the escrow as present")
				}
				return
			}
			if err != nil {
				t.Fatalf("GetEscrow: %v", err)
			}
			if found != testCase.wantFound {
				t.Fatalf("found = %v, want %v", found, testCase.wantFound)
			}
			if found && info.Balance != testCase.escrow.Balance {
				t.Fatalf("balance = %d, want %d", info.Balance, testCase.escrow.Balance)
			}
		})
	}
}

// An id the chain cannot be asked about is a caller error, not an absent escrow.
func TestGetEscrowRejectsANonNumericID(t *testing.T) {
	client := newFakeTxClient(t, newFakeTransport())

	_, found, err := client.GetEscrow(t.Context(), "not-a-number")

	if err == nil {
		t.Fatal("want an error for an unusable escrow id, got nil")
	}
	if found {
		t.Fatal("an unusable id reported an escrow as present")
	}
}
