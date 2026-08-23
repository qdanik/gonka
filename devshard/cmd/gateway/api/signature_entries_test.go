package api

import (
	"testing"

	"devshard/types"
	"devshard/user"
)

func slotsSigned(slots ...uint32) types.Bitmap128 {
	var signed types.Bitmap128
	for _, slot := range slots {
		signed.Set(slot)
	}
	return signed
}

// The question this route answers is "why will this escrow not finalize", and a slot list alone cannot
// answer it: whether those slots are enough depends on their weight against the group's threshold.
func TestSignatureEntriesCarryTheWeightBesideTheSlots(t *testing.T) {
	signed := map[uint64]types.Bitmap128{
		1: slotsSigned(0, 1),
		2: slotsSigned(0),
	}
	status := []user.SignatureStatusEntry{
		{Nonce: 1, SigWeight: 11, Total: 16, HasQuorum: true},
		{Nonce: 2, SigWeight: 4, Total: 16},
	}

	entries := signatureEntries(signed, status)

	if len(entries) != 2 || entries[0].Nonce != 1 || entries[1].Nonce != 2 {
		t.Fatalf("entries = %+v, want one per nonce in order", entries)
	}
	if entries[0].SigWeight != 11 || !entries[0].HasQuorum || entries[0].TotalSlots != 16 {
		t.Errorf("nonce 1 = %+v, want the quorum it reached", entries[0])
	}
	if entries[1].SigWeight != 4 || entries[1].HasQuorum {
		t.Errorf("nonce 2 = %+v, want the weight that fell short", entries[1])
	}
}

// A nonce the status pass did not reach still lists its slots; reporting nothing for it would hide the
// signatures this gateway does hold.
func TestANonceWithoutAStatusEntryStillListsItsSlots(t *testing.T) {
	entries := signatureEntries(map[uint64]types.Bitmap128{7: slotsSigned(3, 5)}, nil)

	if len(entries) != 1 || len(entries[0].Slots) != 2 {
		t.Fatalf("entries = %+v, want nonce 7 with both slots", entries)
	}
	if entries[0].SigWeight != 0 || entries[0].HasQuorum {
		t.Errorf("nonce 7 = %+v, want no weight claimed for it", entries[0])
	}
}
