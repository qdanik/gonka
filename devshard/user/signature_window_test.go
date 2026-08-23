package user

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The bytes of a signature are read only for the nonce being finalized, and every one of them is in
// storage. Keeping them all was the largest thing a live escrow held and the one nothing read back.
func TestOldSignatureBytesAreDroppedAndTheSlotsAreKept(t *testing.T) {
	session, _, _ := setupSession(t, 3, 1_000_000, 0)
	const latest = signatureWindow + 10

	session.mu.Lock()
	for nonce := uint64(1); nonce <= latest; nonce++ {
		session.recordSignatureLocked(nonce, 1, []byte("state-signature"))
	}
	heldBytes, heldSlots := len(session.signatures), len(session.signedSlots)
	_, oldestKept := session.signatures[latest-signatureWindow]
	_, evicted := session.signatures[1]
	signed := session.signedSlots[1]
	session.mu.Unlock()

	require.EqualValues(t, latest, heldSlots, "every nonce keeps the slots that signed it")
	require.False(t, evicted, "the first nonce is far outside the window and must have lost its bytes")
	require.True(t, oldestKept, "the window's own edge must still carry its bytes")
	require.LessOrEqual(t, heldBytes, signatureWindow+1, "no more than the window may hold bytes")
	require.Equal(t, []uint32{1}, signed.SetBits(), "the evicted nonce still reports which slot signed it")
}

// The status report is what an operator reads to see why an escrow will not finalize, so it must cover
// the whole history rather than the window the bytes happen to occupy.
func TestSignatureStatusCoversNoncesWhoseBytesAreGone(t *testing.T) {
	session, _, _ := setupSession(t, 3, 1_000_000, 0)
	const latest = signatureWindow + 10

	session.mu.Lock()
	for nonce := uint64(1); nonce <= latest; nonce++ {
		for slot := range uint32(3) {
			session.recordSignatureLocked(nonce, slot, []byte("state-signature"))
		}
	}
	session.mu.Unlock()

	entries, highestQuorum, hasAny := session.SignatureStatus()

	require.True(t, hasAny)
	require.Len(t, entries, int(latest), "every nonce ever signed must appear")
	require.EqualValues(t, latest, highestQuorum, "the whole group signed every nonce")
}

// A group is bounded by types.MaxGroupSize, not by the width of a word: a mask kept in a uint32 drops
// every slot past the thirty-second silently, and the quorum report then reads a signed nonce as unsigned.
func TestASlotBeyondAWordStillSigns(t *testing.T) {
	session, _, _ := setupSession(t, 3, 1_000_000, 0)

	session.mu.Lock()
	session.recordSignatureLocked(1, 40, []byte("state-signature"))
	signed := session.signedSlots[1]
	session.mu.Unlock()

	require.Equal(t, []uint32{40}, signed.SetBits(), "slot 40 is inside the group ceiling and must be recorded")
}
