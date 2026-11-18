package types

import (
	"crypto/sha256"
	"encoding/binary"

	"golang.org/x/crypto/sha3"
)

const DealerValidityProofDomain = "gonka:share-proof-v1"

// DealerProofOfPossessionDomain separates dealer proofs of possession from every other message signed with DKG secrets.
const DealerProofOfPossessionDomain = "gonka:dealer-pop-v1"

func BuildDealerValidityProofHash(epochID uint64, dealerIndex uint32) []byte {
	domain := []byte(DealerValidityProofDomain)
	payload := make([]byte, len(domain)+8+4)
	copy(payload, domain)
	binary.BigEndian.PutUint64(payload[len(domain):len(domain)+8], epochID)
	binary.BigEndian.PutUint32(payload[len(domain)+8:], dealerIndex)

	hash := sha3.NewLegacyKeccak256()
	hash.Write(payload)
	return hash.Sum(nil)
}

// BuildDealerProofOfPossessionHash returns the message a dealer signs with the secret behind its first commitment.
// The dealer address is the only variable-length field and comes last, so the encoding is unambiguous.
func BuildDealerProofOfPossessionHash(chainID string, epochID uint64, dealerAddress string) []byte {
	chainIDHash := sha256.Sum256([]byte(chainID))
	var epochIDBytes [8]byte
	binary.BigEndian.PutUint64(epochIDBytes[:], epochID)

	hash := sha3.NewLegacyKeccak256()
	hash.Write([]byte(DealerProofOfPossessionDomain))
	hash.Write(chainIDHash[:])
	hash.Write(epochIDBytes[:])
	hash.Write([]byte(dealerAddress))
	return hash.Sum(nil)
}
