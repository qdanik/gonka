package keeper_test

import (
	"math/big"
	"testing"

	"cosmossdk.io/math"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/stretchr/testify/require"

	"github.com/productscience/inference/x/bls/types"
)

func randomPolynomial(t *testing.T, degree int) []fr.Element {
	t.Helper()
	polynomial := make([]fr.Element, degree+1)
	for i := range polynomial {
		_, err := polynomial[i].SetRandom()
		require.NoError(t, err)
	}
	return polynomial
}

func polynomialWithRootAt(t *testing.T, degree int, root uint64) []fr.Element {
	t.Helper()
	quotient := randomPolynomial(t, degree-1)
	var rootFr fr.Element
	rootFr.SetUint64(root)

	polynomial := make([]fr.Element, degree+1)
	for i := range quotient {
		var scaled fr.Element
		scaled.Mul(&quotient[i], &rootFr)
		polynomial[i].Sub(&polynomial[i], &scaled)
		polynomial[i+1].Add(&polynomial[i+1], &quotient[i])
	}
	return polynomial
}

func evaluatePolynomialAt(polynomial []fr.Element, x uint64) fr.Element {
	var xFr, result fr.Element
	xFr.SetUint64(x)
	for i := len(polynomial) - 1; i >= 0; i-- {
		result.Mul(&result, &xFr)
		result.Add(&result, &polynomial[i])
	}
	return result
}

func polynomialCommitments(polynomial []fr.Element) [][]byte {
	_, _, _, g2Gen := bls12381.Generators()
	commitments := make([][]byte, len(polynomial))
	for i := range polynomial {
		var commitment bls12381.G2Affine
		commitment.ScalarMultiplication(&g2Gen, polynomial[i].BigInt(new(big.Int)))
		commitmentBytes := commitment.Bytes()
		commitments[i] = commitmentBytes[:]
	}
	return commitments
}

func signWithScalar(t *testing.T, messageHash []byte, secret fr.Element) []byte {
	t.Helper()
	messageG1, err := mapProofHashToG1(messageHash)
	require.NoError(t, err)

	var signature bls12381.G1Affine
	signature.ScalarMultiplication(&messageG1, secret.BigInt(new(big.Int)))
	signatureBytes := signature.Bytes()
	return signatureBytes[:]
}

func dealerProofOfPossession(t *testing.T, chainID string, epochID uint64, dealerAddress string, secret fr.Element) []byte {
	t.Helper()
	return signWithScalar(t, types.BuildDealerProofOfPossessionHash(chainID, epochID, dealerAddress), secret)
}

func dealerValidityProofSignature(t *testing.T, epochID uint64, dealerIndex uint32, polynomial []fr.Element, participant types.BLSParticipantInfo) []byte {
	t.Helper()
	proofHash := types.BuildDealerValidityProofHash(epochID, dealerIndex)
	var proofSignature []byte
	for slot := participant.SlotStartIndex; slot <= participant.SlotEndIndex; slot++ {
		proofSignature = append(proofSignature, signWithScalar(t, proofHash, evaluatePolynomialAt(polynomial, uint64(slot+1)))...)
	}
	return proofSignature
}

func dealingEpoch(epochID uint64, addresses []string, slotsPerParticipant uint32, deadlineBlock int64) types.EpochBLSData {
	participants := make([]types.BLSParticipantInfo, len(addresses))
	dealerParts := make([]*types.DealerPartStorage, len(addresses))
	for i, address := range addresses {
		participants[i] = types.BLSParticipantInfo{
			Address:            address,
			Secp256K1PublicKey: []byte("pubkey" + address),
			PercentageWeight:   math.LegacyNewDec(1),
			SlotStartIndex:     uint32(i) * slotsPerParticipant,
			SlotEndIndex:       uint32(i+1)*slotsPerParticipant - 1,
		}
		dealerParts[i] = &types.DealerPartStorage{}
	}

	totalSlots := uint32(len(addresses)) * slotsPerParticipant
	return types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               totalSlots,
		TSlotsDegree:              totalSlots / 2,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: deadlineBlock,
		Participants:              participants,
		DealerParts:               dealerParts,
	}
}

func dealerPartMsg(epoch types.EpochBLSData, dealerIndex int, commitments [][]byte, proofOfPossession []byte) *types.MsgSubmitDealerPart {
	encryptedShares := make([]types.EncryptedSharesForParticipant, len(epoch.Participants))
	for i, participant := range epoch.Participants {
		encryptedShares[i] = types.EncryptedSharesForParticipant{
			EncryptedShares: dummyEncryptedShares(int(participant.SlotEndIndex-participant.SlotStartIndex) + 1),
		}
	}
	return &types.MsgSubmitDealerPart{
		Creator:                        epoch.Participants[dealerIndex].Address,
		EpochId:                        epoch.EpochId,
		Commitments:                    commitments,
		EncryptedSharesForParticipants: encryptedShares,
		ProofOfPossession:              proofOfPossession,
	}
}
