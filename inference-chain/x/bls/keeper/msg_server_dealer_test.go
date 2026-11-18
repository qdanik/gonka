package keeper_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/math"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	sdk "github.com/cosmos/cosmos-sdk/types"
	keepertest "github.com/productscience/inference/testutil/keeper"
	"github.com/productscience/inference/x/bls/keeper"
	"github.com/productscience/inference/x/bls/types"
)

func setupMsgServerDealer(t testing.TB) (keeper.Keeper, types.MsgServer, context.Context) {
	k, ctx := keepertest.BlsKeeper(t)
	return k, keeper.NewMsgServerImpl(k), ctx
}

func TestSubmitDealerPart_Success(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Setup test data
	epochID := uint64(1)
	dealerAddr := "dealer1"
	participant1Addr := "participant1"
	participant2Addr := "participant2"

	// Create epoch BLS data with participants
	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               3,
		TSlotsDegree:              1,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100, // Future deadline
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(33),
				SlotStartIndex:     0,
				SlotEndIndex:       0,
			},
			{
				Address:            participant1Addr,
				Secp256K1PublicKey: []byte("pubkey2"),
				PercentageWeight:   math.LegacyNewDec(33),
				SlotStartIndex:     1,
				SlotEndIndex:       1,
			},
			{
				Address:            participant2Addr,
				Secp256K1PublicKey: []byte("pubkey3"),
				PercentageWeight:   math.LegacyNewDec(34),
				SlotStartIndex:     2,
				SlotEndIndex:       2,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	// Create test message
	polynomial := randomPolynomial(t, int(epochBLSData.TSlotsDegree))
	msg := &types.MsgSubmitDealerPart{
		Creator:     dealerAddr,
		EpochId:     epochID,
		Commitments: polynomialCommitments(polynomial),
		EncryptedSharesForParticipants: []types.EncryptedSharesForParticipant{
			{EncryptedShares: [][]byte{dummyEncryptedShare(1)}},
			{EncryptedShares: [][]byte{dummyEncryptedShare(2)}},
			{EncryptedShares: [][]byte{dummyEncryptedShare(3)}},
		},
		ProofOfPossession: dealerProofOfPossession(t, ctx.ChainID(), epochID, dealerAddr, polynomial[0]),
	}

	// Execute
	resp, err := ms.SubmitDealerPart(goCtx, msg)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Check that dealer part was stored
	updatedEpochBLSData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)

	// Dealer should be at index 0
	dealerPart := updatedEpochBLSData.DealerParts[0]
	require.NotNil(t, dealerPart)
	assert.Equal(t, dealerAddr, dealerPart.DealerAddress)
	assert.Equal(t, msg.Commitments, dealerPart.Commitments)
	assert.Len(t, dealerPart.ParticipantShares, 3)

	// Verify participant shares were stored correctly
	for i, expectedShare := range msg.EncryptedSharesForParticipants {
		assert.Equal(t, expectedShare.EncryptedShares, dealerPart.ParticipantShares[i].EncryptedShares)
	}
}

func TestSubmitDealerPart_EpochNotFound(t *testing.T) {
	_, ms, goCtx := setupMsgServerDealer(t)

	msg := &types.MsgSubmitDealerPart{
		Creator: "dealer1",
		EpochId: 999, // Non-existent epoch
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "epoch BLS data not found")
}

func TestSubmitDealerPart_WrongPhase(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	epochBLSData := types.EpochBLSData{
		EpochId:  epochID,
		DkgPhase: types.DKGPhase_DKG_PHASE_VERIFYING, // Wrong phase
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: "dealer1",
		EpochId: epochID,
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in dealing phase")
}

func TestSubmitDealerPart_DeadlinePassed(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() - 1, // Past deadline
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: "dealer1",
		EpochId: epochID,
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dealing phase deadline has passed")
}

func TestSubmitDealerPart_NotParticipant(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               100,
		TSlotsDegree:              33,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            "other_participant",
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(100),
				SlotStartIndex:     0,
				SlotEndIndex:       99,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: "not_a_participant",
		EpochId: epochID,
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a participant")
}

func TestSubmitDealerPart_AlreadySubmitted(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	dealerAddr := "dealer1"

	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               100,
		TSlotsDegree:              33,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(100),
				SlotStartIndex:     0,
				SlotEndIndex:       99,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: dealerAddr, Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}}, // Already submitted
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: dealerAddr,
		EpochId: epochID,
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already submitted dealer part")
}

func TestSubmitDealerPart_WrongSharesLength(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	dealerAddr := "dealer1"

	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               100,
		TSlotsDegree:              33,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(50),
				SlotStartIndex:     0,
				SlotEndIndex:       49,
			},
			{
				Address:            "participant2",
				Secp256K1PublicKey: []byte("pubkey2"),
				PercentageWeight:   math.LegacyNewDec(50),
				SlotStartIndex:     50,
				SlotEndIndex:       99,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: dealerAddr,
		EpochId: epochID,
		EncryptedSharesForParticipants: []types.EncryptedSharesForParticipant{
			// Only one share, but there are 2 participants
			{EncryptedShares: [][]byte{[]byte("share1")}},
		},
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected encrypted shares for 2 participants, got 1")
}

func TestSubmitDealerPart_InvalidEncryptedSharesShape(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	dealerAddr := "dealer1"

	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               2,
		TSlotsDegree:              1,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(50),
				SlotStartIndex:     0,
				SlotEndIndex:       0,
			},
			{
				Address:            "participant2",
				Secp256K1PublicKey: []byte("pubkey2"),
				PercentageWeight:   math.LegacyNewDec(50),
				SlotStartIndex:     1,
				SlotEndIndex:       1,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: dealerAddr,
		EpochId: epochID,
		Commitments: [][]byte{
			[]byte("commitment1"),
			[]byte("commitment2"),
		},
		EncryptedSharesForParticipants: []types.EncryptedSharesForParticipant{
			{EncryptedShares: [][]byte{[]byte("share1"), []byte("extra-share")}}, // expected 1, got 2
			{EncryptedShares: [][]byte{[]byte("share2")}},
		},
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid encrypted shares for participant index 0")
}

func TestSubmitDealerPart_UndersizedCiphertextRejected(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	dealerAddr := "dealer1"

	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               1,
		TSlotsDegree:              1,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(100),
				SlotStartIndex:     0,
				SlotEndIndex:       0,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	short := make([]byte, 98)
	short[0] = 0x04
	msg := &types.MsgSubmitDealerPart{
		Creator: dealerAddr,
		EpochId: epochID,
		Commitments: [][]byte{
			[]byte("commitment1"),
			[]byte("commitment2"),
		},
		EncryptedSharesForParticipants: []types.EncryptedSharesForParticipant{
			{EncryptedShares: [][]byte{short}},
		},
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "below ECIES minimum")
}

func TestSubmitDealerPart_WrongCommitmentsLength(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	dealerAddr := "dealer1"

	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               100,
		TSlotsDegree:              2, // Expect exactly 3 commitments
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(50),
				SlotStartIndex:     0,
				SlotEndIndex:       49,
			},
			{
				Address:            "participant2",
				Secp256K1PublicKey: []byte("pubkey2"),
				PercentageWeight:   math.LegacyNewDec(50),
				SlotStartIndex:     50,
				SlotEndIndex:       99,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	msg := &types.MsgSubmitDealerPart{
		Creator: dealerAddr,
		EpochId: epochID,
		Commitments: [][]byte{
			[]byte("commitment1"),
			[]byte("commitment2"), // Too few; expected 3
		},
		EncryptedSharesForParticipants: []types.EncryptedSharesForParticipant{
			{EncryptedShares: [][]byte{[]byte("share1")}},
			{EncryptedShares: [][]byte{[]byte("share2")}},
		},
	}

	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 3 commitments")
}

func TestSubmitDealerPart_EventEmission(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(1)
	dealerAddr := "dealer1"

	epochBLSData := types.EpochBLSData{
		EpochId:                   epochID,
		ITotalSlots:               1,
		TSlotsDegree:              1,
		DkgPhase:                  types.DKGPhase_DKG_PHASE_DEALING,
		DealingPhaseDeadlineBlock: ctx.BlockHeight() + 100,
		Participants: []types.BLSParticipantInfo{
			{
				Address:            dealerAddr,
				Secp256K1PublicKey: []byte("pubkey1"),
				PercentageWeight:   math.LegacyNewDec(100),
				SlotStartIndex:     0,
				SlotEndIndex:       0,
			},
		},
		DealerParts: []*types.DealerPartStorage{
			{DealerAddress: "", Commitments: [][]byte{}, ParticipantShares: []*types.EncryptedSharesForParticipant{}},
		},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	polynomial := randomPolynomial(t, int(epochBLSData.TSlotsDegree))
	msg := &types.MsgSubmitDealerPart{
		Creator:     dealerAddr,
		EpochId:     epochID,
		Commitments: polynomialCommitments(polynomial),
		EncryptedSharesForParticipants: []types.EncryptedSharesForParticipant{
			{EncryptedShares: [][]byte{dummyEncryptedShare(1)}},
		},
		ProofOfPossession: dealerProofOfPossession(t, ctx.ChainID(), epochID, dealerAddr, polynomial[0]),
	}

	// Execute
	_, err := ms.SubmitDealerPart(goCtx, msg)
	require.NoError(t, err)

	// Check that event was emitted
	events := ctx.EventManager().Events()
	var dealerSubmittedEvent sdk.Event
	found := false
	for _, event := range events {
		if event.Type == "inference.bls.EventDealerPartSubmitted" {
			dealerSubmittedEvent = event
			found = true
			break
		}
	}

	require.True(t, found, "EventDealerPartSubmitted should be emitted")

	// Verify event attributes
	epochAttr := false
	dealerAttr := false
	for _, attr := range dealerSubmittedEvent.Attributes {
		if attr.Key == "epoch_id" {
			assert.Equal(t, "\"1\"", attr.Value)
			epochAttr = true
		}
		if attr.Key == "dealer_address" {
			assert.Equal(t, "\""+dealerAddr+"\"", attr.Value)
			dealerAttr = true
		}
	}
	assert.True(t, epochAttr, "Event should contain epoch_id")
	assert.True(t, dealerAttr, "Event should contain dealer_address")
}

// Test flow:
// 1. Open a dealing round with three participants and let the two honest dealers submit parts with valid proofs of possession.
// 2. Build the last dealer's first commitment as s*G2 minus the honest first commitments, so the group key would be s*G2.
// 3. Submit that part with no proof, with a proof signed by s, and with an honest dealer's proof; expect each to be rejected.
// 4. Check that no part was stored for the last dealer.
func TestSubmitDealerPart_RogueFirstCommitmentRejected(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx).WithChainID("gonka-test")

	epochID := uint64(7)
	epoch := dealingEpoch(epochID, []string{"rogue", "honest1", "honest2"}, 2, ctx.BlockHeight()+100)
	require.NoError(t, k.SetEpochBLSData(ctx, epoch))
	degree := int(epoch.TSlotsDegree)

	var honestFirstCommitments bls12381.G2Jac
	var honestProof []byte
	for dealerIndex := 1; dealerIndex <= 2; dealerIndex++ {
		polynomial := randomPolynomial(t, degree)
		commitments := polynomialCommitments(polynomial)
		honestProof = dealerProofOfPossession(t, ctx.ChainID(), epochID, epoch.Participants[dealerIndex].Address, polynomial[0])
		_, err := ms.SubmitDealerPart(ctx, dealerPartMsg(epoch, dealerIndex, commitments, honestProof))
		require.NoError(t, err)

		var firstCommitment bls12381.G2Affine
		_, err = firstCommitment.SetBytes(commitments[0])
		require.NoError(t, err)
		honestFirstCommitments.AddMixed(&firstCommitment)
	}

	var groupSecret fr.Element
	_, err := groupSecret.SetRandom()
	require.NoError(t, err)
	var rogueFirstCommitment bls12381.G2Jac
	rogueFirstCommitment.ScalarMultiplicationBase(groupSecret.BigInt(new(big.Int)))
	rogueFirstCommitment.SubAssign(&honestFirstCommitments)
	var rogueFirstCommitmentAffine bls12381.G2Affine
	rogueFirstCommitmentAffine.FromJacobian(&rogueFirstCommitment)
	rogueFirstCommitmentBytes := rogueFirstCommitmentAffine.Bytes()

	rogueCommitments := polynomialCommitments(randomPolynomial(t, degree))
	rogueCommitments[0] = rogueFirstCommitmentBytes[:]

	proofs := map[string][]byte{
		"no proof":              nil,
		"proof signed by s":     dealerProofOfPossession(t, ctx.ChainID(), epochID, "rogue", groupSecret),
		"honest dealer's proof": honestProof,
	}
	for name, proof := range proofs {
		_, err := ms.SubmitDealerPart(ctx, dealerPartMsg(epoch, 0, rogueCommitments, proof))
		require.ErrorContains(t, err, "proof of possession", name)
	}

	stored, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Empty(t, stored.DealerParts[0].DealerAddress)
}

// Test flow:
// 1. Open a dealing round and build one honest dealer part.
// 2. Sign the proof of possession for another dealer address, another epoch id and another chain id; expect each part to be rejected.
// 3. Sign the proof for this dealer, epoch and chain; expect the part to be stored.
func TestSubmitDealerPart_ProofOfPossessionBoundToDealerEpochAndChain(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx).WithChainID("gonka-test")

	epochID := uint64(7)
	epoch := dealingEpoch(epochID, []string{"dealer1", "dealer2", "dealer3"}, 2, ctx.BlockHeight()+100)
	require.NoError(t, k.SetEpochBLSData(ctx, epoch))

	polynomial := randomPolynomial(t, int(epoch.TSlotsDegree))
	commitments := polynomialCommitments(polynomial)

	wrongBindings := map[string][]byte{
		"another dealer": dealerProofOfPossession(t, ctx.ChainID(), epochID, "dealer2", polynomial[0]),
		"another epoch":  dealerProofOfPossession(t, ctx.ChainID(), epochID+1, "dealer1", polynomial[0]),
		"another chain":  dealerProofOfPossession(t, "gonka-other", epochID, "dealer1", polynomial[0]),
	}
	for name, proof := range wrongBindings {
		_, err := ms.SubmitDealerPart(ctx, dealerPartMsg(epoch, 0, commitments, proof))
		require.ErrorContains(t, err, "proof of possession", name)
	}

	proof := dealerProofOfPossession(t, ctx.ChainID(), epochID, "dealer1", polynomial[0])
	_, err := ms.SubmitDealerPart(ctx, dealerPartMsg(epoch, 0, commitments, proof))
	require.NoError(t, err)

	stored, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Equal(t, "dealer1", stored.DealerParts[0].DealerAddress)
	require.Equal(t, commitments, stored.DealerParts[0].Commitments)
}

// Test flow:
// 1. Open a dealing round with three participants.
// 2. Build a dealer polynomial with a root at one slot of another participant, with a valid proof of possession.
// 3. Submit the part; expect it to be rejected and not stored.
func TestSubmitDealerPart_ZeroShareAtSlotRejected(t *testing.T) {
	k, ms, goCtx := setupMsgServerDealer(t)
	ctx := sdk.UnwrapSDKContext(goCtx).WithChainID("gonka-test")

	epochID := uint64(7)
	epoch := dealingEpoch(epochID, []string{"dealer1", "dealer2", "dealer3"}, 2, ctx.BlockHeight()+100)
	require.NoError(t, k.SetEpochBLSData(ctx, epoch))

	victimSlot := epoch.Participants[2].SlotStartIndex
	polynomial := polynomialWithRootAt(t, int(epoch.TSlotsDegree), uint64(victimSlot+1))
	zeroShare := evaluatePolynomialAt(polynomial, uint64(victimSlot+1))
	require.True(t, zeroShare.IsZero())

	proof := dealerProofOfPossession(t, ctx.ChainID(), epochID, "dealer1", polynomial[0])
	_, err := ms.SubmitDealerPart(ctx, dealerPartMsg(epoch, 0, polynomialCommitments(polynomial), proof))
	require.ErrorContains(t, err, "identity")

	stored, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Empty(t, stored.DealerParts[0].DealerAddress)
}
