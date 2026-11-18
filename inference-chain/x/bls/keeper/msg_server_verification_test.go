package keeper_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fp"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	hashToCurve "github.com/consensys/gnark-crypto/ecc/bls12-381/hash_to_curve"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sdk "github.com/cosmos/cosmos-sdk/types"
	keepertest "github.com/productscience/inference/testutil/keeper"
	"github.com/productscience/inference/x/bls/keeper"
	"github.com/productscience/inference/x/bls/types"
)

func setupMsgServerVerification(t testing.TB) (keeper.Keeper, types.MsgServer, context.Context) {
	k, ctx := keepertest.BlsKeeper(t)
	return k, keeper.NewMsgServerImpl(k), ctx
}

func TestSubmitVerificationVector_Success(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase
	epochID := uint64(100)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	// Create verification message from first participant
	participant := epochBLSData.Participants[0]
	dealerValidity := []bool{false, false, false}

	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: dealerValidity,
	}

	// Submit verification vector
	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify epoch data was updated
	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)

	// Verify successful submission
	submission := storedData.VerificationSubmissions[0] // Alice is at index 0
	require.Len(t, submission.DealerValidity, 3)        // Should have 3 dealer validity entries
	require.Equal(t, []bool{false, false, false}, submission.DealerValidity)

	// Verify other participants haven't submitted yet (empty DealerValidity)
	for i := 1; i < len(storedData.VerificationSubmissions); i++ {
		require.Len(t, storedData.VerificationSubmissions[i].DealerValidity, 0)
	}
}

func TestSubmitVerificationVector_EpochNotFound(t *testing.T) {
	_, msgServer, goCtx := setupMsgServerVerification(t)

	// Try to submit for non-existent epoch
	msg := &types.MsgSubmitVerificationVector{
		Creator:        "participant1",
		EpochId:        999,
		DealerValidity: []bool{false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	// Verify error details
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
	require.Contains(t, st.Message(), "no DKG data found for epoch 999")
}

func TestSubmitVerificationVector_WrongPhase(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in DEALING phase
	epochID := uint64(101)
	epochBLSData := createTestEpochBLSData(epochID, 3)
	// Keep in DEALING phase
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	// Verify error details
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Contains(t, st.Message(), "expected VERIFYING")
}

func TestSubmitVerificationVector_DeadlinePassed(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase with deadline already passed
	epochID := uint64(102)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	// Set current block height past the verification deadline
	ctx = ctx.WithBlockHeight(epochBLSData.VerifyingPhaseDeadlineBlock + 1)
	goCtx = ctx

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	// Verify error details
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "verification deadline passed")
}

func TestSubmitVerificationVector_NotParticipant(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase
	epochID := uint64(103)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	// Try to submit from non-participant address
	msg := &types.MsgSubmitVerificationVector{
		Creator:        "not_a_participant",
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	// Verify error details
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
	require.Contains(t, st.Message(), "not_a_participant is not a participant")
}

func TestSubmitVerificationVector_AlreadySubmitted(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase
	epochID := uint64(104)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)

	// Mark first participant as having already submitted (index-based)
	participant := epochBLSData.Participants[0]
	epochBLSData.VerificationSubmissions[0] = &types.VerificationVectorSubmission{
		DealerValidity: []bool{false, false, false},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	// Try to submit again from same participant
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	// Verify error details
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.AlreadyExists, st.Code())
	require.Contains(t, st.Message(), "has already submitted verification vector")
}

func TestSubmitVerificationVector_WrongDealerValidityLength(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase with 3 participants
	epochID := uint64(105)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	// Provide wrong length dealer validity array (2 instead of 3)
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{true, false}, // Wrong length
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	// Verify error details
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Contains(t, st.Message(), "dealer_validity length 2 does not match participants count 3")
}

func TestSubmitVerificationVector_EventEmission(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase
	epochID := uint64(106)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	// Submit verification vector
	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify event was emitted
	events := ctx.EventManager().ABCIEvents()
	require.Greater(t, len(events), 0)

	// Find our event by type
	found := false
	for _, event := range events {
		if event.Type == "inference.bls.EventVerificationVectorSubmitted" {
			found = true
			break
		}
	}
	require.True(t, found, "EventVerificationVectorSubmitted event should have been emitted")
}

func TestSubmitVerificationVector_MultipleParticipants(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Create test epoch data in VERIFYING phase with 3 participants
	epochID := uint64(107)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	// Submit verification vectors from all participants
	for _, participant := range epochBLSData.Participants {
		dealerValidity := make([]bool, len(epochBLSData.Participants))

		msg := &types.MsgSubmitVerificationVector{
			Creator:        participant.Address,
			EpochId:        epochID,
			DealerValidity: dealerValidity,
		}

		resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
		require.NoError(t, err)
		require.NotNil(t, resp)
	}

	// Verify all submissions were stored
	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Len(t, storedData.VerificationSubmissions, 3)

	// Verify each submission is stored at the correct participant index
	for i := range epochBLSData.Participants {
		submission := storedData.VerificationSubmissions[i]
		require.Len(t, submission.DealerValidity, 3)

		expectedPattern := make([]bool, len(epochBLSData.Participants))
		require.Equal(t, expectedPattern, submission.DealerValidity)
	}
}

func TestSubmitVerificationVector_TrueDealerWithoutProof(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(108)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, true, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Contains(t, st.Message(), "dealer_validity_proofs count 0 does not match true non-self dealer count 1")
}

func TestSubmitVerificationVector_OneMissingNonSelfProofRejected(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(116)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, true, true},
		DealerValidityProofs: []types.DealerValidityProof{
			{
				DealerIndex:    1,
				ProofSignature: []byte{1},
			},
		},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Contains(t, st.Message(), "dealer_validity_proofs count 1 does not match true non-self dealer count 2")
}

func TestSubmitVerificationVector_SelfTrueWithoutProofAllowed(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(115)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{true, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)
	require.NotNil(t, resp)

	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Equal(t, msg.DealerValidity, storedData.VerificationSubmissions[0].DealerValidity)
}

func TestSubmitVerificationVector_TrueDealerWithValidProof(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(113)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)

	// Configure dealer 0 as a valid dealer with a constant polynomial commitment C0 = g2 * s.
	// With one commitment, per-slot dealer shares equal s for every slot.
	const dealerScalar uint64 = 7
	epochBLSData.DealerParts[0].DealerAddress = epochBLSData.Participants[0].Address
	epochBLSData.DealerParts[0].Commitments = [][]byte{g2CommitmentFromScalar(dealerScalar)}
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	slotCount := int(participant.SlotEndIndex-participant.SlotStartIndex) + 1
	proofHash := types.BuildDealerValidityProofHash(epochID, 0)
	proofSignature, err := constantShareProofSignature(proofHash, dealerScalar, slotCount)
	require.NoError(t, err)

	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{true, false, false},
		DealerValidityProofs: []types.DealerValidityProof{
			{
				DealerIndex:    0,
				ProofSignature: proofSignature,
			},
		},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)
	require.NotNil(t, resp)

	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Equal(t, msg.DealerValidity, storedData.VerificationSubmissions[0].DealerValidity)
	require.Empty(t, storedData.DealerComplaints)
}

func TestSubmitVerificationVector_ComplaintsPersisted(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(109)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	// Ensure dealer 1 has ciphertexts for participant 0 so complaint evidence is meaningful.
	epochBLSData.DealerParts[1].DealerAddress = epochBLSData.Participants[1].Address
	epochBLSData.DealerParts[1].Commitments = make([][]byte, int(epochBLSData.TSlotsDegree)+1)
	participant0Shares := dummyEncryptedShares(33)
	epochBLSData.DealerParts[1].ParticipantShares = []*types.EncryptedSharesForParticipant{
		{EncryptedShares: participant0Shares},
		{EncryptedShares: [][]byte{[]byte("c1")}},
		{EncryptedShares: [][]byte{[]byte("c2")}},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
		DealerComplaints: []types.VerificationDealerComplaint{
			{
				DealerIndex:             1,
				DisputedSlotIndex:       0,
				DisputedCiphertextIndex: 0,
			},
		},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)
	require.NotNil(t, resp)

	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Len(t, storedData.DealerComplaints, 1)
	require.Equal(t, uint32(1), storedData.DealerComplaints[0].DealerIndex)
	require.Equal(t, uint32(0), storedData.DealerComplaints[0].ComplainerIndex)
	require.Equal(t, uint32(0), storedData.DealerComplaints[0].DisputedSlotIndex)
	require.Equal(t, uint32(0), storedData.DealerComplaints[0].DisputedCiphertextIndex)
}

func TestSubmitVerificationVector_MissingComplaintForFalseDealerWithSharesRejected(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(110)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	epochBLSData.DealerParts[1].DealerAddress = epochBLSData.Participants[1].Address
	epochBLSData.DealerParts[1].Commitments = make([][]byte, int(epochBLSData.TSlotsDegree)+1)
	participant0Shares := dummyEncryptedShares(33)
	epochBLSData.DealerParts[1].ParticipantShares = []*types.EncryptedSharesForParticipant{
		{EncryptedShares: participant0Shares},
		{EncryptedShares: [][]byte{[]byte("c1")}},
		{EncryptedShares: [][]byte{[]byte("c2")}},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "missing complaint evidence")
}

func TestSubmitVerificationVector_MalformedSharesDoNotRequireComplaintEvidence(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(114)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	epochBLSData.DealerParts[1].DealerAddress = epochBLSData.Participants[1].Address
	epochBLSData.DealerParts[1].Commitments = make([][]byte, int(epochBLSData.TSlotsDegree)+1)
	// Participant 0 owns 33 slots in this test fixture; a single ciphertext is malformed shape.
	epochBLSData.DealerParts[1].ParticipantShares = []*types.EncryptedSharesForParticipant{
		{EncryptedShares: [][]byte{[]byte("malformed")}},
		{EncryptedShares: [][]byte{[]byte("c1")}},
		{EncryptedShares: [][]byte{[]byte("c2")}},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)
	require.NotNil(t, resp)

	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Equal(t, msg.DealerValidity, storedData.VerificationSubmissions[0].DealerValidity)
	require.Empty(t, storedData.DealerComplaints)
}

func TestSubmitVerificationVector_SelfComplaintRejected(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(117)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
		DealerComplaints: []types.VerificationDealerComplaint{
			{
				DealerIndex:             0,
				DisputedSlotIndex:       participant.SlotStartIndex,
				DisputedCiphertextIndex: 0,
			},
		},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Contains(t, st.Message(), "self complaint is not allowed")
}

func TestSubmitVerificationVector_InvalidComplaintSlotRejected(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(111)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	epochBLSData.DealerParts[1].DealerAddress = epochBLSData.Participants[1].Address
	epochBLSData.DealerParts[1].Commitments = make([][]byte, int(epochBLSData.TSlotsDegree)+1)
	participant0Shares := dummyEncryptedShares(33)
	epochBLSData.DealerParts[1].ParticipantShares = []*types.EncryptedSharesForParticipant{
		{EncryptedShares: participant0Shares},
		{EncryptedShares: [][]byte{[]byte("c1")}},
		{EncryptedShares: [][]byte{[]byte("c2")}},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
		DealerComplaints: []types.VerificationDealerComplaint{
			{
				DealerIndex:             1,
				DisputedSlotIndex:       33,
				DisputedCiphertextIndex: 0,
			},
		},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "disputed_slot_index")
}

func TestSubmitVerificationVector_InvalidComplaintCiphertextRejected(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(112)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 3)
	epochBLSData.DealerParts[1].DealerAddress = epochBLSData.Participants[1].Address
	epochBLSData.DealerParts[1].Commitments = make([][]byte, int(epochBLSData.TSlotsDegree)+1)
	participant0Shares := dummyEncryptedShares(33)
	epochBLSData.DealerParts[1].ParticipantShares = []*types.EncryptedSharesForParticipant{
		{EncryptedShares: participant0Shares},
		{EncryptedShares: [][]byte{[]byte("c1")}},
		{EncryptedShares: [][]byte{[]byte("c2")}},
	}
	k.SetEpochBLSData(ctx, epochBLSData)

	participant := epochBLSData.Participants[0]
	msg := &types.MsgSubmitVerificationVector{
		Creator:        participant.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, false, false},
		DealerComplaints: []types.VerificationDealerComplaint{
			{
				DealerIndex:             1,
				DisputedSlotIndex:       0,
				DisputedCiphertextIndex: 10,
			},
		},
	}

	resp, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "disputed_ciphertext_index")
}

func g2CommitmentFromScalar(scalar uint64) []byte {
	var g2Gen bls12381.G2Affine
	_, _, _, g2Gen = bls12381.Generators()

	var scalarBigInt big.Int
	scalarBigInt.SetUint64(scalar)

	var commitment bls12381.G2Affine
	commitment.ScalarMultiplication(&g2Gen, &scalarBigInt)
	commitmentBytes := commitment.Bytes()
	return commitmentBytes[:]
}

func constantShareProofSignature(proofHash []byte, scalar uint64, slotCount int) ([]byte, error) {
	if slotCount <= 0 {
		return nil, fmt.Errorf("slotCount must be > 0")
	}

	messageG1, err := mapProofHashToG1(proofHash)
	if err != nil {
		return nil, err
	}

	var scalarBigInt big.Int
	scalarBigInt.SetUint64(scalar)

	var slotSignature bls12381.G1Affine
	slotSignature.ScalarMultiplication(&messageG1, &scalarBigInt)
	slotSignatureBytes := slotSignature.Bytes()

	proofSignature := make([]byte, 0, slotCount*len(slotSignatureBytes))
	for i := 0; i < slotCount; i++ {
		proofSignature = append(proofSignature, slotSignatureBytes[:]...)
	}
	return proofSignature, nil
}

func mapProofHashToG1(hash []byte) (bls12381.G1Affine, error) {
	var out bls12381.G1Affine
	if len(hash) != 32 {
		return out, fmt.Errorf("message hash must be 32 bytes, got %d", len(hash))
	}

	var be [48]byte
	copy(be[48-32:], hash)

	var u fp.Element
	u.SetBytes(be[:])

	p := bls12381.MapToCurve1(&u)
	hashToCurve.G1Isogeny(&p.X, &p.Y)
	out.ClearCofactor(&p)
	return out, nil
}

// Helper function to create test epoch BLS data in VERIFYING phase
func createTestEpochBLSDataInVerifyingPhase(epochID uint64, numParticipants int) types.EpochBLSData {
	epochData := createTestEpochBLSData(epochID, numParticipants)

	// Set phase to VERIFYING
	epochData.DkgPhase = types.DKGPhase_DKG_PHASE_VERIFYING

	// Set verification deadline in the future
	epochData.VerifyingPhaseDeadlineBlock = 200

	return epochData
}

// Test flow:
// 1. Open a verifying round where dealer 1 dealt the verifier a zero share at one slot, and dealers 2 and 3 dealt full polynomials.
// 2. The verifier votes dealers 1 and 2 valid, with a per-slot proof built from its shares, and votes dealer 3 invalid with a complaint.
// 3. Submit the message; expect it to be accepted although the proof for dealer 1 cannot verify.
// 4. Check that the vote for dealer 1 is stored as invalid, the vote for dealer 2 stands, and the complaint against dealer 3 is stored.
func TestSubmitVerificationVector_FailedProofKeepsComplaintAndOtherVotes(t *testing.T) {
	k, msgServer, goCtx := setupMsgServerVerification(t)
	ctx := sdk.UnwrapSDKContext(goCtx)

	epochID := uint64(120)
	epochBLSData := createTestEpochBLSDataInVerifyingPhase(epochID, 4)
	verifier := epochBLSData.Participants[0]
	verifierSlots := int(verifier.SlotEndIndex-verifier.SlotStartIndex) + 1
	degree := int(epochBLSData.TSlotsDegree)

	polynomials := map[int][]fr.Element{
		1: polynomialWithRootAt(t, degree, uint64(verifier.SlotStartIndex+1)),
		2: randomPolynomial(t, degree),
		3: randomPolynomial(t, degree),
	}
	for dealerIndex, polynomial := range polynomials {
		epochBLSData.DealerParts[dealerIndex] = &types.DealerPartStorage{
			DealerAddress: epochBLSData.Participants[dealerIndex].Address,
			Commitments:   polynomialCommitments(polynomial),
			ParticipantShares: []*types.EncryptedSharesForParticipant{
				{EncryptedShares: dummyEncryptedShares(verifierSlots)},
			},
		}
	}
	require.NoError(t, k.SetEpochBLSData(ctx, epochBLSData))

	msg := &types.MsgSubmitVerificationVector{
		Creator:        verifier.Address,
		EpochId:        epochID,
		DealerValidity: []bool{false, true, true, false},
		DealerComplaints: []types.VerificationDealerComplaint{
			{DealerIndex: 3, DisputedSlotIndex: verifier.SlotStartIndex, DisputedCiphertextIndex: 0},
		},
		DealerValidityProofs: []types.DealerValidityProof{
			{DealerIndex: 1, ProofSignature: dealerValidityProofSignature(t, epochID, 1, polynomials[1], verifier)},
			{DealerIndex: 2, ProofSignature: dealerValidityProofSignature(t, epochID, 2, polynomials[2], verifier)},
		},
	}

	_, err := msgServer.SubmitVerificationVector(goCtx, msg)
	require.NoError(t, err)

	storedData, err := k.GetEpochBLSData(ctx, epochID)
	require.NoError(t, err)
	require.Equal(t, []bool{false, false, true, false}, storedData.VerificationSubmissions[0].DealerValidity)
	require.Len(t, storedData.DealerComplaints, 1)
	require.Equal(t, uint32(3), storedData.DealerComplaints[0].DealerIndex)
	require.Equal(t, uint32(0), storedData.DealerComplaints[0].ComplainerIndex)
}
