package chain

import (
	"fmt"
	"strings"
	"time"

	"devshard/signing"
)

// encodeMsgCreateDevshardEscrow builds inference.inference.MsgCreateDevshardEscrow.
func encodeMsgCreateDevshardEscrow(creator string, amount uint64, modelID string) []byte {
	var out []byte
	out = appendBytesField(out, 1, []byte(creator))
	out = appendVarintField(out, 2, amount)
	out = appendBytesField(out, 3, []byte(modelID))
	return out
}

// encodeMsgSettleDevshardEscrow builds inference.inference.MsgSettleDevshardEscrow.
func encodeMsgSettleDevshardEscrow(settler string, input SettlementInput) []byte {
	var out []byte
	out = appendBytesField(out, 1, []byte(settler))
	out = appendVarintField(out, 2, input.EscrowID)
	out = appendBytesField(out, 3, input.StateRoot)
	out = appendVarintField(out, 4, input.Nonce)
	out = appendBytesField(out, 5, input.RestHash)
	for _, hostStat := range input.HostStats {
		out = appendBytesField(out, 6, encodeSettlementHostStats(hostStat))
	}
	for _, slotSig := range input.SlotSigs {
		out = appendBytesField(out, 7, encodeSlotSignature(slotSig))
	}
	out = appendVarintField(out, 8, input.Fees)
	out = appendBytesField(out, 9, []byte(input.Version))
	return out
}

// encodeSettlementHostStats builds one embedded HostStats entry (field 6 of MsgSettleDevshardEscrow).
func encodeSettlementHostStats(hostStat SettlementHostStat) []byte {
	var out []byte
	out = appendVarintField(out, 1, hostStat.SlotID)
	out = appendVarintField(out, 2, hostStat.Missed)
	out = appendVarintField(out, 3, hostStat.Invalid)
	out = appendVarintField(out, 4, hostStat.Cost)
	out = appendVarintField(out, 5, hostStat.RequiredValidations)
	out = appendVarintField(out, 6, hostStat.CompletedValidations)
	return out
}

// encodeSlotSignature builds one embedded slot signature entry (field 7 of MsgSettleDevshardEscrow).
func encodeSlotSignature(slotSig SettlementSlotSig) []byte {
	var out []byte
	out = appendVarintField(out, 1, slotSig.SlotID)
	out = appendBytesField(out, 2, slotSig.Signature)
	return out
}

// truncateSignature validates and drops the recovery byte from a secp256k1
// signature so only r||s (64 bytes) goes into the tx.
func truncateSignature(sig []byte) ([]byte, error) {
	if len(sig) < 64 {
		return nil, fmt.Errorf("invalid signature length %d", len(sig))
	}
	return sig[:64], nil
}

// buildCreateEscrowTx assembles and signs an unordered MsgCreateDevshardEscrow tx; ttl is caller-supplied so encoding stays deterministic.
func buildCreateEscrowTx(signer *signing.Secp256k1Signer, chainID string, accountNumber uint64, feeDenom string, feeAmount, gasLimit, amount uint64, modelID string, ttl time.Time) ([]byte, error) {
	if strings.TrimSpace(chainID) == "" {
		return nil, fmt.Errorf("chain id is required")
	}
	msg := encodeMsgCreateDevshardEscrow(signer.Address(), amount, modelID)
	bodyBytes := encodeUnorderedTxBody(encodeAny(createEscrowMsgTypeURL, msg), ttl)
	pubKeyAny := encodeAny(secp256k1PubKeyTypeURL, encodeSecp256k1PubKey(signer.CompressedPublicKeyBytes()))
	authInfoBytes := encodeAuthInfo(pubKeyAny, 0, feeDenom, feeAmount, gasLimit)
	signDoc := encodeSignDoc(bodyBytes, authInfoBytes, chainID, accountNumber)
	sig, err := signer.Sign(signDoc)
	if err != nil {
		return nil, fmt.Errorf("sign create-escrow tx: %w", err)
	}
	truncated, err := truncateSignature(sig)
	if err != nil {
		return nil, err
	}
	return encodeTxRaw(bodyBytes, authInfoBytes, truncated), nil
}

// buildSettleEscrowTx assembles and signs an unordered MsgSettleDevshardEscrow tx; settler may differ from the broadcasting signer.
func buildSettleEscrowTx(signer *signing.Secp256k1Signer, chainID string, accountNumber uint64, feeDenom string, feeAmount, gasLimit uint64, settler string, input SettlementInput, ttl time.Time) ([]byte, error) {
	if strings.TrimSpace(chainID) == "" {
		return nil, fmt.Errorf("chain id is required")
	}
	msg := encodeMsgSettleDevshardEscrow(settler, input)
	bodyBytes := encodeUnorderedTxBody(encodeAny(settleEscrowMsgTypeURL, msg), ttl)
	pubKeyAny := encodeAny(secp256k1PubKeyTypeURL, encodeSecp256k1PubKey(signer.CompressedPublicKeyBytes()))
	authInfoBytes := encodeAuthInfo(pubKeyAny, 0, feeDenom, feeAmount, gasLimit)
	signDoc := encodeSignDoc(bodyBytes, authInfoBytes, chainID, accountNumber)
	sig, err := signer.Sign(signDoc)
	if err != nil {
		return nil, fmt.Errorf("sign settle-escrow tx: %w", err)
	}
	truncated, err := truncateSignature(sig)
	if err != nil {
		return nil, err
	}
	return encodeTxRaw(bodyBytes, authInfoBytes, truncated), nil
}
