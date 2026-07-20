package chain

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Type URLs for the Any-wrapped messages and pubkey this package signs.
const (
	createEscrowMsgTypeURL = "/inference.inference.MsgCreateDevshardEscrow"
	settleEscrowMsgTypeURL = "/inference.inference.MsgSettleDevshardEscrow"
	secp256k1PubKeyTypeURL = "/cosmos.crypto.secp256k1.PubKey"
)

// appendVarintField appends a protobuf varint-wire-type field: tag then value.
func appendVarintField(dst []byte, field int, value uint64) []byte {
	dst = appendVarint(dst, uint64(field<<3))
	return appendVarint(dst, value)
}

// appendBytesField appends a protobuf length-delimited field: tag, length, bytes.
func appendBytesField(dst []byte, field int, value []byte) []byte {
	dst = appendVarint(dst, uint64(field<<3|2))
	dst = appendVarint(dst, uint64(len(value)))
	return append(dst, value...)
}

// appendVarint appends value as a base-128 little-endian protobuf varint.
func appendVarint(dst []byte, value uint64) []byte {
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

// encodeAny wraps value in a google.protobuf.Any: field 1 type_url, field 2 value.
func encodeAny(typeURL string, value []byte) []byte {
	var out []byte
	out = appendBytesField(out, 1, []byte(typeURL))
	out = appendBytesField(out, 2, value)
	return out
}

// encodeSecp256k1PubKey wraps a compressed key as cosmos.crypto.secp256k1.PubKey.
func encodeSecp256k1PubKey(compressed []byte) []byte {
	var out []byte
	out = appendBytesField(out, 1, compressed)
	return out
}

// encodeFee builds a cosmos tx Fee; the Coin amount is a decimal string field.
func encodeFee(denom string, amount uint64, gasLimit uint64) []byte {
	var coin []byte
	coin = appendBytesField(coin, 1, []byte(denom))
	coin = appendBytesField(coin, 2, []byte(strconv.FormatUint(amount, 10)))
	var out []byte
	out = appendBytesField(out, 1, coin)
	out = appendVarintField(out, 2, gasLimit)
	return out
}

// encodeSignerInfo builds a SignerInfo with a single SIGN_MODE_DIRECT mode.
func encodeSignerInfo(pubKeyAny []byte, sequence uint64) []byte {
	var single []byte
	single = appendVarintField(single, 1, 1) // SIGN_MODE_DIRECT
	var modeInfo []byte
	modeInfo = appendBytesField(modeInfo, 1, single)
	var out []byte
	out = appendBytesField(out, 1, pubKeyAny)
	out = appendBytesField(out, 2, modeInfo)
	out = appendVarintField(out, 3, sequence)
	return out
}

// encodeAuthInfo builds AuthInfo: field 1 SignerInfo, field 2 Fee.
func encodeAuthInfo(pubKeyAny []byte, sequence uint64, denom string, amount, gasLimit uint64) []byte {
	signerInfo := encodeSignerInfo(pubKeyAny, sequence)
	fee := encodeFee(denom, amount, gasLimit)
	var out []byte
	out = appendBytesField(out, 1, signerInfo)
	out = appendBytesField(out, 2, fee)
	return out
}

// encodeSignDoc builds the SignDoc that gets hashed and signed for broadcast.
func encodeSignDoc(bodyBytes, authInfoBytes []byte, chainID string, accountNumber uint64) []byte {
	var out []byte
	out = appendBytesField(out, 1, bodyBytes)
	out = appendBytesField(out, 2, authInfoBytes)
	out = appendBytesField(out, 3, []byte(chainID))
	out = appendVarintField(out, 4, accountNumber)
	return out
}

// encodeTxRaw builds the broadcastable TxRaw: body, auth info, signature.
func encodeTxRaw(bodyBytes, authInfoBytes, signature []byte) []byte {
	var out []byte
	out = appendBytesField(out, 1, bodyBytes)
	out = appendBytesField(out, 2, authInfoBytes)
	out = appendBytesField(out, 3, signature)
	return out
}

// encodeUnorderedTxBody builds a TxBody for the unordered-tx extension:
// field 1 msg, field 4 unordered=true, field 5 timeout_timestamp.
func encodeUnorderedTxBody(msgAny []byte, timeout time.Time) []byte {
	var out []byte
	out = appendBytesField(out, 1, msgAny)
	out = appendVarintField(out, 4, 1)
	out = appendBytesField(out, 5, encodeTimestamp(timeout))
	return out
}

// encodeTimestamp builds a google.protobuf.Timestamp; nanos is omitted when zero.
func encodeTimestamp(ts time.Time) []byte {
	var out []byte
	out = appendVarintField(out, 1, uint64(ts.Unix()))
	if nanos := ts.Nanosecond(); nanos != 0 {
		out = appendVarintField(out, 2, uint64(nanos))
	}
	return out
}

// txHashFromBytes returns the cosmos tx hash: upper-hex SHA-256 of the tx bytes.
func txHashFromBytes(txBytes []byte) string {
	sum := sha256.Sum256(txBytes)
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}
