package settlementwire

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"devshard/gatewayprotocol/settlement"
)

const fixedSettler = "gonka1settler0000000000000000000000000"

func fixedMessageFull() settlement.Message {
	return settlement.Message{
		EscrowID:  123,
		StateRoot: []byte("fixed-state-root-32-bytes-long!"),
		Nonce:     456,
		RestHash:  []byte("fixed-rest-hash-bytes"),
		HostStats: []settlement.HostStat{
			{SlotID: 1, Missed: 2, Invalid: 3, Cost: 400, RequiredValidations: 5, CompletedValidations: 6},
			{SlotID: 7, Missed: 8, Invalid: 9, Cost: 1000, RequiredValidations: 11, CompletedValidations: 12},
		},
		SlotSignatures: []settlement.SlotSignature{
			{SlotID: 1, Signature: []byte("fixed-signature-one")},
			{SlotID: 7, Signature: []byte("fixed-signature-two")},
		},
		Fees:    789,
		Version: "v1.2.3",
	}
}

func fixedMessageEmptyOptional() settlement.Message {
	return settlement.Message{
		EscrowID:  124,
		StateRoot: []byte("fixed-state-root-32-bytes-long!"),
		Nonce:     456,
		RestHash:  []byte("fixed-rest-hash-bytes"),
		Fees:      789,
		Version:   "v1.2.3",
	}
}

func loadGolden(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name+".b64"))
	if err != nil {
		t.Fatalf("reading golden %s: %v", name, err)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("decoding golden %s: %v", name, err)
	}
	return decoded
}

func mustEncode(t *testing.T, encode func() ([]byte, error)) []byte {
	t.Helper()
	encoded, err := encode()
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	return encoded
}

// Test flow:
//  1. Marshal a full settle message and one with empty host stats and signatures through `Marshal`.
//  2. Map one host stat and one slot signature through the package's mappings and marshal each chain entry alone.
//  3. For each case, load its golden fixture and assert the bytes match exactly.
func TestMarshalMatchesGoldens(t *testing.T) {
	settleFull := mustEncode(t, func() ([]byte, error) { return Marshal(fixedSettler, fixedMessageFull()) })
	settleEmptyOptional := mustEncode(t, func() ([]byte, error) { return Marshal(fixedSettler, fixedMessageEmptyOptional()) })
	hostStats := mustEncode(t, chainHostStats([]settlement.HostStat{
		{SlotID: 42, Missed: 3, Invalid: 1, Cost: 555, RequiredValidations: 10, CompletedValidations: 9},
	})[0].Marshal)
	slotSignature := mustEncode(t, chainSignatures([]settlement.SlotSignature{
		{SlotID: 42, Signature: []byte("standalone-fixed-signature")},
	})[0].Marshal)

	cases := []struct {
		name string
		got  []byte
	}{
		{"settle_msg_full", settleFull},
		{"settle_msg_empty_optional", settleEmptyOptional},
		{"settlement_host_stats", hostStats},
		{"slot_signature", slotSignature},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			want := loadGolden(t, testCase.name)
			if !bytes.Equal(testCase.got, want) {
				t.Fatalf("%s mismatch\n got:  %x\n want: %x", testCase.name, testCase.got, want)
			}
		})
	}
}
