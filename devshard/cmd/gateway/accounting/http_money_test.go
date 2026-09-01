package accounting

import (
	"encoding/json"
	"net/http"
	"testing"

	"devshard/types"
)

func TestParticipantsEndpointReturnsCostPerParticipantAndModel(t *testing.T) {
	t.Parallel()
	book := NewBook(nil)
	for _, escrow := range []struct {
		id      string
		model   string
		address string
		cost    uint64
		nonce   uint64
		record  types.InferenceRecord
	}{
		{
			id: "5", model: "Qwen/Test", address: "gonka1aaa", cost: 100, nonce: 2,
			record: types.InferenceRecord{ReservedCost: 900, ActualCost: 400, InputTokens: 64, OutputTokens: 16, Status: types.StatusFinished},
		},
		{
			id: "6", model: "Kimi/Test", address: "gonka1aaa", cost: 70, nonce: 4,
			record: types.InferenceRecord{ReservedCost: 500, ActualCost: 500, InputTokens: 32, OutputTokens: 8, Status: types.StatusFinished},
		},
	} {
		if err := book.OpenEscrow(EscrowMetadata{
			EscrowID:      escrow.id,
			Model:         escrow.model,
			CreationEpoch: testEpoch,
			Slots:         []types.SlotAssignment{{SlotID: 0, ValidatorAddress: escrow.address}},
		}); err != nil {
			t.Fatalf("OpenEscrow %s: %v", escrow.id, err)
		}
		if err := book.ObserveHostStats(escrow.id, 0, types.HostStats{Cost: escrow.cost}); err != nil {
			t.Fatalf("ObserveHostStats %s: %v", escrow.id, err)
		}
		if err := book.ObserveNonceCost(escrow.id, escrow.nonce, escrow.record); err != nil {
			t.Fatalf("ObserveNonceCost %s: %v", escrow.id, err)
		}
	}

	recorder := serve(t, book, "/api/v1/epochs/current/participants")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body)
	}
	var body struct {
		Participants []struct {
			Participant  string `json:"participant"`
			Model        string `json:"model"`
			ChainCost    uint64 `json:"chain_cost"`
			ReservedCost uint64 `json:"reserved_cost"`
			ActualCost   uint64 `json:"actual_cost"`
			RefundedCost uint64 `json:"refunded_cost"`
			InputTokens  uint64 `json:"input_tokens"`
			OutputTokens uint64 `json:"output_tokens"`
		} `json:"participants"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding the reply: %v", err)
	}

	byModel := map[string]int{}
	for index, row := range body.Participants {
		byModel[row.Participant+"|"+row.Model] = index
	}
	qwen, served := byModel["gonka1aaa|Qwen/Test"]
	if !served {
		t.Fatalf("no row for gonka1aaa on Qwen/Test; got %+v", body.Participants)
	}
	kimi, alsoServed := byModel["gonka1aaa|Kimi/Test"]
	if !alsoServed {
		t.Fatalf("one host on two models must be two rows; got %+v", body.Participants)
	}

	for _, want := range []struct {
		name  string
		got   uint64
		value uint64
	}{
		{"qwen chain cost", body.Participants[qwen].ChainCost, 100},
		{"qwen reserved", body.Participants[qwen].ReservedCost, 900},
		{"qwen actual", body.Participants[qwen].ActualCost, 400},
		{"qwen refunded", body.Participants[qwen].RefundedCost, 500},
		{"qwen input tokens", body.Participants[qwen].InputTokens, 64},
		{"qwen output tokens", body.Participants[qwen].OutputTokens, 16},
		{"kimi chain cost", body.Participants[kimi].ChainCost, 70},
		{"kimi actual", body.Participants[kimi].ActualCost, 500},
		{"kimi refunded", body.Participants[kimi].RefundedCost, 0},
	} {
		if want.got != want.value {
			t.Errorf("%s = %d, want %d", want.name, want.got, want.value)
		}
	}
}
