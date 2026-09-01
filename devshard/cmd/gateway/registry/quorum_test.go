package registry

import (
	"context"
	"testing"

	"devshard/cmd/gateway/internal/logcapture"
	"devshard/state"
	"devshard/types"
)

func TestASettlementThatWouldNotVerifyIsReportedNotEnforced(t *testing.T) {
	t.Parallel()
	group := []types.SlotAssignment{
		{SlotID: 0, ValidatorAddress: "gonka1aaa"},
		{SlotID: 1, ValidatorAddress: "gonka1bbb"},
		{SlotID: 2, ValidatorAddress: "gonka1ccc"},
	}
	snapshot := types.EscrowState{
		EscrowID:                    "5",
		StateRootAndProtocolVersion: "v2",
		Group:                       group,
		HostStats:                   map[uint32]*types.HostStats{0: {Cost: 10}},
	}
	payload, err := state.BuildSettlement("5", snapshot, map[uint32][]byte{0: []byte("not-a-signature")}, 9)
	if err != nil {
		t.Fatalf("BuildSettlement: %v", err)
	}

	unverifiable := settlementUnverifiable(*payload, snapshot)
	if unverifiable == nil {
		t.Fatal("a settlement signed by nobody the group knows must be reported")
	}
}

func TestAnAbsentGroupIsNotReported(t *testing.T) {
	t.Parallel()
	snapshot := types.EscrowState{EscrowID: "5", StateRootAndProtocolVersion: "v2"}
	payload, err := state.BuildSettlement("5", snapshot, nil, 9)
	if err != nil {
		t.Fatalf("BuildSettlement: %v", err)
	}

	if unverifiable := settlementUnverifiable(*payload, snapshot); unverifiable != nil {
		t.Errorf("verdict = %v, want none for a snapshot with no group", unverifiable)
	}
}

func TestBuildSettlementStillAnswersWhenTheSignaturesDoNotVerify(t *testing.T) {
	t.Parallel()
	session := settleableSession(9)
	session.escrowState.Group = []types.SlotAssignment{
		{SlotID: 0, ValidatorAddress: "gonka1aaa"},
		{SlotID: 1, ValidatorAddress: "gonka1bbb"},
		{SlotID: 2, ValidatorAddress: "gonka1ccc"},
	}
	registry := New(Deps{
		ReadOnlySessions: newSessions(map[string]*fakeSession{"5": session}).open,
		Now:              fixedClock(),
	})

	input, err := registry.BuildSettlement(context.Background(), "5")

	if err != nil {
		t.Fatalf("BuildSettlement = %v, want nil: an unverifiable settlement is reported, not refused", err)
	}
	if input.Nonce != 9 {
		t.Errorf("nonce = %d, want the payload to be built anyway", input.Nonce)
	}
}

func TestBuildSettlementReportsASettlementThatWouldNotVerify(t *testing.T) {
	t.Parallel()
	session := settleableSession(9)
	session.escrowState.Group = []types.SlotAssignment{
		{SlotID: 0, ValidatorAddress: "gonka1aaa"},
		{SlotID: 1, ValidatorAddress: "gonka1bbb"},
		{SlotID: 2, ValidatorAddress: "gonka1ccc"},
	}
	logged := logcapture.Install(t)
	registry := New(Deps{
		ReadOnlySessions: newSessions(map[string]*fakeSession{"5": session}).open,
		Now:              fixedClock(),
	})

	if _, err := registry.BuildSettlement(context.Background(), "5"); err != nil {
		t.Fatalf("BuildSettlement = %v, want nil", err)
	}

	if _, found := logged.Find("settlement signatures did not verify"); !found {
		t.Errorf("the payload left without being checked: %v", logged.All())
	}
}
