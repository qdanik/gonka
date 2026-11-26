package accounting

import "testing"

// Test flow:
//  1. Attach shares to three records with a lookup that holds a row for one participant and model only.
//  2. Assert the held record carries the row and the other two carry none.
//  3. Attach with a nil lookup and assert nothing changes.
func TestSharesAreAttachedOnlyToHostsTheLedgerHolds(t *testing.T) {
	records := []ParticipantRecord{
		{Participant: "host-a", Model: "model-a"},
		{Participant: "host-a", Model: "model-b"},
		{Participant: "host-b", Model: "model-a"},
	}
	row := HostShare{HostLoadMode: "balance", Weight: 9, WeightShare: 0.5, SlotShare: 0.4, TargetShare: 0.45, OfferedShare: 0.6, Surplus: 3, ServeRatio: 0.7, CapacityFactor: 0.7,
		Assigned: 10, Offered: 9, Served: 6, Failed: 1, Passed: 2, Excused: 1, ProbesAnswered: 1, ProbesMissed: 1, VotesOwed: 1, PassUnvoted: 1}

	attachShares(records, nil)
	attachShares(records, func(participant, model string) (HostShare, bool) {
		return row, participant == "host-a" && model == "model-a"
	})

	if records[0].Share == nil || *records[0].Share != row {
		t.Errorf("held record share = %+v, want %+v", records[0].Share, row)
	}
	if records[1].Share != nil || records[2].Share != nil {
		t.Errorf("unheld records carry shares: %+v %+v", records[1].Share, records[2].Share)
	}
}
