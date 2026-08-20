package accounting

import "testing"

func TestCapabilityAttachesOnlyWhenSomethingIsKnown(t *testing.T) {
	records := []ParticipantRecord{{Participant: "refused"}, {Participant: "clean"}}

	attachCapabilities(records, func(participant, _ string) HostCapability {
		if participant == "refused" {
			return HostCapability{ProtocolVersionUnsupported: true, VersionRefusals: 3, ContextLimit: 8192}
		}
		return HostCapability{}
	})

	if records[0].Capability == nil {
		t.Fatal("a host that refused carries no capability block")
	}
	if !records[0].Capability.ProtocolVersionUnsupported || records[0].Capability.VersionRefusals != 3 {
		t.Errorf("capability = %+v, want the verdict and its count", records[0].Capability)
	}
	if records[1].Capability != nil {
		t.Errorf("a host with nothing wrong carries %+v, want nothing", records[1].Capability)
	}
}

func TestCapabilityNoLookupLeavesRecordsUntouched(t *testing.T) {
	records := []ParticipantRecord{{Participant: "any"}}

	attachCapabilities(records, nil)

	if records[0].Capability != nil {
		t.Error("a gateway with no perf tracker still attached a capability block")
	}
}

func TestCapabilityStillReportsAHostWhoseVerdictExpired(t *testing.T) {
	records := []ParticipantRecord{{Participant: "recovered"}}

	attachCapabilities(records, func(string, string) HostCapability {
		return HostCapability{VersionRefusals: 2}
	})

	if records[0].Capability == nil {
		t.Fatal("the refusal total vanished with the verdict that expired")
	}
	if records[0].Capability.ProtocolVersionUnsupported {
		t.Error("an expired verdict is still reported as blocking")
	}
}
