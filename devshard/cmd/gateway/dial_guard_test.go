package main

import (
	"testing"

	"common/httpguard"

	"devshard/cmd/gateway/internal/logcapture"
)

const dialGuardOffMessage = "dials to private addresses are allowed: the SSRF guard is off"

// The guard is process-wide and a host URL is participant-controlled, so the gateway arms it before
// anything can dial, and a stand that opts out is told about it in the log.
func TestTheDialGuardIsArmedBeforeAnythingDials(t *testing.T) {
	testCases := []struct {
		name      string
		raw       string
		wantOpen  bool
		wantWarns bool
	}{
		{name: "unset keeps the guard on and says nothing", raw: "", wantOpen: false, wantWarns: false},
		{name: "false keeps the guard on and says nothing", raw: "false", wantOpen: false, wantWarns: false},
		{name: "an opt-out is armed and announced", raw: "true", wantOpen: true, wantWarns: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Cleanup(func() { httpguard.SetAllowPrivate(false) })
			httpguard.SetAllowPrivate(!testCase.wantOpen)
			t.Setenv("GATEWAY_ALLOW_PRIVATE_ADDRESSES", testCase.raw)
			logs := logcapture.Install(t)

			applyDialGuard()

			if got := httpguard.AllowPrivate(); got != testCase.wantOpen {
				t.Fatalf("httpguard.AllowPrivate() = %v, want %v", got, testCase.wantOpen)
			}
			if _, warned := logs.Find(dialGuardOffMessage); warned != testCase.wantWarns {
				t.Fatalf("the log warned = %v, want %v", warned, testCase.wantWarns)
			}
		})
	}
}
