package env

import "testing"

// The dial-time SSRF guard is process-wide and must be set before the first host dial, so it is read
// apart from Load. Production leaves it unset; only a stand whose hosts are private addresses opts out.
func TestPrivateAddressesAreRefusedUnlessTheStandAsksForThem(t *testing.T) {
	testCases := []struct {
		name    string
		gateway string
		legacy  string
		want    bool
	}{
		{name: "unset keeps the guard on", want: false},
		{name: "false keeps the guard on", gateway: "false", want: false},
		{name: "true opts the process out", gateway: "true", want: true},
		{name: "the fleet spelling still answers", legacy: "true", want: true},
		{name: "the gateway spelling wins over the fleet one", gateway: "false", legacy: "true", want: false},
		{name: "a value that is not a boolean keeps the guard on", gateway: "maybe", want: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("GATEWAY_ALLOW_PRIVATE_ADDRESSES", testCase.gateway)
			t.Setenv("DEVSHARD_ALLOW_PRIVATE_ADDRESSES", testCase.legacy)

			if got := AllowPrivateAddresses(); got != testCase.want {
				t.Fatalf("AllowPrivateAddresses() = %v, want %v", got, testCase.want)
			}
		})
	}
}
