package env

import "testing"

// Test flow:
//  1. Table-driven: each case sets the gateway and/or legacy environment spellings for allowing private addresses, expecting the guard to stay on unless explicitly opted out.
//  2. For each case, set both environment variables and call `AllowPrivateAddresses`.
//  3. Assert the result matches the case's expectation, including the gateway spelling winning over the legacy one and any non-boolean value keeping the guard on.
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
