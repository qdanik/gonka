package main

import (
	"testing"

	"devshard/cmd/gateway/store"
)

// The prefix an escrow was created under outranks the one this gateway is currently serving.
func TestEscrowRoutePrefixPrefersThePinOverTheRunningGateway(t *testing.T) {
	pinned := store.DevshardRecord{EscrowID: "58128", RoutePrefix: "/devshard/v3"}
	if got := escrowRoutePrefix(pinned, "/devshard/v4"); got != "/devshard/v3" {
		t.Fatalf("escrowRoutePrefix(pinned) = %q, want %q", got, "/devshard/v3")
	}

	unpinned := store.DevshardRecord{EscrowID: "58128"}
	if got := escrowRoutePrefix(unpinned, "/devshard/v4"); got != "/devshard/v4" {
		t.Fatalf("escrowRoutePrefix(unpinned) = %q, want the gateway's own %q", got, "/devshard/v4")
	}
}
