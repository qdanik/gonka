package registry

import (
	"reflect"
	"testing"
)

// Test flow:
//  1. Build a registry with two escrows: one session dialing hostA's address twice plus hostB, another session dialing hostB again.
//  2. Add both escrows to the registry.
//  3. Assert `HostDials` reports each address exactly once, for hostA and hostB.
func TestHostDialsNameEachAddressOnce(t *testing.T) {
	t.Parallel()
	first := newFakeSession("hostA", "hostA", "hostB")
	first.dials = []HostDial{
		{ParticipantKey: "hostA", BaseURL: "http://a:8080", RoutePrefix: "/api"},
		{ParticipantKey: "hostA", BaseURL: "http://a:8080", RoutePrefix: "/api"},
		{ParticipantKey: "hostB", BaseURL: "http://b:8080", RoutePrefix: "/api"},
	}
	second := newFakeSession("hostB")
	second.dials = []HostDial{{ParticipantKey: "hostB", BaseURL: "http://b:8080", RoutePrefix: "/api"}}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": first, "2": second}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	mustAdd(t, registry, "2", "qwen")

	want := []HostDial{
		{ParticipantKey: "hostA", BaseURL: "http://a:8080", RoutePrefix: "/api"},
		{ParticipantKey: "hostB", BaseURL: "http://b:8080", RoutePrefix: "/api"},
	}
	if got := registry.HostDials(); !reflect.DeepEqual(got, want) {
		t.Fatalf("HostDials() = %+v, want %+v", got, want)
	}
}

// Test flow:
//  1. Build a registry with one escrow for hostA and add it.
//  2. Retire that escrow.
//  3. Assert `HostDials` reports no addresses.
func TestARetiredEscrowsHostsAreNotPinged(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.dials = []HostDial{{ParticipantKey: "hostA", BaseURL: "http://a:8080"}}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire() = %v", err)
	}

	if got := registry.HostDials(); len(got) != 0 {
		t.Fatalf("HostDials() = %+v, want none", got)
	}
}

// Test flow:
//  1. Build a registry with one escrow whose dial has a blank BaseURL and add it.
//  2. Assert `HostDials` reports no addresses.
func TestAHostWithNoAddressIsNoTarget(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.dials = []HostDial{{ParticipantKey: "hostA", BaseURL: ""}}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	if got := registry.HostDials(); len(got) != 0 {
		t.Fatalf("HostDials() = %+v, want none", got)
	}
}
