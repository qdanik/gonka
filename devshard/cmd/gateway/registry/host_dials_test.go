package registry

import (
	"reflect"
	"testing"
)

// A ping is owed to each host an escrow can actually reach, once per address: a validator holding
// several slots answers on one socket, and pinging it per slot would say the same thing three times.
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

// A retired escrow's hosts are nobody's to ping: the gateway stopped routing to them.
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

// An address the session cannot name is not a target; a blank one would probe the gateway itself.
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
