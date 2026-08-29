package burns

import (
	"testing"
	"time"
)

func TestTheProbeGateAdmitsOneHostAtATime(t *testing.T) {
	gate := newProbeGate()
	now := time.Unix(1_700_000_000, 0)

	if !gate.enter("host-a", now) {
		t.Fatal("the first probe was refused")
	}
	if gate.enter("host-a", now.Add(time.Hour)) {
		t.Error("a second probe entered while the first was still in flight")
	}
	if !gate.enter("host-b", now) {
		t.Error("one host in flight blocked another")
	}
}

func TestTheProbeGateHoldsTheIntervalAfterOneFinishes(t *testing.T) {
	gate := newProbeGate()
	now := time.Unix(1_700_000_000, 0)

	gate.enter("host-a", now)
	gate.leave("host-a")

	if gate.enter("host-a", now.Add(probeInterval-time.Millisecond)) {
		t.Error("a probe entered before the interval elapsed")
	}
	if !gate.enter("host-a", now.Add(probeInterval)) {
		t.Error("a probe was refused once the interval had elapsed")
	}
}
