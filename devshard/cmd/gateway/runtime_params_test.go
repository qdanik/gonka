package main

import "testing"

// A provider that dials nothing leaves a feed that was never built, and a typed nil in an interface
// field is not nil — the session factory and the shutdown order both reach this one through it.
func TestAnUnbuiltParamsFeedAnswersAndClosesWithoutPanicking(t *testing.T) {
	var absent *runtimeParams

	if schedule := absent.Heartbeat(); schedule != nil {
		t.Fatalf("Heartbeat() = %+v, want nil so the session keeps its compiled schedule", *schedule)
	}
	if err := absent.Close(); err != nil {
		t.Fatalf("Close() = %v, want a feed that was never opened to close cleanly", err)
	}
}
