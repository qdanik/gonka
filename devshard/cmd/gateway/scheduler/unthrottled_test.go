package scheduler

import "testing"

// laddered builds the fleet ladder a drain freezes, with every rung open unless a test closes it.
func laddered(closed func(*availability)) availability {
	gates := availability{
		pocRequired:  always(false),
		congested:    windowFullWhen(always(false)),
		ejected:      always(false),
		stateBlocked: always(false),
	}
	closed(&gates)
	return gates
}

// A full window is the gate this list exists to cross: the host is working, and the burn it would
// earn is a queueing decision rather than a fact about the host.
func TestUnthrottledCrossesAFullWindow(t *testing.T) {
	t.Parallel()
	gates := laddered(func(gates *availability) {
		gates.congested = windowFullWhen(always(true))
		gates.unthrottled = always(true)
	})

	if reason := gates.participantBlocked(hostA); reason != blockNone {
		t.Errorf("reason = %v, want blockNone: an unthrottled host is served over a full window", reason)
	}
}

func TestUnthrottledCrossesEjection(t *testing.T) {
	t.Parallel()
	gates := laddered(func(gates *availability) {
		gates.ejected = always(true)
		gates.unthrottled = always(true)
	})

	if reason := gates.participantBlocked(hostA); reason != blockNone {
		t.Errorf("reason = %v, want blockNone", reason)
	}
}

// The two lists are one admission decision: naming a host unthrottled admits it whether or not the
// allowlist names it, and says nothing about anybody else.
func TestUnthrottledIsAdmittedOutsideTheAllowlist(t *testing.T) {
	t.Parallel()
	gates := laddered(func(gates *availability) {
		gates.notAllowed = refusedByAllowlist([]string{hostB}, []string{hostA})
		gates.unthrottled = waivesThrottling([]string{hostA})
	})

	if reason := gates.participantBlocked(hostA); reason != blockNone {
		t.Errorf("unthrottled host: reason = %v, want blockNone", reason)
	}
	if reason := gates.participantBlocked(hostB); reason != blockNone {
		t.Errorf("allowlisted host: reason = %v, want blockNone", reason)
	}
	if reason := gates.participantBlocked("stranger"); reason != blockNotAllowed {
		t.Errorf("stranger: reason = %v, want blockNotAllowed", reason)
	}
}

// Each of these is a fact about the host or the chain that no operator list can make untrue.
func TestUnthrottledStopsAtTheGatesThatProtectTheNonce(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		close func(*availability)
		want  blockReason
	}{
		"cut off": {
			close: func(gates *availability) {
				gates.congested = func(string) blockReason { return blockCutOff }
			},
			want: blockCutOff,
		},
		"owes proof-of-compute": {
			close: func(gates *availability) { gates.pocRequired = always(true) },
			want:  blockPoCRequired,
		},
		"escrow state diverged": {
			close: func(gates *availability) { gates.stateBlocked = always(true) },
			want:  blockStateDiverged,
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gates := laddered(func(gates *availability) {
				testCase.close(gates)
				gates.unthrottled = always(true)
			})

			if reason := gates.participantBlocked(hostA); reason != testCase.want {
				t.Errorf("reason = %v, want %v", reason, testCase.want)
			}
		})
	}
}

// Reachability is read at escrow pick as well as at dispatch, so it has to admit the same set.
func TestEscrowHoldingOnlyAnUnthrottledParticipantIsReachable(t *testing.T) {
	t.Parallel()
	reachable := reachableByAllowlist([]string{"nobody-holds-this"}, []string{hostA})

	if !reachable(Escrow{Session: &fakeSession{slots: soleHost, participants: soleHost}}) {
		t.Error("an escrow whose group holds an unthrottled participant must stay reachable")
	}
	stranger := []string{"stranger"}
	if reachable(Escrow{Session: &fakeSession{slots: stranger, participants: stranger}}) {
		t.Error("an escrow holding neither an allowed nor an unthrottled participant must stay unreachable")
	}
}

// The end the operator asked for: the nonce reaches its host instead of being spent on nobody.
func TestUnthrottledServesOverAFullWindowWithoutBurning(t *testing.T) {
	test := newHarness(t, harnessConfig{
		slots:       soleHost,
		windowFull:  always(true),
		unthrottled: always(true),
	})

	queued := test.submit(t, test.clock.Now().Add(-2*matchWaitWindow))

	result := awaitReply(t, queued)
	if result.err != nil {
		t.Fatalf("err = %v, want an assignment over the full window", result.err)
	}
	test.dispatcher.stop()

	if burns := test.observer.burns(); len(burns) != 0 {
		t.Errorf("ghost burns = %v, want none", burns)
	}
}
