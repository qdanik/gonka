package user

import (
	"context"
	"testing"
	"time"

	"devshard/host"
	"devshard/internal/statetest"
	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/stub"
	"devshard/types"

	"github.com/stretchr/testify/require"
)

// verifyingClient is a host that both serves and votes, which is what a deployed one is. The
// in-process client alone cannot vote, so a sweep over it would never post anything.
type verifyingClient struct {
	HostClient
	TimeoutVerifier
}

func setupSweepSession(t *testing.T, accept bool) (*Session, []*signing.Secp256k1Signer) {
	t.Helper()
	hosts := make([]*signing.Secp256k1Signer, 3)
	for index := range hosts {
		hosts[index] = testutil.MustGenerateKey(t)
	}
	userKey := testutil.MustGenerateKey(t)
	group := testutil.MakeGroup(hosts)
	configuration := testutil.DefaultConfig(len(hosts))
	verifier := signing.NewSecp256k1Verifier()

	clients := make([]HostClient, len(hosts))
	for index := range hosts {
		machine := statetest.MustStateMachine(t, "escrow-1", configuration, group, 1_000_000, userKey.Address(), verifier)
		served, err := host.NewHost(machine, hosts[index], stub.NewInferenceEngine(), "escrow-1", group, nil)
		require.NoError(t, err)
		clients[index] = verifyingClient{
			HostClient:      &InProcessClient{Host: served},
			TimeoutVerifier: &mockTimeoutVerifier{accept: accept, signer: hosts[index], group: group, slotIdx: index},
		}
	}
	userMachine := statetest.MustStateMachine(t, "escrow-1", configuration, group, 1_000_000, userKey.Address(), verifier)
	session, err := NewSession(userMachine, userKey, "escrow-1", group, clients, verifier)
	require.NoError(t, err)
	return session, hosts
}

const sweepStartHeight = 100

func startedNonceAged(t *testing.T, session *Session, hosts []*signing.Secp256k1Signer, age time.Duration) uint64 {
	t.Helper()
	slots := uint64(len(hosts))
	raiseRawFloor(t, session.sm, hosts[0], slots, sweepStartHeight)
	nonce := startedNonce(t, session, hosts, 0)
	if age > 0 {
		raiseRawFloor(t, session.sm, hosts[0], slots, sweepStartHeight+uint64(age/(testutil.HeightClockBlockMs*time.Millisecond)))
	}
	return nonce
}

// The vote the race never posted is the whole point: without it the record settles at full reserve in
// the executor's favour, and the host keeps none of the miss it earned.
func TestSweepVotesAStartedNonceNoRaceIsLeftToRetry(t *testing.T) {
	session, hosts := setupSweepSession(t, true)
	nonce := startedNonceAged(t, session, hosts, 2*time.Hour)

	report := session.SweepExecutionTimeouts(context.Background(), 0, 4)

	require.Equal(t, SweepReport{Due: 1, Applied: 1}, report)
	record, tracked := session.sm.GetInference(nonce)
	require.True(t, tracked)
	require.Equal(t, types.StatusTimedOut, record.Status)
}

// A round that collects nothing must leave the nonce where the next sweep can find it again.
func TestSweepKeepsANonceItsVerifiersRefusedToDecide(t *testing.T) {
	session, hosts := setupSweepSession(t, false)
	nonce := startedNonceAged(t, session, hosts, 2*time.Hour)

	first := session.SweepExecutionTimeouts(context.Background(), 0, 4)
	second := session.SweepExecutionTimeouts(context.Background(), 0, 4)

	require.Equal(t, SweepReport{Due: 1, Failed: 1}, first)
	require.Equal(t, SweepReport{Due: 1, Failed: 1}, second, "an undecided nonce stays claimable")
	record, tracked := session.sm.GetInference(nonce)
	require.True(t, tracked)
	require.Equal(t, types.StatusStarted, record.Status)
}

// A record inside its deadline belongs to the race that owns it, and a second vote is pure waste.
func TestSweepLeavesANonceInsideItsDeadlineAlone(t *testing.T) {
	session, hosts := setupSweepSession(t, true)
	startedNonceAged(t, session, hosts, 0)

	report := session.SweepExecutionTimeouts(context.Background(), 0, 4)

	require.Equal(t, SweepReport{}, report)
}

// The grace keeps the sweep off a nonce whose own race is still due to wake and vote.
func TestSweepHonoursTheGraceBeforeClaimingANonce(t *testing.T) {
	session, hosts := setupSweepSession(t, true)
	executionTimeout := time.Duration(session.sm.Config().ExecutionTimeout) * time.Second
	startedNonceAged(t, session, hosts, executionTimeout+time.Minute)

	report := session.SweepExecutionTimeouts(context.Background(), time.Hour, 4)

	require.Equal(t, SweepReport{}, report)
}

// A nonce a previous process committed has no race left to wake in this one, so waiting out the grace only delays its vote.
func TestSweepClaimsANonceFromBeforeTheRestartWithoutWaitingOutTheGrace(t *testing.T) {
	session, hosts := setupSweepSession(t, true)
	executionTimeout := time.Duration(session.sm.Config().ExecutionTimeout) * time.Second
	nonce := startedNonceAged(t, session, hosts, executionTimeout+time.Minute)
	session.adoptRecoveredNonces()

	report := session.SweepExecutionTimeouts(context.Background(), time.Hour, 4)

	require.Equal(t, SweepReport{Due: 1, Applied: 1}, report)
	record, tracked := session.sm.GetInference(nonce)
	require.True(t, tracked)
	require.Equal(t, types.StatusTimedOut, record.Status)
}

// A cancelled context stops the sweep where it stands: shutdown must not wait out a vote round.
func TestSweepStopsOnACancelledContext(t *testing.T) {
	session, hosts := setupSweepSession(t, true)
	startedNonceAged(t, session, hosts, 2*time.Hour)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	report := session.SweepExecutionTimeouts(cancelled, 0, 4)

	require.Equal(t, SweepReport{Due: 1}, report, "a sweep that never voted reports no verdict either way")
}

// A zero budget is the off switch, and it must cost nothing at all.
func TestSweepWithoutABudgetDoesNothing(t *testing.T) {
	session, hosts := setupSweepSession(t, true)
	startedNonceAged(t, session, hosts, 2*time.Hour)

	require.Equal(t, SweepReport{}, session.SweepExecutionTimeouts(context.Background(), 0, 0))
}
