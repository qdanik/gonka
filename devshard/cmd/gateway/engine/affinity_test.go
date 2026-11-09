package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/scheduler"
)

type recordedWinner struct {
	key         affinity.Key
	participant string
}

type spyMemory struct {
	participant string
	status      affinity.Status
	recorded    []recordedWinner
}

func (s *spyMemory) Lookup(affinity.Key) (string, affinity.Status) { return s.participant, s.status }

func (s *spyMemory) Record(key affinity.Key, participant, _ string) {
	s.recorded = append(s.recorded, recordedWinner{key: key, participant: participant})
}

func affinityCoordinator(memory hostMemory, key affinity.Key) *raceCoordinator {
	return &raceCoordinator{deps: raceDeps{Affinity: memory}, request: raceRequest{Request: Request{AffinityKey: key}}}
}

// Test flow:
//  1. Build coordinators over a memory that answers a hit, an expired entry and an unknown key.
//  2. Ask each for the first attempt's affinity intent.
//  3. Assert a hit names its host with no miss, an expired entry and an unknown key name no host and say why.
//  4. Assert a request with no key, and a coordinator with no memory at all, ask for nothing and report no miss.
func TestTheFirstAttemptAsksForTheHostTheKeyLastWon(t *testing.T) {
	cases := []struct {
		name       string
		memory     *spyMemory
		key        affinity.Key
		wantTarget string
		wantMiss   scheduler.AffinityException
	}{
		{"a hit", &spyMemory{participant: "host-a", status: affinity.StatusHit}, "key", "host-a", scheduler.AffinityExceptionNone},
		{"an expired entry", &spyMemory{status: affinity.StatusExpired}, "key", scheduler.NoTarget, scheduler.AffinityExceptionExpired},
		{"an unknown key", &spyMemory{status: affinity.StatusNew}, "key", scheduler.NoTarget, scheduler.AffinityExceptionNewKey},
		{"a request with no key", &spyMemory{participant: "host-a", status: affinity.StatusHit}, affinity.NoKey, scheduler.NoTarget, scheduler.AffinityExceptionNone},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			target, miss := affinityCoordinator(testCase.memory, testCase.key).affinityIntent()

			require.Equal(t, testCase.wantTarget, target)
			require.Equal(t, testCase.wantMiss, miss)
		})
	}

	target, miss := affinityCoordinator(nil, "key").affinityIntent()
	require.Equal(t, scheduler.NoTarget, target)
	require.Equal(t, scheduler.AffinityExceptionNone, miss)
}

// Test flow:
//  1. Build a coordinator over a spy memory and crown a winner.
//  2. Assert the key was recorded against the winner's participant.
//  3. Crown a winner for a request with no key and assert nothing was recorded.
func TestTheCrownedHostBecomesTheKeysLastWinner(t *testing.T) {
	memory := &spyMemory{}
	coordinator := affinityCoordinator(memory, "key")

	coordinator.crownWinner(&liveAttempt{participant: "host-b"}, crownFirstClaim)

	require.Equal(t, []recordedWinner{{key: "key", participant: "host-b"}}, memory.recorded)

	unkeyed := &spyMemory{}
	affinityCoordinator(unkeyed, affinity.NoKey).crownWinner(&liveAttempt{participant: "host-b"}, crownFirstClaim)
	require.Empty(t, unkeyed.recorded)
}

// Test flow:
//  1. Crown host-a for a request the gateway issued session key K, looked up by its derived key and reconnecting under K's digest, over a real tracker.
//  2. Assert a request that echoes K, keyed by K's digest alone, asks for host-a with no miss.
//  3. Assert a repeat that never echoed, keyed by the derived key, asks for host-a too and the tracker hands back K for it.
//  4. Assert a session digest nobody won is a new key that names no host, and that its winner is recorded under it.
func TestAWinUnderAnIssuedSessionBringsItsEchoAndItsRepeatBackToTheHost(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tracker := affinity.NewTracker(func() affinity.Limits { return affinity.Limits{TTL: time.Minute, MaxEntries: 16} }, func() time.Time { return now })
	derived, session := affinity.Key("derived"), affinity.Key("session")
	first := &raceCoordinator{deps: raceDeps{Affinity: tracker}, request: raceRequest{Request: Request{AffinityKey: derived, SessionDigest: session, IssuedSession: "K"}}}

	first.crownWinner(&liveAttempt{participant: "host-a"}, crownFirstClaim)

	target, miss := affinityCoordinator(tracker, session).affinityIntent()
	require.Equal(t, "host-a", target)
	require.Equal(t, scheduler.AffinityExceptionNone, miss)
	target, miss = affinityCoordinator(tracker, derived).affinityIntent()
	require.Equal(t, "host-a", target)
	require.Equal(t, scheduler.AffinityExceptionNone, miss)
	require.Equal(t, "K", tracker.Session(derived))

	unknown := affinityCoordinator(tracker, "unknown-session")
	target, miss = unknown.affinityIntent()
	require.Equal(t, scheduler.NoTarget, target)
	require.Equal(t, scheduler.AffinityExceptionNewKey, miss)
	unknown.crownWinner(&liveAttempt{participant: "host-b"}, crownFirstClaim)
	participant, status := tracker.Lookup("unknown-session")
	require.Equal(t, "host-b", participant)
	require.Equal(t, affinity.StatusHit, status)
}
