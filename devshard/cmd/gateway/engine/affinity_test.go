package engine

import (
	"testing"

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

func (s *spyMemory) Record(key affinity.Key, participant string) {
	s.recorded = append(s.recorded, recordedWinner{key: key, participant: participant})
}

func affinityCoordinator(memory hostMemory, key affinity.Key) *raceCoordinator {
	return &raceCoordinator{deps: raceDeps{Affinity: memory}, request: raceRequest{Request: Request{AffinityKey: key}}}
}

// Test flow:
//  1. Build coordinators over a memory that answers a hit, an expired entry, an aged-out pin, a share-capped win and an unknown key.
//  2. Ask each for the first attempt's affinity intent.
//  3. Assert a hit names its host with no miss, every other answer names no host and says why.
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
		{"an aged-out pin", &spyMemory{status: affinity.StatusPinAged}, "key", scheduler.NoTarget, scheduler.AffinityExceptionPinAged},
		{"a share-capped win", &spyMemory{status: affinity.StatusShareCapped}, "key", scheduler.NoTarget, scheduler.AffinityExceptionShareCapped},
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
