package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/scheduler"
)

type recordedWinner struct {
	key          affinity.Key
	participant  string
	promptTokens uint64
}

type spyMemory struct {
	participant string
	status      affinity.Status
	recorded    []recordedWinner
	lookedUp    []uint64
}

func (s *spyMemory) Lookup(_ affinity.Key, promptTokens uint64) (string, affinity.Status) {
	s.lookedUp = append(s.lookedUp, promptTokens)
	return s.participant, s.status
}

func (s *spyMemory) Record(key affinity.Key, participant string, promptTokens uint64) {
	s.recorded = append(s.recorded, recordedWinner{key: key, participant: participant, promptTokens: promptTokens})
}

func affinityCoordinator(memory hostMemory, key affinity.Key) *raceCoordinator {
	return &raceCoordinator{deps: raceDeps{Affinity: memory}, request: raceRequest{Request: Request{AffinityKey: key}}}
}

// Test flow:
//  1. Build coordinators over a memory that answers a hit, an expired entry, an aged-out pin, a share-capped win, a hit below the minimum cached prefix and an unknown key.
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
		{"a hit whose prefix is below the minimum", &spyMemory{status: affinity.StatusBelowMinCached}, "key", scheduler.NoTarget, scheduler.AffinityExceptionBelowMinCached},
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
	coordinator.request.InputTokens = 5_000

	coordinator.crownWinner(&liveAttempt{participant: "host-b"}, crownFirstClaim)

	require.Equal(t, []recordedWinner{{key: "key", participant: "host-b", promptTokens: 5_000}}, memory.recorded)

	unkeyed := &spyMemory{}
	affinityCoordinator(unkeyed, affinity.NoKey).crownWinner(&liveAttempt{participant: "host-b"}, crownFirstClaim)
	require.Empty(t, unkeyed.recorded)
}

// Test flow:
//  1. Build a coordinator whose request carries the admission count.
//  2. Ask for the affinity intent and crown a winner.
//  3. Assert the lookup and the record both got that one count.
func TestTheAdmissionCountReachesTheMemory(t *testing.T) {
	memory := &spyMemory{participant: "host-a", status: affinity.StatusHit}
	coordinator := affinityCoordinator(memory, "key")
	coordinator.request.InputTokens = 6_144

	coordinator.affinityIntent()
	coordinator.crownWinner(&liveAttempt{participant: "host-a"}, crownFirstClaim)

	require.Equal(t, []uint64{6_144}, memory.lookedUp)
	require.Equal(t, []recordedWinner{{key: "key", participant: "host-a", promptTokens: 6_144}}, memory.recorded)
}
