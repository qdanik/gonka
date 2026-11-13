package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/config"
)

const (
	affinityPickDeadline = 10 * time.Second
	unboundedMaxBurns    = 1_000
)

type affinityJob struct {
	requestID string
	target    string
}

type affinityOutcome struct {
	host string
	err  error
}

func permutedGroup(seed int) []string {
	strides := []int{1, 3, 5, 7}
	stride := strides[seed%len(strides)]
	group := make([]string, affinityGroupSize)
	for index := range group {
		group[index] = benchParticipant((index*stride + seed) % affinityGroupSize)
	}
	return group
}

func (h *schedulerHarness) pickAll(jobs []affinityJob) []affinityOutcome {
	outcomes := make([]affinityOutcome, len(jobs))
	var running sync.WaitGroup
	for index, job := range jobs {
		running.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), affinityPickDeadline)
			defer cancel()
			assignment, err := h.scheduler.Pick(ctx, RequestProfile{RequestID: job.requestID, Model: modelA, Target: job.target, Params: "payload"})
			if err == nil {
				assignment.ReleaseHostSlot()
				assignment.ReleaseEscrow()
			}
			outcomes[index] = affinityOutcome{host: assignment.Host, err: err}
		})
	}
	running.Wait()
	return outcomes
}

// awaitAffinityReports waits for the observer to hold a report for every bound request: the dispatcher reports a landing just after it wakes the picker.
func (h *schedulerHarness) awaitAffinityReports(t *testing.T, bound int) {
	t.Helper()
	eventually(t, fmt.Sprintf("the observer to record all %d bound requests", bound), func() bool {
		landed, exceptions := h.observer.affinity()
		return landed+len(exceptions) >= bound
	})
}

func affinityMixes() map[string][]affinityJob {
	hosts := groupOf(affinityGroupSize)
	mixes := map[string][]affinityJob{}
	for index := range affinityGroupSize {
		mixes["one follow-up at a time"] = append(mixes["one follow-up at a time"], affinityJob{fmt.Sprintf("serial-%d", index), hosts[(index*5)%affinityGroupSize]})
	}
	for index := range affinityGroupSize {
		mixes["every host at once beside free requests"] = append(mixes["every host at once beside free requests"],
			affinityJob{fmt.Sprintf("bound-%d", index), hosts[index]}, affinityJob{requestID: fmt.Sprintf("free-%d", index)})
	}
	for index := range 10 {
		mixes["one host at once"] = append(mixes["one host at once"], affinityJob{fmt.Sprintf("same-%d", index), hosts[11]})
	}
	seed := uint32(1)
	for index := range 96 {
		seed = seed*1664525 + 1013904223
		job := affinityJob{requestID: fmt.Sprintf("mixed-%d", index), target: hosts[int(seed>>16)%affinityGroupSize]}
		if index%4 == 3 {
			job.target = NoTarget
		}
		mixes["shuffled mixture"] = append(mixes["shuffled mixture"], job)
	}
	return mixes
}

// Test flow:
//  1. For one, two and four escrows whose groups hold the same 16 hosts in different orders, build a scheduler whose burn cap is too large to bind.
//  2. For each queue mix (one at a time, every host at once beside free requests, ten requests for one host, a shuffled mixture) pick every request concurrently.
//  3. Assert every request bound for a host lands on that host, and every free request is served.
//  4. Wait until the observer has recorded a report for every bound request.
//  5. Assert the hits counted equal the bound requests, no exception was counted, and every burn is an `affinity_skip`.
func TestEveryRequestBoundForAHostLandsOnItAcrossEscrowsAndQueueMixes(t *testing.T) {
	for _, escrowCount := range []int{1, 2, 4} {
		for name, jobs := range affinityMixes() {
			t.Run(fmt.Sprintf("%d escrows/%s", escrowCount, name), func(t *testing.T) {
				cfg := schedulerConfig{slotsByEscrow: map[string][]string{}}
				for index := range escrowCount {
					escrowID := fmt.Sprintf("escrow-%d", index)
					cfg.escrows = append(cfg.escrows, escrowID)
					cfg.slotsByEscrow[escrowID] = permutedGroup(index)
				}
				cfg.tune = func(c *config.Config) { c.Affinity.MaxBurns = unboundedMaxBurns }
				test := newSchedulerHarness(t, cfg)

				outcomes := test.pickAll(jobs)

				bound := 0
				for index, job := range jobs {
					require.NoError(t, outcomes[index].err, "pick of %s did not finish within %s", job.requestID, affinityPickDeadline)
					if job.target == NoTarget {
						continue
					}
					bound++
					require.Equal(t, job.target, outcomes[index].host, "request %s left its warm host", job.requestID)
				}
				test.awaitAffinityReports(t, bound)
				landed, exceptions := test.observer.affinity()
				require.Equal(t, bound, landed)
				require.Empty(t, exceptions)
				for _, reason := range test.observer.burns() {
					require.Equal(t, GhostReasonAffinitySkip, reason)
				}
			})
		}
	}
}

// Test flow:
//  1. For burn caps of zero, one and two, and one and four escrows over the same 16 hosts, build a scheduler.
//  2. For each queue mix, pick every request concurrently.
//  3. Assert every request is served, and every bound request either landed on its host or was counted as a `max_burns` exception.
//  4. Wait until the observer has recorded a report for every bound request.
//  5. Assert no request was charged more `affinity_skip` burns than the cap, and every hit was counted.
func TestNoRequestIsChargedMoreThanTheBurnCapAcrossEscrowsAndQueueMixes(t *testing.T) {
	for _, budget := range []int64{0, 1, 2} {
		for _, escrowCount := range []int{1, 4} {
			for name, jobs := range affinityMixes() {
				t.Run(fmt.Sprintf("budget %d/%d escrows/%s", budget, escrowCount, name), func(t *testing.T) {
					cfg := schedulerConfig{slotsByEscrow: map[string][]string{}}
					for index := range escrowCount {
						escrowID := fmt.Sprintf("escrow-%d", index)
						cfg.escrows = append(cfg.escrows, escrowID)
						cfg.slotsByEscrow[escrowID] = permutedGroup(index)
					}
					cfg.tune = func(c *config.Config) { c.Affinity.MaxBurns = budget }
					test := newSchedulerHarness(t, cfg)

					outcomes := test.pickAll(jobs)

					bound, landedOnHost := 0, 0
					for index, job := range jobs {
						require.NoError(t, outcomes[index].err, "pick of %s did not finish within %s", job.requestID, affinityPickDeadline)
						if job.target == NoTarget {
							continue
						}
						bound++
						if outcomes[index].host == job.target {
							landedOnHost++
						}
					}
					test.awaitAffinityReports(t, bound)
					landed, exceptions := test.observer.affinity()
					require.LessOrEqual(t, landed, landedOnHost, "a hit is a request that landed on its host")
					require.Equal(t, bound, landed+len(exceptions))
					for _, reason := range exceptions {
						require.Equal(t, affinity.ExceptionMaxBurns, reason)
					}
					charged := map[string]int64{}
					for _, requestID := range test.observer.burnRequests() {
						charged[requestID]++
					}
					for requestID, burns := range charged {
						require.LessOrEqual(t, burns, budget, "request %s was charged more than the burn cap", requestID)
					}
				})
			}
		}
	}
}
