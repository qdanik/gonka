package scheduler

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	simSessions         = 188
	simTurns            = 8
	simSessionSpacing   = 800 * time.Millisecond
	simThinkTime        = 15 * time.Second
	simThinkJitter      = 2 * time.Second
	simCoarrivalWindow  = 20 * time.Millisecond
	simSettledTokens    = 5150.0
	simBurnReserveToken = 60.0
	simBaselineLabel    = "today, no affinity"
)

type simEvent struct {
	at      time.Duration
	session int
	turn    int
}

type simRun struct {
	label            string
	escrows          int
	requests         int
	followUps        int
	landedOnLast     int
	followUpsInGroup int
	landedInGroup    int
	burns            int
}

func (r simRun) landingShare() float64 { return share(r.landedOnLast, r.followUps) }

func (r simRun) landingShareWhereReachable() float64 {
	return share(r.landedInGroup, r.followUpsInGroup)
}

func (r simRun) burnsPerRequest() float64 { return float64(r.burns) / float64(r.requests) }

func (r simRun) burnShareOfSpend() float64 {
	return simBurnReserveToken * r.burnsPerRequest() / simSettledTokens
}

func share(part, whole int) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

func simEvents(random *rand.Rand) []simEvent {
	events := make([]simEvent, 0, simSessions*simTurns)
	for session := range simSessions {
		at := time.Duration(session)*simSessionSpacing + time.Duration(random.Int64N(int64(simSessionSpacing)))
		for turn := range simTurns {
			events = append(events, simEvent{at: at, session: session, turn: turn})
			at += simThinkTime + time.Duration(random.Int64N(int64(simThinkJitter)))
		}
	}
	slices.SortStableFunc(events, func(a, b simEvent) int { return int(a.at - b.at) })
	return events
}

func simBatches(events []simEvent, coarrival bool) [][]simEvent {
	var batches [][]simEvent
	for _, event := range events {
		last := len(batches) - 1
		if coarrival && last >= 0 && event.at-batches[last][0].at < simCoarrivalWindow {
			batches[last] = append(batches[last], event)
			continue
		}
		batches = append(batches, []simEvent{event})
	}
	return batches
}

func runSimulation(t *testing.T, label string, escrowCount int, affinity, coarrival bool) simRun {
	t.Helper()
	random := rand.New(rand.NewPCG(7, uint64(escrowCount)))
	hosts := groupOf(affinityGroupSize)
	cfg := schedulerConfig{slotsByEscrow: map[string][]string{}}
	groups := make([][]string, escrowCount)
	for index := range escrowCount {
		escrowID := fmt.Sprintf("escrow-%d", index)
		groups[index] = make([]string, affinityGroupSize)
		for slot := range groups[index] {
			groups[index][slot] = hosts[random.IntN(affinityGroupSize)]
		}
		cfg.escrows = append(cfg.escrows, escrowID)
		cfg.slotsByEscrow[escrowID] = groups[index]
	}
	test := newSchedulerHarness(t, cfg)
	reachable := func(host string) bool {
		return slices.ContainsFunc(groups, func(group []string) bool { return slices.Contains(group, host) })
	}

	run := simRun{label: label, escrows: escrowCount}
	lastHost := map[int]string{}
	for _, batch := range simBatches(simEvents(random), coarrival) {
		jobs := make([]affinityJob, len(batch))
		for index, event := range batch {
			jobs[index] = affinityJob{requestID: fmt.Sprintf("s%d-t%d", event.session, event.turn)}
			if affinity {
				jobs[index].target = lastHost[event.session]
			}
		}
		outcomes := test.pickAll(jobs)
		for index, event := range batch {
			require.NoError(t, outcomes[index].err, "pick of %s did not finish within %s", jobs[index].requestID, affinityPickDeadline)
			run.requests++
			previous, followUp := lastHost[event.session]
			lastHost[event.session] = outcomes[index].host
			if !followUp {
				continue
			}
			run.followUps++
			landed := outcomes[index].host == previous
			if landed {
				run.landedOnLast++
			}
			if reachable(previous) {
				run.followUpsInGroup++
				if landed {
					run.landedInGroup++
				}
			}
		}
	}
	run.burns = affinityBurnsOf(test.observer.burns())
	return run
}

func simTable(runs []simRun) string {
	var table strings.Builder
	table.WriteString("| escrows | policy | follow-up turns landing on the last winner | landing where its host is in a group | affinity burns per request | burn share of spend |\n")
	table.WriteString("|---|---|---|---|---|---|\n")
	for _, run := range runs {
		fmt.Fprintf(&table, "| %d | %s | %.3f | %.3f | %.2f | %.2f%% |\n",
			run.escrows, run.label, run.landingShare(), run.landingShareWhereReachable(), run.burnsPerRequest(), 100*run.burnShareOfSpend())
	}
	return table.String()
}

// Test flow:
//  1. For one and four escrows, replay 1504 chat turns (188 sessions of 8 turns, 10 requests a second, groups of 16 slots drawn from 16 equal participants) through the real scheduler, with no affinity, with affinity one request at a time, and with affinity and requests arriving within 20 ms of each other together.
//  2. Assert that with affinity every follow-up turn whose last host is in some escrow's group lands on it, and that without affinity the landing share is the chance one.
//  3. Assert the burns per request stay under a group's other slots, and that the run with no target burned nothing.
//  4. Log the landing share, burns per request and burn share of spend as a table.
func TestAffinitySimulationReportsLandingBurnsAndSpend(t *testing.T) {
	runs := make([]simRun, 0, 6)
	for _, escrowCount := range []int{1, 4} {
		baseline := runSimulation(t, simBaselineLabel, escrowCount, false, false)
		sequential := runSimulation(t, "guaranteed, one request at a time", escrowCount, true, false)
		batched := runSimulation(t, "guaranteed, co-arriving requests, order not fixed", escrowCount, true, true)

		require.Zero(t, baseline.burns, "without a target nothing is burned for affinity")
		require.Less(t, baseline.landingShare(), 0.3)
		for _, run := range []simRun{sequential, batched} {
			require.Equal(t, run.followUpsInGroup, run.landedInGroup, "%d escrows, %s: a follow-up whose host is in a group must land on it", escrowCount, run.label)
			require.Greater(t, run.followUpsInGroup, 0)
			require.Less(t, run.burnsPerRequest(), float64(affinityGroupSize-1))
		}
		runs = append(runs, baseline, sequential, batched)
	}
	t.Log("\n" + simTable(runs))
}
