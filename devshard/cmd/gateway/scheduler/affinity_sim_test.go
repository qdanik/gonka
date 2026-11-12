package scheduler

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/config"
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
	simUnboundedPinAge  = 1000 * time.Hour
	simUnboundedShare   = 10_000
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
	budgetExceptions int
	shareCapped      int
	pinAged          int
	mostCharged      int64
}

type simPolicy struct {
	label     string
	affinity  bool
	maxBurns  int64
	maxPinAge time.Duration
	shareCap  int
}

func simDefaults(label string) simPolicy {
	scheduling := config.Defaults().Scheduler
	return simPolicy{
		label: label, affinity: true, maxBurns: scheduling.AffinityMaxBurns,
		maxPinAge: time.Duration(scheduling.AffinityMaxPinAgeMS) * time.Millisecond, shareCap: int(scheduling.AffinityShareCap),
	}
}

func simBoundsOff(label string) simPolicy {
	return simPolicy{label: label, affinity: true, maxBurns: unboundedMaxBurns, maxPinAge: simUnboundedPinAge, shareCap: simUnboundedShare}
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

func runSimulation(t *testing.T, policy simPolicy, escrowCount int, coarrival bool) simRun {
	t.Helper()
	random := rand.New(rand.NewPCG(7, uint64(escrowCount)))
	hosts := groupOf(affinityGroupSize)
	cfg := schedulerConfig{slotsByEscrow: map[string][]string{}}
	groups := make([][]string, escrowCount)
	slots := map[string]int{}
	for index := range escrowCount {
		escrowID := fmt.Sprintf("escrow-%d", index)
		groups[index] = make([]string, affinityGroupSize)
		for slot := range groups[index] {
			groups[index][slot] = hosts[random.IntN(affinityGroupSize)]
			slots[groups[index][slot]]++
		}
		cfg.escrows = append(cfg.escrows, escrowID)
		cfg.slotsByEscrow[escrowID] = groups[index]
	}
	cfg.tune = func(c *config.Config) { c.Scheduler.AffinityMaxBurns = policy.maxBurns }
	test := newSchedulerHarness(t, cfg)
	reachable := func(host string) bool {
		return slices.ContainsFunc(groups, func(group []string) bool { return slices.Contains(group, host) })
	}
	scheduling := config.Defaults().Scheduler
	limits := affinity.Limits{
		TTL: time.Duration(scheduling.AffinityTTLMS) * time.Millisecond, MaxEntries: int(scheduling.AffinityMaxEntries),
		MaxPinAge: policy.maxPinAge, ShareCap: policy.shareCap,
	}
	var elapsed time.Duration
	memory := affinity.NewTracker(
		func() affinity.Limits { return limits },
		func() map[string]int { return slots },
		func() time.Time { return time.Unix(0, 0).Add(elapsed) },
	)

	run := simRun{label: policy.label, escrows: escrowCount}
	lastHost := map[int]string{}
	for _, batch := range simBatches(simEvents(random), coarrival) {
		elapsed = batch[0].at
		jobs := make([]affinityJob, len(batch))
		for index, event := range batch {
			jobs[index] = affinityJob{requestID: fmt.Sprintf("s%d-t%d", event.session, event.turn)}
			if !policy.affinity {
				continue
			}
			host, status := memory.Lookup(affinity.Key(fmt.Sprintf("s%d", event.session)))
			jobs[index].target = host
			switch status {
			case affinity.StatusShareCapped:
				run.shareCapped++
			case affinity.StatusPinAged:
				run.pinAged++
			}
		}
		outcomes := test.pickAll(jobs)
		for index, event := range batch {
			require.NoError(t, outcomes[index].err, "pick of %s did not finish within %s", jobs[index].requestID, affinityPickDeadline)
			run.requests++
			if policy.affinity {
				memory.Record(affinity.Key(fmt.Sprintf("s%d", event.session)), outcomes[index].host)
			}
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
	charged := map[string]int64{}
	for _, requestID := range test.observer.burnRequests() {
		charged[requestID]++
		run.mostCharged = max(run.mostCharged, charged[requestID])
	}
	_, exceptions := test.observer.affinity()
	for _, reason := range exceptions {
		if reason == AffinityExceptionMaxBurns {
			run.budgetExceptions++
		}
	}
	return run
}

func simTable(runs []simRun) string {
	var table strings.Builder
	table.WriteString("| escrows | policy | follow-up turns landing on the last winner | landing where its host is in a group | affinity burns per request | burn share of spend | unbound turns: burn cap / share cap / pin age |\n")
	table.WriteString("|---|---|---|---|---|---|---|\n")
	for _, run := range runs {
		fmt.Fprintf(&table, "| %d | %s | %.3f | %.3f | %.2f | %.2f%% | %d / %d / %d |\n",
			run.escrows, run.label, run.landingShare(), run.landingShareWhereReachable(), run.burnsPerRequest(), 100*run.burnShareOfSpend(),
			run.budgetExceptions, run.shareCapped, run.pinAged)
	}
	return table.String()
}

// Test flow:
//  1. For one and four escrows, replay 1504 chat turns (188 sessions of 8 turns, 10 requests a second, groups of 16 slots drawn from 16 equal participants) through the real scheduler and a real tracker on the simulated clock, with no affinity, with the bounds off one request at a time and co-arriving, and with the default bounds one request at a time and co-arriving.
//  2. Assert that with the bounds off every follow-up turn whose last host is in some escrow's group lands on it, and that without affinity the landing share is the chance one.
//  3. Assert that with the defaults no single request is charged more burns than the burn cap, the burns per request fall below the bounds-off run's, and the landing share stays above the chance one.
//  4. Replay the same turns with a burn cap of zero and two, and with a 60 s pin age, and assert no request is charged more than the cap and the cap orders the burns per request.
//  5. Log the landing share, burns per request, burn share of spend and the unbound turns as a table.
func TestAffinitySimulationReportsLandingBurnsAndSpend(t *testing.T) {
	runs := make([]simRun, 0, 14)
	for _, escrowCount := range []int{1, 4} {
		baseline := runSimulation(t, simPolicy{label: simBaselineLabel}, escrowCount, false)
		require.Zero(t, baseline.burns, "without a target nothing is burned for affinity")
		require.Less(t, baseline.landingShare(), 0.3)
		runs = append(runs, baseline)

		for _, coarrival := range []bool{false, true} {
			suffix := ", one request at a time"
			if coarrival {
				suffix = ", co-arriving requests, order not fixed"
			}
			off := runSimulation(t, simBoundsOff("bounds off"+suffix), escrowCount, coarrival)
			defaults := runSimulation(t, simDefaults("default bounds"+suffix), escrowCount, coarrival)

			require.Equal(t, off.followUpsInGroup, off.landedInGroup, "%d escrows, %s: a follow-up whose host is in a group must land on it", escrowCount, off.label)
			require.Greater(t, off.followUpsInGroup, 0)
			require.Less(t, off.burnsPerRequest(), float64(affinityGroupSize-1))
			require.LessOrEqual(t, defaults.mostCharged, simDefaults("").maxBurns, "%d escrows, %s: no request pays more than the cap", escrowCount, defaults.label)
			require.Less(t, defaults.burnsPerRequest(), off.burnsPerRequest())
			require.Greater(t, defaults.landingShare(), baseline.landingShare())
			runs = append(runs, off, defaults)
		}

		var previousBurns float64
		for _, budget := range []int64{0, 1, 2} {
			policy := simDefaults(fmt.Sprintf("burn cap %d", budget))
			policy.maxBurns = budget
			run := runSimulation(t, policy, escrowCount, false)
			require.GreaterOrEqual(t, run.burnsPerRequest(), previousBurns)
			require.LessOrEqual(t, run.mostCharged, budget, "%s: no request pays more than the cap", run.label)
			previousBurns = run.burnsPerRequest()
			if budget != 1 {
				runs = append(runs, run)
			}
		}
		aged := simDefaults("default bounds, pin age 60 s")
		aged.maxPinAge = time.Minute
		agedRun := runSimulation(t, aged, escrowCount, false)
		require.Positive(t, agedRun.pinAged)
		runs = append(runs, agedRun)
	}
	t.Log("\n" + simTable(runs))
}
