package engine

import (
	"testing"

	"devshard/cmd/gateway/share"
)

type shareCounts struct{ counted []share.Kind }

func (c *shareCounts) Count(_, _ string, kind share.Kind) { c.counted = append(c.counted, kind) }

// Test flow:
//  1. Record a clean finish, a nonce left unfinished, and a phase-transition abort through `recordJudgedSample` with a counting ledger.
//  2. Assert the clean finish counts served, the unfinished nonce counts failed, and the abort that the sampling exempts counts nothing.
func TestAJudgedSampleIsCountedServedOrFailedAndAnExemptOneIsNot(t *testing.T) {
	unfinished := cleanAttempt()
	unfinished.NonceFinished = false
	aborted := cleanAttempt()
	aborted.PhaseTransitionAborted = true
	counts := &shareCounts{}
	engine := &Engine{deps: Deps{Perf: &simTracker{stubPerf: &stubPerf{}}, Shares: counts}}

	engine.recordJudgedSample(race(cleanAttempt()), cleanAttempt())
	engine.recordJudgedSample(race(unfinished), unfinished)
	engine.recordJudgedSample(race(aborted), aborted)

	if len(counts.counted) != 2 || counts.counted[0] != share.Served || counts.counted[1] != share.Failed {
		t.Errorf("counted = %v, want served then failed and nothing for the exempt abort", counts.counted)
	}
}
