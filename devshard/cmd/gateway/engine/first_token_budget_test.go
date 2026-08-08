package engine

import (
	"testing"
	"time"
)

func budgetPolicy() EscalationPolicy {
	return EscalationPolicy{FirstTokenFloor: time.Second, FirstTokenCeiling: 60 * time.Second}
}

func TestFirstTokenBudget_GivesRoomToAHostThatReliablyAnswers(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	curve := policy.firstTokenTimeout(400)

	budget := policy.firstTokenBudget(400, 5*time.Second)

	if budget <= curve {
		t.Fatalf("budget = %v, want more than the curve %v", budget, curve)
	}
	if want := 7500 * time.Millisecond; budget != want {
		t.Errorf("budget = %v, want %v", budget, want)
	}
}

func TestFirstTokenBudget_KeepsTheCurveForAHostThatNeverAnswersInTime(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	curve := policy.firstTokenTimeout(400)

	for _, observed := range []time.Duration{75 * time.Second, 300 * time.Second} {
		if budget := policy.firstTokenBudget(400, observed); budget != curve {
			t.Errorf("observed %v: budget = %v, want the curve %v", observed, budget, curve)
		}
	}
}

func TestFirstTokenBudget_FallsBackToTheCurveWithoutAHistory(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	curve := policy.firstTokenTimeout(400)

	if budget := policy.firstTokenBudget(400, 0); budget != curve {
		t.Errorf("budget = %v, want the curve %v", budget, curve)
	}
}

func TestFirstTokenBudget_NeverShortensTheDeadline(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	curve := policy.firstTokenTimeout(400)

	if budget := policy.firstTokenBudget(400, 200*time.Millisecond); budget != curve {
		t.Errorf("budget = %v, want the curve %v", budget, curve)
	}
}

func TestFirstTokenBudget_StaysUnderTheConfiguredCeiling(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	policy.FirstTokenCeiling = 4 * time.Second

	if budget := policy.firstTokenBudget(400, 5*time.Second); budget != 4*time.Second {
		t.Errorf("budget = %v, want the ceiling 4s", budget)
	}
}

func TestFirstTokenBudget_AtTheEdgeOfWhatCountsAsReliable(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	curve := policy.firstTokenTimeout(400)

	if budget := policy.firstTokenBudget(400, firstTokenObservedLimit*curve); budget <= curve {
		t.Errorf("a host exactly at the limit still counts as reliable: budget = %v", budget)
	}
	if budget := policy.firstTokenBudget(400, firstTokenObservedLimit*curve+time.Nanosecond); budget != curve {
		t.Errorf("one nanosecond past the limit must keep the curve: budget = %v", budget)
	}
}

// A deployment may leave the ceiling unset; the limit on what counts as reliable still bounds it.
func TestFirstTokenBudget_StaysBoundedWithoutACeiling(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	policy.FirstTokenCeiling = 0
	curve := policy.firstTokenTimeout(400)

	budget := policy.firstTokenBudget(400, firstTokenObservedLimit*curve)

	if ceiling := firstTokenObservedLimit * firstTokenObservedSlack * curve / 2; budget > ceiling {
		t.Errorf("budget = %v, want no more than %v", budget, ceiling)
	}
}

// A clock that hands back a negative reading must not read as a fast host.
func TestFirstTokenBudget_IgnoresANegativeObservation(t *testing.T) {
	t.Parallel()
	policy := budgetPolicy()
	curve := policy.firstTokenTimeout(400)

	if budget := policy.firstTokenBudget(400, -time.Second); budget != curve {
		t.Errorf("budget = %v, want the curve %v", budget, curve)
	}
}
