package metrics

import "testing"

func TestTheJournalCollectorReportsWhatTheJournalCounted(t *testing.T) {
	telemetry := New()
	telemetry.Register(NewJournalCollector(func() JournalCounts {
		return JournalCounts{MoneyRefused: 3, ProgressDropped: 5, LateEvents: 1}
	}))

	expectCounter(t, telemetry, "devshard_gateway_journal_money_refused_total", labels{}, 3)
	expectCounter(t, telemetry, "devshard_gateway_journal_progress_dropped_total", labels{}, 5)
	expectCounter(t, telemetry, "devshard_gateway_journal_late_events_total", labels{}, 1)
}

func TestAJournalCollectorWithNoSourceReportsNothing(t *testing.T) {
	telemetry := New()
	telemetry.Register(NewJournalCollector(nil))

	expectSeriesCount(t, telemetry, "devshard_gateway_journal_money_refused_total", 0)
}
