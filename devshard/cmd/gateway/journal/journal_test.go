package journal

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/leakcheck"
	"devshard/cmd/gateway/internal/logcapture"
	"devshard/cmd/gateway/scheduler"
)

func TestMain(m *testing.M) {
	leakcheck.VerifyTestMain(m)
}

// ledgerSpy records what reached the ledger, in the order it arrived.
type ledgerSpy struct {
	mu       sync.Mutex
	arrivals []string
}

func (s *ledgerSpy) note(arrival string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arrivals = append(s.arrivals, arrival)
}

func (s *ledgerSpy) RecordRace(outcome engine.RaceOutcome) { s.note("race " + outcome.RequestID) }

func (s *ledgerSpy) RecordGhost(escrowID string, nonce uint64, reason string) {
	s.note(fmt.Sprintf("ghost %s %d %s", escrowID, nonce, reason))
}

func (s *ledgerSpy) RecordTimeout(vote engine.TimeoutEvent) {
	s.note(fmt.Sprintf("timeout %s %d %s", vote.EscrowID, vote.Nonce, vote.Action))
}

func (s *ledgerSpy) arrived() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.arrivals...)
}

// heldLedger parks the consumer inside its first race until released, so a test can fill the queue behind it.
type heldLedger struct {
	ledgerSpy
	entered chan struct{}
	release chan struct{}
	holding sync.Once
}

func newHeldLedger() *heldLedger {
	return &heldLedger{entered: make(chan struct{}), release: make(chan struct{})}
}

func (l *heldLedger) RecordRace(outcome engine.RaceOutcome) {
	l.holding.Do(func() {
		close(l.entered)
		<-l.release
	})
	l.ledgerSpy.RecordRace(outcome)
}

func newJournal(t *testing.T, settings Settings) *Journal {
	t.Helper()
	created := New(settings)
	t.Cleanup(func() { _ = created.Close() })
	return created
}

// holdConsumer parks the consumer and returns its release; cleanup releases it before the journal closes.
func holdConsumer(t *testing.T, events *Journal, ledger *heldLedger) func() {
	t.Helper()
	release := sync.OnceFunc(func() { close(ledger.release) })
	t.Cleanup(release)
	events.RecordRace(engine.RaceOutcome{RequestID: "holding"})
	select {
	case <-ledger.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer never took the first race")
	}
	return release
}

// The ledger applies facts in the order they happened, whichever producer reported them.
func TestTheLedgerReceivesFactsInTheOrderTheyWereRecorded(t *testing.T) {
	ledger := &ledgerSpy{}
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: ledger})

	events.RecordRace(engine.RaceOutcome{RequestID: "request-1"})
	events.GhostBurned("escrow-1", scheduler.Burn{Nonce: 5, Reason: "participant_throttled_no_send"})
	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "escrow-1", Nonce: 4, Action: engine.TimeoutActionCompleted})
	events.Flush()

	require.Equal(t, []string{
		"race request-1",
		"ghost escrow-1 5 participant_throttled_no_send",
		"timeout escrow-1 4 completed",
	}, ledger.arrived())
}

func TestAJournalWithoutALedgerAcceptsEveryFact(t *testing.T) {
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}})

	events.RecordRace(engine.RaceOutcome{RequestID: "request-1"})
	events.GhostBurned("escrow-1", scheduler.Burn{Nonce: 5})
	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "escrow-1"})
	events.Flush()

	require.NoError(t, events.Close())
}

// A money-lane event is refused only past the ceiling, the one the consumer holds included, and the refusal is counted and returned by Close.
func TestAMoneyFactPastTheCeilingIsRefusedCountedAndReported(t *testing.T) {
	ledger := newHeldLedger()
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: ledger, MoneyCeiling: 3})
	release := holdConsumer(t, events, ledger)

	events.RecordRace(engine.RaceOutcome{RequestID: "request-1"})
	events.RecordRace(engine.RaceOutcome{RequestID: "request-2"})
	events.RecordRace(engine.RaceOutcome{RequestID: "request-3"})
	release()
	events.Flush()

	require.Equal(t, []string{"race holding", "race request-1", "race request-2"}, ledger.arrived())
	moneyRefused, _, _ := events.Counts()
	require.Equal(t, uint64(1), moneyRefused)
	err := events.Close()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "refused 1 money-lane events"), "Close() = %v", err)
}

// A progress line past the backlog is dropped and counted, and the batch after the drop says how many.
func TestAProgressLinePastTheBacklogIsDroppedAndTheNextBatchSaysHowMany(t *testing.T) {
	lines := &logcapture.Recorder{}
	ledger := newHeldLedger()
	events := newJournal(t, Settings{Lines: lines, Ledger: ledger, ProgressBacklog: 1})
	release := holdConsumer(t, events, ledger)

	events.emit(queuedEvent{kind: KindNonceCommitted})
	events.emit(queuedEvent{kind: KindNonceCommitted})
	events.emit(queuedEvent{kind: KindNonceCommitted})
	release()
	events.Flush()

	_, progressDropped, _ := events.Counts()
	require.Equal(t, uint64(2), progressDropped)
	lines.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "journal skipped progress lines", Fields: []any{
		"skipped_lines", uint64(2),
	}})
}

// Close waits for everything already accepted, so the ledger closing after it holds every fact.
func TestCloseDrainsWhatWasAcceptedBeforeIt(t *testing.T) {
	ledger := newHeldLedger()
	events := newJournal(t, Settings{Lines: &logcapture.Recorder{}, Ledger: ledger})
	release := holdConsumer(t, events, ledger)
	events.RecordRace(engine.RaceOutcome{RequestID: "request-1"})
	release()

	require.NoError(t, events.Close())

	require.Equal(t, []string{"race holding", "race request-1"}, ledger.arrived())
}

// An event after Close reaches no sink; it is counted, and the first of each kind is written as an error.
func TestAnEventAfterCloseIsCountedAndItsKindWrittenOnce(t *testing.T) {
	lines := &logcapture.Recorder{}
	ledger := &ledgerSpy{}
	events := newJournal(t, Settings{Lines: lines, Ledger: ledger})
	require.NoError(t, events.Close())

	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "escrow-1", Nonce: 4})
	events.RecordTimeout(engine.TimeoutEvent{EscrowID: "escrow-1", Nonce: 5})
	events.GhostBurned("escrow-1", scheduler.Burn{Nonce: 6})

	require.Empty(t, ledger.arrived())
	_, _, lateEvents := events.Counts()
	require.Equal(t, uint64(3), lateEvents)
	require.Equal(t, []logcapture.Entry{
		{Level: "error", Msg: "journal received an event after it closed", Fields: []any{"kind", "timeout_vote"}},
		{Level: "error", Msg: "journal received an event after it closed", Fields: []any{"kind", "nonce_burned"}},
	}, lines.All())
}

func TestEveryKindHasANameAndTheLaneTheSpecAssigns(t *testing.T) {
	moneyLane := map[Kind]bool{
		KindRaceReported: true, KindTimeoutVote: true, KindNonceBurned: true, KindBurnBudgetExhausted: true,
		KindDiffComposed: true, KindWarmupProbe: true, KindNonceStranded: true, KindHostDiverged: true,
		KindReplyNotCached: true,
	}
	for kind := KindRaceReported; kind < kindCount; kind++ {
		require.NotEqual(t, "unknown", kind.String(), "kind %d has no name", kind)
		require.Equal(t, moneyLane[kind], kind.onMoneyLane(), "kind %s is on the wrong lane", kind)
	}
}
