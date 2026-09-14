// Package journal is the one place a lifecycle step becomes a log line or a nonce ledger fact. See README.md.
package journal

import (
	"fmt"
	"sync"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
)

const (
	defaultMoneyCeiling    = 100_000
	defaultProgressBacklog = 8_192

	// retainedBatchCapacity keeps one burst's buffer from being held for the life of the process.
	retainedBatchCapacity = 4_096

	skippedProgressMessage = "journal skipped progress lines"
	lateEventMessage       = "journal received an event after it closed"
)

type logSink interface {
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
}

type ledgerSink interface {
	RecordRace(outcome engine.RaceOutcome)
	RecordGhost(escrowID string, nonce uint64, reason string)
	RecordTimeout(event engine.TimeoutEvent)
}

// Settings wires a journal: zero limits take the defaults, and a nil Ledger means nonce accounting is off.
type Settings struct {
	Lines           logSink
	Ledger          ledgerSink
	MoneyCeiling    int
	ProgressBacklog int
}

// Journal hands every lifecycle step to its sinks in order, from one consumer goroutine. See README.md.
type Journal struct {
	lines           logSink
	ledger          ledgerSink
	moneyCeiling    int
	progressBacklog int
	done            chan struct{}

	mu               sync.Mutex
	arrived          *sync.Cond
	drained          *sync.Cond
	pending          []queuedEvent
	spare            []queuedEvent
	pendingMoney     int
	pendingProgress  int
	accepted         uint64
	delivered        uint64
	skippedSinceLine uint64
	skippedWritten   uint64
	closed           bool
	lateKinds        [kindCount]bool

	moneyRefused    uint64
	progressDropped uint64
	lateEvents      uint64
}

// gatewayLog writes through devshard/logging, so internal/logcapture sees every line.
type gatewayLog struct{}

func (gatewayLog) Info(msg string, keyvals ...any)  { logging.Info(msg, keyvals...) }
func (gatewayLog) Warn(msg string, keyvals ...any)  { logging.Warn(msg, keyvals...) }
func (gatewayLog) Error(msg string, keyvals ...any) { logging.Error(msg, keyvals...) }

// New starts the consumer; Close stops it.
func New(settings Settings) *Journal {
	created := &Journal{
		lines:           settings.Lines,
		ledger:          settings.Ledger,
		moneyCeiling:    settings.MoneyCeiling,
		progressBacklog: settings.ProgressBacklog,
		done:            make(chan struct{}),
	}
	if created.lines == nil {
		created.lines = gatewayLog{}
	}
	if created.moneyCeiling <= 0 {
		created.moneyCeiling = defaultMoneyCeiling
	}
	if created.progressBacklog <= 0 {
		created.progressBacklog = defaultProgressBacklog
	}
	created.arrived = sync.NewCond(&created.mu)
	created.drained = sync.NewCond(&created.mu)
	go created.run()
	return created
}

// Flush returns once every event accepted and every drop counted before it has reached the sinks; never call it from a sink.
func (j *Journal) Flush() {
	j.mu.Lock()
	defer j.mu.Unlock()
	acceptedBefore, droppedBefore := j.accepted, j.progressDropped
	for j.delivered < acceptedBefore || j.skippedWritten < droppedBefore {
		j.drained.Wait()
	}
}

// Close refuses later events, drains the accepted ones, and reports money-lane events refused over the journal's life.
func (j *Journal) Close() error {
	j.mu.Lock()
	j.closed = true
	j.arrived.Signal()
	j.mu.Unlock()
	<-j.done

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.moneyRefused > 0 {
		return fmt.Errorf("journal refused %d money-lane events past its ceiling of %d", j.moneyRefused, j.moneyCeiling)
	}
	return nil
}

// Counts is what the journal refused, dropped and received after Close.
func (j *Journal) Counts() (moneyRefused, progressDropped, lateEvents uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.moneyRefused, j.progressDropped, j.lateEvents
}

// RecordRace hands a race's outcome to the ledger; its attempts are shared with the response path and never written.
func (j *Journal) RecordRace(outcome engine.RaceOutcome) {
	j.emit(queuedEvent{kind: KindRaceReported, race: &outcome})
}

// RecordTimeout hands a race's timeout vote to the ledger.
func (j *Journal) RecordTimeout(vote engine.TimeoutEvent) {
	j.emit(queuedEvent{kind: KindTimeoutVote, timeout: &vote})
}

// GhostBurned hands a nonce the scheduler spent on nobody to the ledger.
func (j *Journal) GhostBurned(escrowID string, burned scheduler.Burn) {
	j.emit(queuedEvent{kind: KindNonceBurned, escrowID: escrowID, burn: burned})
}

// emit admits one event to its lane and never waits. See README.md, "Two lanes".
func (j *Journal) emit(entry queuedEvent) {
	j.mu.Lock()
	switch moneyLane := entry.kind.onMoneyLane(); {
	case j.closed:
		j.lateEvents++
		firstOfKind := !j.lateKinds[entry.kind]
		j.lateKinds[entry.kind] = true
		j.mu.Unlock()
		if firstOfKind {
			j.lines.Error(lateEventMessage, logkey.Kind, entry.kind.String())
		}
		return
	case moneyLane && j.pendingMoney >= j.moneyCeiling:
		j.moneyRefused++
	case !moneyLane && j.pendingProgress >= j.progressBacklog:
		j.progressDropped++
		j.skippedSinceLine++
	default:
		j.pending = append(j.pending, entry)
		j.accepted++
		if moneyLane {
			j.pendingMoney++
		} else {
			j.pendingProgress++
		}
		j.arrived.Signal()
	}
	j.mu.Unlock()
}

// run is the only goroutine that reaches a sink, so the sinks see events in the order they were accepted.
func (j *Journal) run() {
	defer close(j.done)
	for {
		batch, skipped, open := j.takeBatch()
		if !open {
			return
		}
		moneyEvents := 0
		for index := range batch {
			if batch[index].kind.onMoneyLane() {
				moneyEvents++
			}
			j.deliver(&batch[index])
			batch[index] = queuedEvent{}
		}
		if skipped > 0 {
			j.lines.Warn(skippedProgressMessage, logkey.SkippedLines, skipped)
		}
		j.finishBatch(batch, moneyEvents, skipped)
	}
}

// takeBatch waits for events or an unwritten drop and swaps the queue out, so producers keep appending while a batch is delivered.
func (j *Journal) takeBatch() (batch []queuedEvent, skipped uint64, open bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for len(j.pending) == 0 && j.skippedSinceLine == 0 && !j.closed {
		j.arrived.Wait()
	}
	if len(j.pending) == 0 && j.skippedSinceLine == 0 {
		return nil, 0, false
	}
	batch, j.pending, j.spare = j.pending, j.spare, nil
	skipped, j.skippedSinceLine = j.skippedSinceLine, 0
	return batch, skipped, true
}

// finishBatch releases the batch from its lanes only now, so a lane bounds what is accepted and not yet delivered.
func (j *Journal) finishBatch(batch []queuedEvent, moneyEvents int, skipped uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.delivered += uint64(len(batch))
	j.skippedWritten += skipped
	j.pendingMoney -= moneyEvents
	j.pendingProgress -= len(batch) - moneyEvents
	if cap(batch) <= retainedBatchCapacity {
		j.spare = batch[:0]
	}
	j.drained.Broadcast()
}

// deliver is the one place an event reaches a sink. See README.md, "Kinds".
func (j *Journal) deliver(entry *queuedEvent) {
	switch entry.kind {
	case KindRaceReported:
		if j.ledger != nil {
			j.ledger.RecordRace(*entry.race)
		}
	case KindTimeoutVote:
		if j.ledger != nil {
			j.ledger.RecordTimeout(*entry.timeout)
		}
	case KindNonceBurned:
		if j.ledger != nil {
			j.ledger.RecordGhost(entry.escrowID, entry.burn.Nonce, entry.burn.Reason)
		}
	}
}
