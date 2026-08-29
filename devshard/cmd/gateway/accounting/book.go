package accounting

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"devshard/types"
)

var ErrUnknownEscrow = errors.New("accounting: unknown escrow")

type Book struct {
	mu        sync.RWMutex
	escrows   map[string]*escrowLedger
	updatedAt time.Time
	now       func() time.Time

	rejected uint64
}

type escrowLedger struct {
	metadata    EscrowMetadata
	latest      uint64
	hostStats   map[uint32]types.HostStats
	challenged  map[uint32]uint64
	validations map[uint32]uint64
	timeouts    map[uint32]uint64
	rejected    map[uint32]uint64
	counters    map[CounterKey]uint64
	nonces      map[uint64]*nonceRecord
	events      []protocolEvent
	retired     bool
	retiredAt   time.Time
}

// newEscrowLedger is the only way to build one. A second construction site that forgot a map would
// panic on the first write to it, and the write is a recording path with no error to return.
func newEscrowLedger(metadata EscrowMetadata) *escrowLedger {
	return &escrowLedger{
		metadata:    metadata,
		hostStats:   make(map[uint32]types.HostStats),
		challenged:  make(map[uint32]uint64),
		validations: make(map[uint32]uint64),
		timeouts:    make(map[uint32]uint64),
		rejected:    make(map[uint32]uint64),
		counters:    make(map[CounterKey]uint64),
		nonces:      make(map[uint64]*nonceRecord),
	}
}

type nonceRecord struct {
	requestID    string
	sent         bool
	finished     bool
	acknowledged bool
	usage        Usage
	ghostReason  string

	timeoutKind   string
	timeoutAction string
	timeoutReason string

	terminal        string
	phase           Phase
	slowReceipt     bool
	slowChunk       bool
	slowDecode      bool
	clockDrifted    bool
	logprobsDecoded bool

	counted *CounterKey
}

func NewBook(now func() time.Time) *Book {
	if now == nil {
		now = time.Now
	}
	return &Book{escrows: make(map[string]*escrowLedger), now: now}
}

// Re-opening keeps the counters and pins the epoch at first sighting: an escrow that re-stamped
// itself on every sweep would drag its whole history into the current epoch.
func (b *Book) OpenEscrow(metadata EscrowMetadata) error {
	if metadata.EscrowID == "" {
		return fmt.Errorf("accounting: escrow id is required")
	}
	if len(metadata.Slots) == 0 {
		return fmt.Errorf("accounting: escrow %s has no slots", metadata.EscrowID)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, known := b.escrows[metadata.EscrowID]; known {
		pinnedEpoch := existing.metadata.CreationEpoch
		existing.metadata = metadata
		existing.metadata.CreationEpoch = pinnedEpoch
		existing.retired = false
		b.touchLocked()
		return nil
	}
	b.escrows[metadata.EscrowID] = newEscrowLedger(metadata)
	b.touchLocked()
	return nil
}

// ResetEpoch drops what the ledger holds for one epoch and reports how many escrows went with it.
func (b *Book) ResetEpoch(epoch uint64) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	cleared := 0
	for escrowID, escrow := range b.escrows {
		if escrow.metadata.CreationEpoch == epoch {
			delete(b.escrows, escrowID)
			cleared++
		}
	}
	if cleared > 0 {
		b.touchLocked()
	}
	return cleared
}

func (b *Book) RetireEscrow(escrowID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if escrow, known := b.escrows[escrowID]; known {
		escrow.retired = true
		escrow.retiredAt = b.now().UTC()
		b.touchLocked()
	}
}

func (b *Book) ObserveLatestNonce(escrowID string, nonce uint64) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		if nonce > escrow.latest {
			escrow.latest = nonce
		}
		return nil
	})
}

func (b *Book) ObserveHostStats(escrowID string, slotID uint32, stats types.HostStats) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		escrow.hostStats[slotID] = stats
		return nil
	})
}

func (b *Book) ObserveChallenges(escrowID string, open map[uint32]uint64) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		escrow.challenged = open
		return nil
	})
}

func (b *Book) RecordValidation(escrowID string, validatorSlot uint32) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		if int(validatorSlot) >= len(escrow.metadata.Slots) {
			return fmt.Errorf("slot %d out of range", validatorSlot)
		}
		escrow.validations[validatorSlot]++
		return nil
	})
}

func (b *Book) RecordAppliedTimeout(escrowID string, nonce uint64) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		escrow.timeouts[escrow.slotOf(nonce)]++
		escrow.appendEvent(nonce, ProtocolTimeoutApplied, b.now())
		return nil
	})
}

func (b *Book) RecordInvalidVerdict(escrowID string, nonce uint64) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		escrow.rejected[escrow.slotOf(nonce)]++
		escrow.appendEvent(nonce, ProtocolInvalidated, b.now())
		return nil
	})
}

func (b *Book) RecordGhost(escrowID string, nonce uint64, reason string) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		record := escrow.record(nonce)
		record.ghostReason = reason
		escrow.reclassify(nonce, record)
		return nil
	})
}

func (b *Book) RecordRace(escrowID string, attempts []Attempt) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		for _, attempt := range attempts {
			record := escrow.record(attempt.Nonce)
			record.requestID = attempt.RequestID
			record.sent = attempt.Sent
			record.finished = attempt.Finished
			record.acknowledged = attempt.Acknowledged
			record.usage = attempt.Usage
			record.terminal = attempt.Terminal
			record.phase = attempt.Phase
			record.slowReceipt = attempt.SlowReceipt
			record.slowChunk = attempt.SlowChunk
			record.clockDrifted = attempt.ClockDrifted
			record.slowDecode = attempt.SlowDecode
			record.logprobsDecoded = attempt.LogprobsDecoded
			escrow.reclassify(attempt.Nonce, record)
		}
		return nil
	})
}

func (b *Book) RecordTimeout(escrowID string, nonce uint64, kind, action, reason string) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		record := escrow.record(nonce)
		if kind != "" {
			record.timeoutKind = kind
		}
		record.timeoutAction = action
		record.timeoutReason = reason
		escrow.reclassify(nonce, record)
		return nil
	})
}

func (b *Book) UnfinishedNonces(escrowID string) []uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	escrow, known := b.escrows[escrowID]
	if !known {
		return nil
	}
	unfinished := make([]uint64, 0, len(escrow.nonces))
	for nonce, record := range escrow.nonces {
		if record.counted != nil && !record.finished && revisable(record) {
			unfinished = append(unfinished, nonce)
		}
	}
	slices.Sort(unfinished)
	return unfinished
}

func (b *Book) MarkFinished(escrowID string, nonces []uint64) error {
	if len(nonces) == 0 {
		return nil
	}
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		for _, nonce := range nonces {
			record, known := escrow.nonces[nonce]
			if !known || record.finished {
				continue
			}
			record.finished = true
			escrow.reclassify(nonce, record)
		}
		return nil
	})
}

func (b *Book) Rejected() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.rejected
}

func (b *Book) withEscrow(escrowID string, apply func(*escrowLedger) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	escrow, known := b.escrows[escrowID]
	if !known {
		b.rejected++
		return fmt.Errorf("%w: %s", ErrUnknownEscrow, escrowID)
	}
	if err := apply(escrow); err != nil {
		return err
	}
	b.touchLocked()
	return nil
}

func (b *Book) touchLocked() { b.updatedAt = b.now().UTC() }

// Seeing a nonce raises the assigned watermark too: the chain's latest is polled on a sweep while
// nonces arrive on every race, and without this the counters outrun the range they are measured
// against and report the gap as a disagreement with the chain.
func (e *escrowLedger) record(nonce uint64) *nonceRecord {
	if nonce > e.latest {
		e.latest = nonce
	}
	existing, known := e.nonces[nonce]
	if known {
		return existing
	}
	fresh := &nonceRecord{usage: UsageUnknown}
	e.nonces[nonce] = fresh
	return fresh
}

func (e *escrowLedger) slotOf(nonce uint64) uint32 {
	return uint32(nonce % uint64(len(e.metadata.Slots)))
}

func (e *escrowLedger) reclassify(nonce uint64, record *nonceRecord) {
	key, settled := classify(e.slotOf(nonce), record)
	if record.counted != nil {
		if settled && *record.counted == key {
			return
		}
		e.counters[*record.counted]--
		if e.counters[*record.counted] == 0 {
			delete(e.counters, *record.counted)
		}
		record.counted = nil
	}
	if !settled {
		return
	}
	e.counters[key]++
	counted := key
	record.counted = &counted
}

func classify(slotID uint32, record *nonceRecord) (CounterKey, bool) {
	key := CounterKey{SlotID: slotID}
	switch {
	case record.ghostReason != "":
		// A burn the host is charged for votes like any other nonce, so its outcome has to reach the key
		// the burn already made; without this the vote is posted and counted nowhere.
		key.Disposition = DispositionGhost
		key.GhostReason = record.ghostReason
		key.TimeoutKind = record.timeoutKind
		key.TimeoutAction = record.timeoutAction
		key.TimeoutReason = record.timeoutReason
		return key, true
	case record.finished:
		key.Disposition = finishedDisposition(record.usage)
		return raceFacts(key, record), true
	case !record.sent:
		// Committed and never dispatched. It is not a ghost -- nothing decided to burn it -- and it is
		// not in flight, because the race that held it has reported. It is an unfinished refusal: no
		// host ever saw it.
		if record.timeoutAction == "" {
			return key, false
		}
		key.Disposition = DispositionUnfinishedRefused
	case record.timeoutAction == "":
		return key, false
	default:
		key.Disposition = unfinishedDisposition(record)
	}
	key.TimeoutKind = record.timeoutKind
	key.TimeoutAction = record.timeoutAction
	key.TimeoutReason = record.timeoutReason
	return raceFacts(key, record), true
}

// An empty terminal is itself a fact -- the nonce reached classification without the race ever
// reporting on it -- so it is named rather than left blank, which reads as missing data and drops the
// bucket from any grouping by terminal.
func raceFacts(key CounterKey, record *nonceRecord) CounterKey {
	key.Terminal = record.terminal
	if key.Terminal == "" {
		key.Terminal = TerminalUnreported
	}
	key.Phase = record.phase
	key.SlowReceipt = record.slowReceipt
	key.SlowChunk = record.slowChunk
	key.ClockDrifted = record.clockDrifted
	key.SlowDecode = record.slowDecode
	key.LogprobsDecoded = record.logprobsDecoded
	return key
}

func finishedDisposition(usage Usage) Disposition {
	switch usage {
	case UsageWinner:
		return DispositionFinishedUsed
	case UsageLoser:
		return DispositionFinishedUnused
	default:
		return DispositionFinishedUsageUnknown
	}
}

func unfinishedDisposition(record *nonceRecord) Disposition {
	switch {
	case record.timeoutKind == TimeoutKindRefused:
		return DispositionUnfinishedRefused
	case record.timeoutKind != "":
		return DispositionUnfinishedExecution
	case record.acknowledged:
		return DispositionUnfinishedExecution
	default:
		return DispositionUnfinishedRefused
	}
}

const TimeoutActionAbandoned = "abandoned_by_restart"

const TimeoutKindRefused = "refused"

const (
	TerminalUnreported   = "unreported"
	TerminalUnnamed      = "unnamed"
	TerminalUnclassified = "unclassified"
)

const (
	TerminalWarmupProbe     = "warmup_probe"
	TerminalClientGone      = "client_gone_before_delivery"
	TerminalClientCancelled = "client_cancelled"
)

func assignedForSlot(latest uint64, groupSize, slotID uint32) uint64 {
	if groupSize == 0 || latest == 0 {
		return 0
	}
	group, slot := uint64(groupSize), uint64(slotID)
	if slot == 0 {
		return latest / group
	}
	if latest < slot {
		return 0
	}
	return (latest-slot)/group + 1
}
