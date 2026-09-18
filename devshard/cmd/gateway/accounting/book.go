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
	costs       map[uint64]nonceCost
	folded      map[uint32]SlotMoney
	produced    map[uint32]uint64
	events      []protocolEvent
	retired     bool
}

// The only way to build one: a second site that forgot a map would panic on a path with no error to return.
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
		costs:       make(map[uint64]nonceCost),
		folded:      make(map[uint32]SlotMoney),
		produced:    make(map[uint32]uint64),
	}
}

type nonceRecord struct {
	requestID    string
	sent         bool
	finished     bool
	acknowledged bool
	isCounted    bool
	outputAdded  bool
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

	countedAs CounterKey
}

func NewBook(now func() time.Time) *Book {
	if now == nil {
		now = time.Now
	}
	return &Book{escrows: make(map[string]*escrowLedger), now: now}
}

// Re-opening keeps the counters and pins the epoch at first sighting. See README.md, "How a nonce is classified".
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
		if _, seen := escrow.hostStats[slotID]; !seen && escrow.timeouts[slotID] == 0 {
			escrow.timeouts[slotID] = uint64(stats.Missed)
		}
		escrow.hostStats[slotID] = stats
		return nil
	})
}

// ObserveInferences replaces an escrow's per-nonce money and its open-challenge counts from one sweep.
func (b *Book) ObserveInferences(escrowID string, inferences map[uint64]*types.InferenceRecord) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		clear(escrow.folded)
		challenged := make(map[uint32]uint64)
		for nonce, record := range inferences {
			if record == nil {
				continue
			}
			escrow.costs[nonce] = nonceCost{
				reserved:    record.ReservedCost,
				actual:      record.ActualCost,
				inputLength: record.InputLength,
				maxTokens:   record.MaxTokens,
				input:       record.InputTokens,
				output:      record.OutputTokens,
				status:      record.Status,
			}
			if record.Status == types.StatusChallenged {
				challenged[record.ExecutorSlot]++
			}
		}
		escrow.challenged = challenged
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

func (b *Book) RecordAssigned(escrowID string, nonce uint64, requestID string) error {
	return b.withEscrow(escrowID, func(escrow *escrowLedger) error {
		record := escrow.record(nonce)
		record.sent = true
		if record.requestID == "" {
			record.requestID = requestID
		}
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
			escrow.addProduced(attempt.Nonce, record, attempt.OutputTokens)
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
	var unfinished []uint64
	for nonce, record := range escrow.nonces {
		if record.isCounted && !record.finished && revisable(record) {
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

func (b *Book) withEscrow(escrowID string, apply func(*escrowLedger) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	escrow, known := b.escrows[escrowID]
	if !known {
		return fmt.Errorf("%w: %s", ErrUnknownEscrow, escrowID)
	}
	if err := apply(escrow); err != nil {
		return err
	}
	b.touchLocked()
	return nil
}

func (b *Book) touchLocked() { b.updatedAt = b.now().UTC() }

// Seeing a nonce raises the assigned watermark too, or the counters outrun the range they are measured against.
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

func (e *escrowLedger) moneyBySlot() map[uint32]SlotMoney {
	folded := e.foldMoney()
	money := make(map[uint32]SlotMoney, len(folded))
	for slotID, slot := range folded {
		if slot != (SlotMoney{}) {
			money[uint32(slotID)] = slot
		}
	}
	return money
}
