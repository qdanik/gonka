package api

import (
	"bytes"
	"cmp"
	"net/http"
	"sync"

	"devshard/cmd/gateway/filters"
)

type streamLog struct {
	mu       sync.Mutex
	id       string
	key      cacheKey
	model    string
	limit    int64
	budget   *BufferBudget
	index    *streamIndex
	results  StreamEvents
	escrowID string

	body        []byte
	bounds      []int
	state       streamLogState
	charged     int64
	dropped     bool
	dropRefusal string

	hasSocket  bool
	reading    bool
	generation uint64
	orphan     bool
	wasOrphan  bool
	changed    chan struct{}
}

func newStreamLog(id string, key cacheKey, model string, limit int64, budget *BufferBudget, index *streamIndex, results StreamEvents) *streamLog {
	log := &streamLog{
		id: id, key: key, model: model, limit: limit, budget: budget, index: index, results: results,
		bounds:    make([]int, 0, expectedStreamEvents),
		hasSocket: true,
		changed:   make(chan struct{}),
	}
	index.add(log)
	return log
}

func (l *streamLog) noteEscrow(escrowID string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.escrowID = escrowID
}

func (l *streamLog) escrow() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.escrowID
}

func (l *streamLog) eventCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.bounds)
}

func (l *streamLog) heldBytesLocked() int64 { return int64(len(l.body)) }

func (l *streamLog) socketAttached() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hasSocket
}

func (l *streamLog) append(data []byte) (socketAttached bool) {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == streamLive && len(data) > 0 {
		l.appendLocked(data)
	}
	return l.hasSocket
}

func (l *streamLog) appendLocked(data []byte) {
	size := int64(len(data))
	if int64(len(l.body))+size > l.limit || !l.budget.reserve(size) {
		l.dropLocked(orphanDroppedBudget)
		return
	}
	if l.orphan && !l.index.growOrphan(l, size) {
		l.budget.release(size)
		l.dropLocked(orphanDroppedBudget)
		return
	}
	l.charged += size
	l.body = append(l.body, data...)
	l.cutEventsLocked()
	l.wakeLocked()
}

func (l *streamLog) cutEventsLocked() {
	from := 0
	if len(l.bounds) > 0 {
		from = l.bounds[len(l.bounds)-1]
	}
	for {
		at := bytes.Index(l.body[from:], sseEventSeparator)
		if at < 0 {
			return
		}
		from += at + len(sseEventSeparator)
		l.bounds = append(l.bounds, from)
	}
}

func (l *streamLog) sealTailLocked() {
	end := len(l.body)
	if end == 0 || (len(l.bounds) > 0 && l.bounds[len(l.bounds)-1] == end) {
		return
	}
	l.bounds = append(l.bounds, end)
}

func (l *streamLog) wakeLocked() {
	close(l.changed)
	l.changed = make(chan struct{})
}

func (l *streamLog) dropLocked(result string) {
	if l.state != streamLive {
		return
	}
	l.state, l.dropped = streamFailed, true
	l.dropRefusal = cacheRefusedTooLarge
	if l.orphan {
		l.dropRefusal = cacheRefusedOrphanDropped
	}
	l.index.remove(l)
	l.budget.release(l.charged)
	l.charged = 0
	l.body, l.bounds = nil, nil
	l.countOrphanLocked(result)
	l.wakeLocked()
}

func (l *streamLog) countOrphanLocked(result string) {
	if l.wasOrphan && l.results != nil {
		l.results.OrphanOutcome(l.model, result)
	}
}

func (l *streamLog) drop(result string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropLocked(result)
}

func (l *streamLog) evict() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.orphan && !l.reading {
		l.dropLocked(orphanDroppedCap)
	}
}

func (l *streamLog) conclude(escrowID string, unstorable error) (cachedResponse, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dropped {
		return cachedResponse{}, l.dropRefusal
	}
	l.sealTailLocked()
	escrowID = cmp.Or(escrowID, l.escrowID)
	refusal := replayRefusal(false, nil, unstorable, len(l.body), escrowID)
	l.state = streamFinished
	if refusal != filters.CacheStorable {
		l.state = streamFailed
	}
	l.index.remove(l)
	l.budget.release(l.charged)
	l.charged = 0
	l.wakeLocked()
	if refusal != filters.CacheStorable {
		l.countOrphanLocked(orphanFailed)
		return cachedResponse{}, refusal
	}
	l.countOrphanLocked(orphanStored)
	return cachedResponse{
		escrowID:    escrowID,
		stream:      true,
		status:      http.StatusOK,
		contentType: contentTypeEventStream,
		body:        exactly(l.body),
		bounds:      exactly(l.bounds),
		streamID:    l.id,
	}, filters.CacheStorable
}

func (l *streamLog) detachSocket() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if !l.hasSocket {
		l.mu.Unlock()
		return
	}
	l.hasSocket = false
	evicted := l.orphanIfIdleLocked()
	l.mu.Unlock()
	evicted.evict()
}

func (l *streamLog) orphanIfIdleLocked() (evicted *streamLog) {
	if l.state != streamLive || l.hasSocket || l.reading || l.orphan {
		return nil
	}
	l.orphan, l.wasOrphan = true, true
	evicted, admitted := l.index.registerOrphan(l, l.heldBytesLocked())
	if !admitted {
		l.dropLocked(orphanDroppedBudget)
	}
	return evicted
}

type logReader struct {
	log        *streamLog
	generation uint64
	sent       int
}

func (l *streamLog) attach(from int, takeover bool) (*logReader, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dropped || (!takeover && !l.orphan) {
		return nil, false
	}
	if l.orphan {
		l.orphan = false
		l.index.unregisterOrphan(l)
		if !takeover {
			l.countOrphanLocked(orphanAdopted)
		}
	}
	l.hasSocket, l.reading = false, true
	l.generation++
	l.wakeLocked()
	return &logReader{log: l, generation: l.generation, sent: from}, true
}

func (r *logReader) next() (events []byte, done, dropped bool, changed <-chan struct{}) {
	l := r.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.generation != r.generation {
		return nil, true, false, nil
	}
	if len(l.bounds) > r.sent {
		start := 0
		if r.sent > 0 {
			start = l.bounds[r.sent-1]
		}
		events = l.body[start:l.bounds[len(l.bounds)-1]]
		r.sent = len(l.bounds)
		return events, false, false, nil
	}
	return nil, l.state != streamLive, l.dropped, l.changed
}

func (r *logReader) release() {
	l := r.log
	l.mu.Lock()
	var evicted *streamLog
	if l.generation == r.generation {
		l.reading = false
		evicted = l.orphanIfIdleLocked()
	}
	l.mu.Unlock()
	evicted.evict()
}
