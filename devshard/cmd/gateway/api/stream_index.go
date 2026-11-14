package api

import "sync"

// StreamEvents counts what became of streams whose client left and how explicit resumes were answered; *metrics.StreamResumeRecorder satisfies it.
type StreamEvents interface {
	OrphanOutcome(model, result string)
	Resume(model, result string)
}

const orphansDisabled = 0

type streamIndex struct {
	mu               sync.Mutex
	logs             map[cacheKey][]*streamLog
	orphans          map[cacheKey][]*streamLog
	orphanSizes      map[*streamLog]int64
	orphanBytes      int64
	maxOrphanBytes   int64
	maxOrphansPerKey int
}

func newStreamIndex(maxOrphanBytes int64, maxOrphansPerKey int) *streamIndex {
	return &streamIndex{
		logs:             make(map[cacheKey][]*streamLog),
		orphans:          make(map[cacheKey][]*streamLog),
		orphanSizes:      make(map[*streamLog]int64),
		maxOrphanBytes:   maxOrphanBytes,
		maxOrphansPerKey: maxOrphansPerKey,
	}
}

func (x *streamIndex) tune(maxOrphanBytes int64, maxOrphansPerKey int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.maxOrphanBytes, x.maxOrphansPerKey = maxOrphanBytes, maxOrphansPerKey
}

func (x *streamIndex) stats() (orphans, bytes int64) {
	if x == nil {
		return 0, 0
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return int64(len(x.orphanSizes)), x.orphanBytes
}

func (x *streamIndex) add(log *streamLog) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.logs[log.key] = append(x.logs[log.key], log)
}

func (x *streamIndex) remove(log *streamLog) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.logs[log.key] = without(x.logs[log.key], log)
	if len(x.logs[log.key]) == 0 {
		delete(x.logs, log.key)
	}
	x.unregisterOrphanLocked(log)
}

func (x *streamIndex) find(key cacheKey, id string) *streamLog {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, log := range x.logs[key] {
		if log.id == id {
			return log
		}
	}
	return nil
}

func (x *streamIndex) newestFirst(key cacheKey) []*streamLog {
	x.mu.Lock()
	defer x.mu.Unlock()
	held := x.logs[key]
	reversed := make([]*streamLog, len(held))
	for i, log := range held {
		reversed[len(held)-1-i] = log
	}
	return reversed
}

func (x *streamIndex) registerOrphan(log *streamLog, bodyBytes int64) (evicted *streamLog, admitted bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.maxOrphanBytes == orphansDisabled || x.orphanBytes+bodyBytes > x.maxOrphanBytes {
		return nil, false
	}
	held := x.orphans[log.key]
	held = append(held, log)
	if len(held) > x.maxOrphansPerKey {
		evicted, held = held[0], held[1:]
		x.orphanBytes -= x.orphanSizes[evicted]
		delete(x.orphanSizes, evicted)
	}
	x.orphans[log.key] = held
	x.orphanSizes[log] = bodyBytes
	x.orphanBytes += bodyBytes
	return evicted, true
}

func (x *streamIndex) unregisterOrphan(log *streamLog) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.unregisterOrphanLocked(log)
}

func (x *streamIndex) unregisterOrphanLocked(log *streamLog) {
	size, held := x.orphanSizes[log]
	if !held {
		return
	}
	delete(x.orphanSizes, log)
	x.orphanBytes -= size
	remaining := without(x.orphans[log.key], log)
	if len(remaining) == 0 {
		delete(x.orphans, log.key)
		return
	}
	x.orphans[log.key] = remaining
}

func (x *streamIndex) growOrphan(log *streamLog, bytes int64) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.orphanBytes+bytes > x.maxOrphanBytes {
		return false
	}
	x.orphanSizes[log] += bytes
	x.orphanBytes += bytes
	return true
}

func without(logs []*streamLog, removed *streamLog) []*streamLog {
	for i, log := range logs {
		if log == removed {
			return append(logs[:i:i], logs[i+1:]...)
		}
	}
	return logs
}
