package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
)

const (
	testOrphanBytes   = 1 << 20
	testOrphansPerKey = 4
	testLogLimit      = 1 << 20
)

func testLog(index *streamIndex, id string, results StreamEvents, budget *BufferBudget, limit int64) *streamLog {
	return newStreamLog(id, cacheKey{model: "qwen"}, "qwen", limit, budget, index, results)
}

// Test flow:
//  1. Append bytes to a log in pieces that split an event, hold two events in one piece, and end on an unterminated event.
//  2. Assert the bounds fall after each blank line.
//  3. Conclude the log and assert the unterminated tail became the last event of the stored entry.
func TestALogCutsEventsAtBlankLinesAndSealsAnUnterminatedTail(t *testing.T) {
	log := testLog(newStreamIndex(testOrphanBytes, testOrphansPerKey), "request-1", nil, nil, testLogLimit)

	log.append([]byte("data: a\n"))
	log.append([]byte("\ndata: b\n\ndata: c\n\ndata: d"))

	if got, want := log.bounds, []int{9, 18, 27}; !slices.Equal(got, want) {
		t.Fatalf("bounds = %v, want %v", got, want)
	}
	entry, refusal := log.conclude("7", nil)
	if refusal != filters.CacheStorable {
		t.Fatalf("conclude refusal = %q, want storable", refusal)
	}
	if got, want := entry.bounds, []int{9, 18, 27, 34}; !slices.Equal(got, want) {
		t.Fatalf("entry bounds = %v, want %v", got, want)
	}
	if entry.streamID != "request-1" || !entry.stream || entry.escrowID != "7" {
		t.Fatalf("entry = %+v, want a stream entry under its id and escrow", entry)
	}
}

// Test flow:
//  1. Give a log a small limit and a shared budget, and append past it.
//  2. Assert the log dropped itself, returned what it held to the budget, and refuses to be stored.
func TestALogPastItsLimitIsDroppedAndReturnsItsBudget(t *testing.T) {
	budget := NewBufferBudget(1 << 20)
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, budget, 16)

	log.append([]byte("data: 1\n\n"))
	if budget.Held() == 0 {
		t.Fatal("a live log holds no budget")
	}
	log.append([]byte("data: 2\n\n"))

	if budget.Held() != 0 {
		t.Fatalf("budget holds %d after the log dropped, want 0", budget.Held())
	}
	if _, refusal := log.conclude("7", nil); refusal != cacheRefusedTooLarge {
		t.Fatalf("conclude refusal = %q, want %q", refusal, cacheRefusedTooLarge)
	}
	if index.find(log.key, "request-1") != nil {
		t.Fatal("a dropped log is still in the index")
	}
}

// Test flow:
//  1. Give a log the shared budget and append an event.
//  2. Conclude it as stored.
//  3. Assert the budget was returned and the log left the index.
func TestAConcludedLogHandsItsBudgetBack(t *testing.T) {
	budget := NewBufferBudget(1 << 20)
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, budget, testLogLimit)
	log.append([]byte("data: 1\n\n"))

	if _, refusal := log.conclude("7", nil); refusal != filters.CacheStorable {
		t.Fatalf("conclude refusal = %q, want storable", refusal)
	}

	if budget.Held() != 0 || index.find(log.key, "request-1") != nil {
		t.Fatalf("budget %d, indexed %v: a concluded log holds neither", budget.Held(), index.find(log.key, "request-1") != nil)
	}
}

// Test flow:
//  1. Conclude a log as a failed race, and one with no body, and one with no escrow.
//  2. Assert each is refused for its own reason.
func TestAConcludedLogIsRefusedForTheReasonItsRaceGives(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	failed := testLog(index, "request-1", nil, nil, testLogLimit)
	failed.append([]byte("data: 1\n\n"))
	empty := testLog(index, "request-2", nil, nil, testLogLimit)
	unpinned := testLog(index, "request-3", nil, nil, testLogLimit)
	unpinned.append([]byte("data: 1\n\n"))

	if _, refusal := failed.conclude("7", engine.ErrAllAttemptsFailed); refusal != cacheRefusedRaceFailed {
		t.Fatalf("failed race refusal = %q, want %q", refusal, cacheRefusedRaceFailed)
	}
	if _, refusal := empty.conclude("7", nil); refusal != filters.CacheRefusedEmptyBody {
		t.Fatalf("empty log refusal = %q, want %q", refusal, filters.CacheRefusedEmptyBody)
	}
	if _, refusal := unpinned.conclude("", nil); refusal != cacheRefusedNoEscrow {
		t.Fatalf("unpinned log refusal = %q, want %q", refusal, cacheRefusedNoEscrow)
	}
}

// Test flow:
//  1. Orphan three logs of one key under a limit of two per key.
//  2. Assert the oldest was dropped and counted as `dropped_cap`, and the other two are held as orphans.
func TestAnOrphanPastTheKeyLimitDropsTheOldest(t *testing.T) {
	results := newSpyStreamEvents()
	index := newStreamIndex(testOrphanBytes, 2)
	logs := []*streamLog{
		testLog(index, "request-1", results, nil, testLogLimit),
		testLog(index, "request-2", results, nil, testLogLimit),
		testLog(index, "request-3", results, nil, testLogLimit),
	}

	for _, log := range logs {
		log.append([]byte("data: 1\n\n"))
		log.detachSocket()
	}

	if !logs[0].dropped || logs[1].dropped || logs[2].dropped {
		t.Fatalf("dropped = %v %v %v, want only the oldest", logs[0].dropped, logs[1].dropped, logs[2].dropped)
	}
	if got := results.seenOutcomes(); len(got) != 1 || got[0] != "qwen/"+orphanDroppedCap {
		t.Fatalf("outcomes = %v, want one %s", got, orphanDroppedCap)
	}
	if orphans, _ := index.stats(); orphans != 2 {
		t.Fatalf("orphans = %d, want 2", orphans)
	}
}

// Test flow:
//  1. Orphan a log under a byte bound, then append past the bound.
//  2. Assert the log dropped itself as `dropped_budget` and the index holds no orphan bytes.
func TestAnOrphanThatGrowsPastTheByteBoundIsDropped(t *testing.T) {
	results := newSpyStreamEvents()
	index := newStreamIndex(20, testOrphansPerKey)
	log := testLog(index, "request-1", results, nil, testLogLimit)
	log.append([]byte("data: 1\n\n"))
	log.detachSocket()

	log.append([]byte("data: 2\n\n"))
	log.append([]byte("data: 3\n\n"))

	if !log.dropped {
		t.Fatal("an orphan past the byte bound was kept")
	}
	if got := results.seenOutcomes(); len(got) != 1 || got[0] != "qwen/"+orphanDroppedBudget {
		t.Fatalf("outcomes = %v, want one %s", got, orphanDroppedBudget)
	}
	if orphans, bytes := index.stats(); orphans != 0 || bytes != 0 {
		t.Fatalf("index holds %d orphans %d bytes, want none", orphans, bytes)
	}
}

// Test flow:
//  1. Give a log a socket and append an event.
//  2. Assert the log reports the socket attached and the log cannot be adopted.
//  3. Detach the socket and assert the log is an orphan that an adopting reader can take, once.
func TestALogWithASocketOrAReaderIsNotAdopted(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, testLogLimit)
	log.append([]byte("data: 1\n\n"))

	if _, adopted := log.attach(0, false); adopted {
		t.Fatal("a log with a live socket was adopted")
	}
	log.detachSocket()
	reader, adopted := log.attach(0, false)
	if !adopted {
		t.Fatal("an orphan was not adopted")
	}
	if _, again := log.attach(0, false); again {
		t.Fatal("a log somebody reads was adopted a second time")
	}
	reader.release()
	if _, readopted := log.attach(0, false); !readopted {
		t.Fatal("a log whose reader left was not adoptable again")
	}
}

// Test flow:
//  1. Attach a reader to a log and read its events.
//  2. Take the log over with an explicit resume from event 1.
//  3. Assert the displaced reader is told it is done and the new reader starts at event 1.
func TestAnExplicitResumeDisplacesTheReaderAndStartsAtItsPosition(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, testLogLimit)
	log.append([]byte("data: 1\n\ndata: 2\n\n"))
	first, _ := log.attach(0, true)
	if events, _, _, _ := first.next(); string(events) != "data: 1\n\ndata: 2\n\n" {
		t.Fatalf("first reader read %q, want both events", events)
	}

	second, attached := log.attach(1, true)

	if !attached {
		t.Fatal("an explicit resume was refused")
	}
	if _, done, _, _ := first.next(); !done {
		t.Fatal("the displaced reader was not told it is done")
	}
	if events, _, _, _ := second.next(); string(events) != "data: 2\n\n" {
		t.Fatalf("second reader read %q, want from event 1", events)
	}
}

// Test flow:
//  1. Attach a reader at the end of a live log.
//  2. Assert it waits on a channel, then append an event and assert the channel closed and the event is read.
//  3. Conclude the log and assert the reader reports done after the last event.
func TestAReaderWaitsForNewEventsAndEndsWhenTheLogIsSealed(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, testLogLimit)
	reader, _ := log.attach(0, true)

	events, done, _, changed := reader.next()
	if len(events) != 0 || done || changed == nil {
		t.Fatalf("empty live log read %q done %v changed %v, want a wait", events, done, changed)
	}
	log.append([]byte("data: 1\n\n"))
	select {
	case <-changed:
	default:
		t.Fatal("appending did not wake the reader")
	}
	if events, _, _, _ := reader.next(); string(events) != "data: 1\n\n" {
		t.Fatalf("read %q after the append", events)
	}
	_, _ = log.conclude("7", nil)
	if events, done, _, _ := reader.next(); len(events) != 0 || !done {
		t.Fatalf("sealed log read %q done %v, want done", events, done)
	}
}

// Test flow:
//  1. Parse a header with a valid cursor, an absent header, and values that do not parse.
//  2. Assert the valid cursor carries its id and position, the absent one is not requested, and the rest are `ErrInvalidResumeCursor`.
func TestParseResumeCursor(t *testing.T) {
	cursor, err := parseResumeCursor("request-1/12")
	if err != nil || !cursor.requested || cursor.streamID != "request-1" || cursor.position != 12 {
		t.Fatalf("parse = %+v, %v, want request-1 at 12", cursor, err)
	}
	if cursor, err = parseResumeCursor(resumeAbsent); err != nil || cursor.requested {
		t.Fatalf("absent header = %+v, %v, want no resume", cursor, err)
	}
	for _, value := range []string{"nope", "/1", "id/", "id/-1", "id/1.5", "id/99999999999"} {
		if _, err := parseResumeCursor(value); !errors.Is(err, ErrInvalidResumeCursor) {
			t.Errorf("parse(%q) error = %v, want ErrInvalidResumeCursor", value, err)
		}
	}
}

// Test flow:
//  1. Slice a stored entry of three events from position 0, 1 and 3.
//  2. Assert each slice is the matching suffix with bounds rebased to it, and the original entry is untouched.
func TestAStoredEntryIsSlicedFromAnEventPosition(t *testing.T) {
	entry := cachedResponse{body: []byte("aa\n\nbbb\n\nc\n\n"), bounds: []int{4, 9, 12}}

	for position, want := range map[int]struct {
		body   string
		bounds []int
	}{0: {"aa\n\nbbb\n\nc\n\n", []int{4, 9, 12}}, 1: {"bbb\n\nc\n\n", []int{5, 8}}, 3: {"", []int{}}} {
		sliced := entry.fromEvent(position)
		if string(sliced.body) != want.body || !slices.Equal(sliced.bounds, want.bounds) {
			t.Errorf("from %d = %q %v, want %q %v", position, sliced.body, sliced.bounds, want.body, want.bounds)
		}
	}
	if !slices.Equal(entry.bounds, []int{4, 9, 12}) {
		t.Fatalf("original bounds = %v, the slice must not rewrite them", entry.bounds)
	}
}

// Test flow:
//  1. Attach a reader to a log, then drop the log by appending past its limit.
//  2. Pump the reader into a recorder.
//  3. Assert the reply ends with an error event naming the dropped stream, not silently.
func TestAReaderOfADroppedLogIsToldTheStreamWasDropped(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, 16)
	reader, _ := log.attach(0, true)
	log.append([]byte("data: 1\n\n"))
	log.append([]byte("data: 2\n\n"))
	recorder := httptest.NewRecorder()

	pumpLog(context.Background(), recorder, reader)

	if got := recorder.Body.String(); !strings.Contains(got, ErrStreamDropped.Error()) {
		t.Fatalf("reader got %q, want an error event naming the dropped stream", got)
	}
}

// Test flow:
//  1. Give the index a byte bound of zero and orphan an empty log.
//  2. Assert the log was dropped, labelled as an orphan drop, and the index holds nothing.
func TestAZeroByteBoundKeepsNoOrphanNotEvenAnEmptyOne(t *testing.T) {
	index := newStreamIndex(0, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, testLogLimit)

	log.detachSocket()

	if _, refusal := log.conclude("7", nil); refusal != cacheRefusedOrphanDropped {
		t.Fatalf("conclude refusal = %q, want %q", refusal, cacheRefusedOrphanDropped)
	}
	if orphans, _ := index.stats(); orphans != 0 {
		t.Fatalf("orphans = %d, want none", orphans)
	}
}

// Test flow:
//  1. Orphan a log, adopt it, and then try to evict it as the oldest orphan of its key.
//  2. Assert the eviction left the adopted log alone.
func TestAnEvictionDoesNotDropALogThatWasAdoptedMeanwhile(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, testLogLimit)
	log.append([]byte("data: 1\n\n"))
	log.detachSocket()
	if _, adopted := log.attach(0, false); !adopted {
		t.Fatal("the orphan was not adopted")
	}

	log.evict()

	if log.dropped {
		t.Fatal("an eviction dropped a log somebody was reading")
	}
}

// Test flow:
//  1. Orphan a client stream whose socket write is stalled, holding its lock, and report the race's outcome.
//  2. Assert the stream is settled without waiting for the stalled write to give the lock back.
func TestAnOutcomeIsSettledWhileAStalledSocketHoldsTheStream(t *testing.T) {
	index := newStreamIndex(testOrphanBytes, testOrphansPerKey)
	log := testLog(index, "request-1", nil, nil, testLogLimit)
	stream := newClientStream(httptest.NewRecorder(), "request-1", true, false, filters.LogprobIntent{}, 0, nil)
	settled := make(chan struct{})
	stream.recordTo(log, func(engine.RaceOutcome) { close(settled) })
	if !stream.orphan() {
		t.Fatal("the stream was not orphaned")
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()

	stream.reportOutcome(engine.RaceOutcome{Succeeded: true})

	awaitValue(t, settled, "the outcome to be settled")
}
