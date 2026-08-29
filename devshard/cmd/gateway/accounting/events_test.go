package accounting

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// tickingBook advances one second per reading, so the feed's order is a fact of the test rather than
// of how fast it runs.
func tickingBook(t *testing.T, groupSize int) *Book {
	t.Helper()
	tick := time.Unix(0, 0).UTC()
	book := NewBook(func() time.Time {
		tick = tick.Add(time.Second)
		return tick
	})
	openTestEscrow(t, book, testEscrow, testEpoch, groupSize)
	return book
}

func decodeEvents(t *testing.T, recorder *httptest.ResponseRecorder) []ProtocolEventRecord {
	t.Helper()
	var body struct {
		Events []ProtocolEventRecord `json:"events"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding %s: %v", recorder.Body.String(), err)
	}
	return body.Events
}

// The counters say a host took three misses; this says which nonce and, through it, which client
// request took them. Without the request id the number has no drill-down.
func TestAVerdictNamesTheNonceAndTheRequestThatSpentIt(t *testing.T) {
	book := tickingBook(t, 2)
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 3, RequestID: "req-a", Sent: true}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := book.RecordAppliedTimeout(testEscrow, 3); err != nil {
		t.Fatalf("RecordAppliedTimeout(): %v", err)
	}

	events := book.Events(QueryFilter{})

	if len(events) != 1 {
		t.Fatalf("got %d events, want the one applied timeout", len(events))
	}
	want := ProtocolEventRecord{
		EscrowID: testEscrow, Participant: participantFor(1), Model: testModel,
		Nonce: 3, SlotID: 1, Kind: ProtocolTimeoutApplied, RequestID: "req-a", At: events[0].At,
	}
	if events[0] != want {
		t.Errorf("event = %+v, want %+v", events[0], want)
	}
}

func TestAnInvalidVerdictReachesTheFeedUnderItsOwnKind(t *testing.T) {
	book := tickingBook(t, 2)
	if err := book.RecordInvalidVerdict(testEscrow, 2); err != nil {
		t.Fatalf("RecordInvalidVerdict(): %v", err)
	}

	events := book.Events(QueryFilter{})

	if len(events) != 1 {
		t.Fatalf("got %d events, want the one invalid verdict", len(events))
	}
	if events[0].Kind != ProtocolInvalidated {
		t.Errorf("kind = %q, want %q", events[0].Kind, ProtocolInvalidated)
	}
	if events[0].Participant != participantFor(0) {
		t.Errorf("charged %s, want the executor of nonce 2", events[0].Participant)
	}
}

func TestTheFeedReadsNewestFirst(t *testing.T) {
	book := tickingBook(t, 2)
	for _, nonce := range []uint64{2, 4, 6} {
		if err := book.RecordAppliedTimeout(testEscrow, nonce); err != nil {
			t.Fatalf("RecordAppliedTimeout(%d): %v", nonce, err)
		}
	}

	events := book.Events(QueryFilter{})

	got := []uint64{events[0].Nonce, events[1].Nonce, events[2].Nonce}
	if want := []uint64{6, 4, 2}; got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("nonces = %v, want %v", got, want)
	}
}

// An escrow that goes badly wrong must not grow the feed without bound, and what it keeps must be the
// end of the run rather than its beginning.
func TestTheRingKeepsTheNewestVerdictsOnly(t *testing.T) {
	book := tickingBook(t, 2)
	for nonce := range uint64(maxEventsPerEscrow + 10) {
		if err := book.RecordAppliedTimeout(testEscrow, nonce); err != nil {
			t.Fatalf("RecordAppliedTimeout(%d): %v", nonce, err)
		}
	}

	events := book.Events(QueryFilter{})

	if len(events) != maxEventsPerEscrow {
		t.Fatalf("feed holds %d events, want the ring's %d", len(events), maxEventsPerEscrow)
	}
	if newest := events[0].Nonce; newest != maxEventsPerEscrow+9 {
		t.Errorf("newest nonce = %d, want the last one recorded", newest)
	}
	if oldest := events[len(events)-1].Nonce; oldest != 10 {
		t.Errorf("oldest nonce = %d, want the first the ring still holds", oldest)
	}
}

func TestTheFeedAnswersOnlyAboutTheHostAsked(t *testing.T) {
	book := tickingBook(t, 2)
	if err := book.RecordAppliedTimeout(testEscrow, 2); err != nil {
		t.Fatalf("RecordAppliedTimeout(): %v", err)
	}
	if err := book.RecordAppliedTimeout(testEscrow, 3); err != nil {
		t.Fatalf("RecordAppliedTimeout(): %v", err)
	}

	events := book.Events(QueryFilter{Participant: participantFor(1)})

	if len(events) != 1 || events[0].Nonce != 3 {
		t.Fatalf("got %+v, want only the nonce that landed on slot 1", events)
	}
}

func TestBothEventRoutesAnswerTheSameFeed(t *testing.T) {
	book := tickingBook(t, 2)
	if err := book.RecordAppliedTimeout(testEscrow, 3); err != nil {
		t.Fatalf("RecordAppliedTimeout(): %v", err)
	}

	for _, target := range []string{
		"/api/v1/epochs/current/events?participant=" + participantFor(1),
		"/api/v1/epochs/current/events/" + participantFor(1),
	} {
		t.Run(target, func(t *testing.T) {
			recorder := serve(t, book, target)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body)
			}
			events := decodeEvents(t, recorder)
			if len(events) != 1 || events[0].Nonce != 3 {
				t.Fatalf("got %+v, want the one verdict against that host", events)
			}
		})
	}
}

// A host with nothing against it is healthy, not missing.
func TestAHostWithNoVerdictsGetsAnEmptyFeed(t *testing.T) {
	book := tickingBook(t, 2)

	recorder := serve(t, book, "/api/v1/epochs/current/events/"+participantFor(0))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body)
	}
	if events := decodeEvents(t, recorder); len(events) != 0 {
		t.Errorf("got %+v, want an empty feed", events)
	}
}
