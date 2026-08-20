package accounting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serve(t *testing.T, book *Book, target string) *httptest.ResponseRecorder {
	t.Helper()
	currentEpoch := func(context.Context) (uint64, error) { return testEpoch, nil }
	recorder := httptest.NewRecorder()
	NewHandler(book, currentEpoch, nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	return recorder
}

func decodeParticipants(t *testing.T, recorder *httptest.ResponseRecorder) []ParticipantRecord {
	t.Helper()
	var body struct {
		Participants []ParticipantRecord `json:"participants"`
		Records      []ParticipantRecord `json:"records"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding %s: %v", recorder.Body.String(), err)
	}
	return append(body.Participants, body.Records...)
}

// TestParticipantRouteAnswersOnlyAboutThatParticipant is the route the whole surface exists for: a
// host asking what this gateway saw of it must not receive its neighbours' records.
func TestParticipantRouteAnswersOnlyAboutThatParticipant(t *testing.T) {
	book := twoEpochBook(t)

	recorder := serve(t, book, "/api/v1/epochs/current/participants/"+participantFor(0))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body)
	}
	records := decodeParticipants(t, recorder)
	if len(records) != 1 {
		t.Fatalf("got %d records, want the one participant of the current epoch", len(records))
	}
	if records[0].Participant != participantFor(0) || records[0].EpochIndex != testEpoch {
		t.Fatalf("got %s in epoch %d, want %s in epoch %d",
			records[0].Participant, records[0].EpochIndex, participantFor(0), testEpoch)
	}
}

func TestParticipantRouteReportsAnAddressTheEpochNeverSaw(t *testing.T) {
	book := twoEpochBook(t)

	recorder := serve(t, book, "/api/v1/epochs/current/participants/gonka1nobody")

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for an address with no record", recorder.Code)
	}
}

// TestEpochZeroIsRefused keeps the path selector honest: the filter reads a zero epoch as "every
// epoch", so serving it would answer a question about one epoch with all of them.
func TestEpochZeroIsRefused(t *testing.T) {
	book := twoEpochBook(t)

	recorder := serve(t, book, "/api/v1/epochs/0/participants")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body)
	}
}

func TestParticipantsRouteNarrowsToItsEpoch(t *testing.T) {
	book := twoEpochBook(t)

	recorder := serve(t, book, "/api/v1/epochs/8/participants")

	records := decodeParticipants(t, recorder)
	if len(records) == 0 {
		t.Fatal("epoch 8 came back empty though the ledger holds it")
	}
	for _, record := range records {
		if record.EpochIndex != 8 {
			t.Fatalf("epoch %d present under the route for epoch 8", record.EpochIndex)
		}
	}
}

func TestEpochsRouteListsEveryEpochTheLedgerHolds(t *testing.T) {
	book := twoEpochBook(t)

	recorder := serve(t, book, "/api/v1/epochs")

	var body struct {
		Epochs []EpochSummary `json:"epochs"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding %s: %v", recorder.Body.String(), err)
	}
	if len(body.Epochs) != 2 {
		t.Fatalf("got %d epochs, want both the ledger holds", len(body.Epochs))
	}
}

// TestParticipantRouteCarriesFindings is the feature seen from where a host operator stands: one GET
// about its own address, answered with what to look at rather than only with what was counted.
func TestParticipantRouteCarriesFindings(t *testing.T) {
	recorder := serve(t, troubledBook(t), "/api/v1/epochs/current/participants/"+participantFor(0))

	records := decodeParticipants(t, recorder)
	if len(records) != 1 {
		t.Fatalf("got %d records, want the participant asked about", len(records))
	}
	findingWithCode(t, records[0].Findings, FindingExecutionTimeouts)
}

func TestUnknownPathIsRefused(t *testing.T) {
	book := twoEpochBook(t)

	recorder := serve(t, book, "/api/v1/participants")

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for the route the epoch selector replaced", recorder.Code)
	}
}
