package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/store"
)

func sendTime() time.Time { return time.Unix(1700000000, 0).UTC() }

func completedRace() engine.RaceOutcome {
	return engine.RaceOutcome{
		RequestID:   "request-1",
		EscrowID:    "escrow-7",
		Model:       "qwen",
		InputTokens: 128,
		Stream:      true,
		Decision:    "hedged",
		WinnerNonce: 42,
		Succeeded:   true,
		Attempts: []engine.AttemptOutcome{
			{
				Participant:           "gonka1loser",
				HostIdx:               1,
				HostLabel:             "host-1",
				Nonce:                 41,
				SendTime:              sendTime().Add(-500 * time.Millisecond),
				Completed:             sendTime().Add(2 * time.Second),
				UsageCompletionTokens: 125,
				Terminal:              engine.TerminalLost,
			},
			{
				Participant:           "gonka1winner",
				HostIdx:               3,
				HostLabel:             "host-3",
				Nonce:                 42,
				SendTime:              sendTime(),
				FirstToken:            sendTime().Add(450 * time.Millisecond),
				Completed:             sendTime().Add(4 * time.Second),
				UsageCompletionTokens: 256,
				Terminal:              engine.TerminalWon,
			},
		},
	}
}

func TestACompletedRaceIsAccountedWithTheOutcomesOwnNumbers(t *testing.T) {
	live := newHarness(t)

	NewRaceLedger(live.accounting).RecordRequest(completedRace())

	rows := live.accounting.rowsRecorded()
	if len(rows) != 1 {
		t.Fatalf("recorded %d rows, want exactly 1", len(rows))
	}
	expected := store.RequestRecord{
		RequestID:          "request-1",
		EscrowID:           "escrow-7",
		Model:              "qwen",
		Outcome:            store.RequestSettled,
		Decision:           "hedged",
		Stream:             true,
		WinnerNonce:        42,
		WinnerParticipant:  "gonka1winner",
		WinnerHost:         "host-3",
		WinnerHostIdx:      3,
		Attempts:           2,
		InputTokens:        128,
		WinnerOutputTokens: 256,
		TotalOutputTokens:  381,
		StartedAt:          sendTime().Add(-500 * time.Millisecond),
		CompletedAt:        sendTime().Add(4 * time.Second),
		FirstTokenMS:       450,
		DurationMS:         4_000,
	}
	if rows[0] != expected {
		t.Fatalf("recorded %+v, want %+v", rows[0], expected)
	}
}

func TestAFailedRaceAndARaceWithoutAnEscrowAreBothAccounted(t *testing.T) {
	testCases := []struct {
		name     string
		outcome  engine.RaceOutcome
		expected store.RequestRecord
	}{
		{
			name: "every attempt failed",
			outcome: engine.RaceOutcome{
				RequestID: "request-2",
				EscrowID:  "escrow-7",
				Model:     "qwen",
				Decision:  "escalated",
				Attempts: []engine.AttemptOutcome{
					{Participant: "gonka1host", Nonce: 41, SendTime: sendTime(), Completed: sendTime().Add(time.Second), Terminal: engine.TerminalDialFailure},
				},
				Lifecycle: engine.Lifecycle{BalanceExhausted: true},
			},
			expected: store.RequestRecord{
				RequestID:        "request-2",
				EscrowID:         "escrow-7",
				Model:            "qwen",
				Outcome:          store.RequestFailed,
				Decision:         "escalated",
				Attempts:         1,
				BalanceExhausted: true,
				StartedAt:        sendTime(),
				CompletedAt:      sendTime().Add(time.Second),
			},
		},
		{
			name:    "no escrow was ever picked",
			outcome: engine.RaceOutcome{RequestID: "request-3", Model: "qwen"},
			expected: store.RequestRecord{
				RequestID: "request-3",
				Model:     "qwen",
				Outcome:   store.RequestNoEscrow,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			live := newHarness(t)

			NewRaceLedger(live.accounting).RecordRequest(testCase.outcome)

			rows := live.accounting.rowsRecorded()
			if len(rows) != 1 {
				t.Fatalf("recorded %d rows, want exactly 1", len(rows))
			}
			if rows[0] != testCase.expected {
				t.Fatalf("recorded %+v, want %+v", rows[0], testCase.expected)
			}
		})
	}
}

func TestTheLookupReturnsTheRecordedRow(t *testing.T) {
	live := newHarness(t)
	NewRaceLedger(live.accounting).RecordRequest(completedRace())

	response := live.request(t, http.MethodGet, "/v1/requests/request-1", "", adminHeaders())

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var body requestAccountingResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	expected := requestAccountingResponse{
		RequestID:          "request-1",
		EscrowID:           "escrow-7",
		Model:              "qwen",
		Outcome:            string(store.RequestSettled),
		Decision:           "hedged",
		Stream:             true,
		WinnerNonce:        42,
		WinnerParticipant:  "gonka1winner",
		WinnerHost:         "host-3",
		WinnerHostIdx:      3,
		Attempts:           2,
		InputTokens:        128,
		WinnerOutputTokens: 256,
		TotalOutputTokens:  381,
		StartedAt:          "2023-11-14T22:13:19.5Z",
		CompletedAt:        "2023-11-14T22:13:24Z",
		FirstTokenMS:       450,
		DurationMS:         4_000,
	}
	if body != expected {
		t.Fatalf("body = %+v, want %+v", body, expected)
	}
}

func TestTheLookupAnswersItsDocumentedStatuses(t *testing.T) {
	testCases := []struct {
		name     string
		method   string
		target   string
		headers  map[string]string
		findErr  error
		expected int
	}{
		{name: "unknown id", method: http.MethodGet, target: "/v1/requests/nope", headers: adminHeaders(), expected: http.StatusNotFound},
		{name: "blank id", method: http.MethodGet, target: "/v1/requests/%20", headers: adminHeaders(), expected: http.StatusBadRequest},
		{name: "wrong method", method: http.MethodPost, target: "/v1/requests/request-1", headers: adminHeaders(), expected: http.StatusMethodNotAllowed},
		{name: "no admin key", method: http.MethodGet, target: "/v1/requests/request-1", expected: http.StatusUnauthorized},
		{name: "lookup failed", method: http.MethodGet, target: "/v1/requests/request-1", headers: adminHeaders(), findErr: errors.New("disk gone"), expected: http.StatusInternalServerError},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			live := newHarness(t)
			live.accounting.findErr = testCase.findErr
			NewRaceLedger(live.accounting).RecordRequest(completedRace())

			response := live.request(t, testCase.method, testCase.target, "", testCase.headers)

			if response.Code != testCase.expected {
				t.Fatalf("status = %d, want %d: %s", response.Code, testCase.expected, response.Body.String())
			}
		})
	}
}

func TestTheLookupStaysAvailableWhileTheGatewayIsDisabled(t *testing.T) {
	live := newHarness(t)
	NewRaceLedger(live.accounting).RecordRequest(completedRace())
	live.swapConfig(func(configuration *config.Config) { configuration.Modes.Disabled = true })

	response := live.request(t, http.MethodGet, "/v1/requests/request-1", "", adminHeaders())

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
}

// wedgedLedger is a real ledger over a real database whose write lock is held by another connection,
// so every insert it attempts sits in busy_timeout for the whole test.
func wedgedLedger(t *testing.T) (*store.Ledger, func()) {
	t.Helper()
	storageDir := t.TempDir()
	gatewayStore, err := store.Open(storageDir)
	if err != nil {
		t.Fatalf("store.Open(): %v", err)
	}
	ledger, err := gatewayStore.NewLedger(
		store.Retention{MaxAge: time.Hour, MaxRows: 100},
		func() time.Time { return time.Unix(1700000000, 0).UTC() })
	if err != nil {
		t.Fatalf("NewLedger(): %v", err)
	}
	blocker, err := sql.Open("sqlite", filepath.Join(storageDir, "gateway.db"))
	if err != nil {
		t.Fatalf("opening the blocking connection: %v", err)
	}
	blocking, err := blocker.Begin()
	if err != nil {
		t.Fatalf("Begin(): %v", err)
	}
	if _, err := blocking.Exec(`INSERT INTO request_accounting (request_id, recorded_at) VALUES ('wedge', '')`); err != nil {
		t.Fatalf("taking the write lock: %v", err)
	}
	release := sync.OnceFunc(func() { _ = blocking.Rollback() })
	t.Cleanup(func() {
		release()
		_ = blocker.Close()
		_ = gatewayStore.Close()
	})
	return ledger, release
}

func TestASlowLedgerNeitherDelaysNorFailsTheClientsResponse(t *testing.T) {
	live := newHarness(t)
	ledger, releaseTheWedge := wedgedLedger(t)
	live.inference.outcome = completedRace()
	live.inference.ledger = NewRaceLedger(ledger)

	served := make(chan responseSnapshot, 1)
	go func() {
		response := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
		served <- responseSnapshot{status: response.Code, body: response.Body.String()}
	}()

	select {
	case response := <-served:
		if response.status != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", response.status, response.body)
		}
		if response.body != live.inference.reply {
			t.Fatalf("body = %q, want %q", response.body, live.inference.reply)
		}
	// Shorter than the store's busy_timeout, so a write that ran on this goroutine would miss it.
	case <-time.After(3 * time.Second):
		t.Fatal("the client's response was held behind the wedged ledger write")
	}

	if written := ledger.Stats().Written; written != 0 {
		t.Fatalf("Stats().Written = %d while the write lock was held; the wedge is not wedging", written)
	}
	releaseTheWedge()
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if _, found, err := ledger.Find(t.Context(), "request-1"); err != nil || !found {
		t.Fatalf("Find() = found %v, err %v; the stalled row never landed", found, err)
	}
}

type responseSnapshot struct {
	status int
	body   string
}
