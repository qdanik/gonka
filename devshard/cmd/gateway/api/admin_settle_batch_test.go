package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
)

type batchSettleAnswer struct {
	Settled int `json:"settled"`
	Failed  int `json:"failed"`
	Results []struct {
		EscrowID string `json:"escrow_id"`
		TxHash   string `json:"tx_hash"`
		Settler  string `json:"settler"`
		Status   int    `json:"status"`
		Error    string `json:"error"`
	} `json:"results"`
}

func decodeBatchSettle(t *testing.T, body []byte) batchSettleAnswer {
	t.Helper()
	var answer batchSettleAnswer
	if err := json.Unmarshal(body, &answer); err != nil {
		t.Fatalf("decoding %s: %v", body, err)
	}
	return answer
}

// A list must not fail whole over its worst entry: each escrow carries the status the single-escrow
// route would have returned for it, so an operator reads one 200 instead of replaying the list.
func TestABatchSettleAnswersForEveryEscrowItWasGiven(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = []store.DevshardRecord{
		{EscrowID: "7", Model: "qwen"},
		{EscrowID: "9", Model: "qwen"},
	}
	live.escrows.busy["9"] = true
	live.operations.settle = chain.SettleEscrowResult{EscrowID: 7, TxHash: "TX7", Settler: "gonka1settler"}

	response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle",
		`{"escrow_ids":["7","9","404"]}`, adminHeaders())

	if response.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s), want 200", response.Code, response.Body.String())
	}
	answer := decodeBatchSettle(t, response.Body.Bytes())
	if answer.Settled != 1 || answer.Failed != 2 {
		t.Fatalf("counts: got settled=%d failed=%d, want 1 and 2", answer.Settled, answer.Failed)
	}
	if len(answer.Results) != 3 {
		t.Fatalf("results: got %d entries, want 3", len(answer.Results))
	}
	byEscrow := map[string]int{}
	for _, result := range answer.Results {
		byEscrow[result.EscrowID] = result.Status
	}
	if answer.Results[0].TxHash != "TX7" || answer.Results[0].Settler != "gonka1settler" {
		t.Fatalf("the settled escrow lost its transaction: %+v", answer.Results[0])
	}
	if byEscrow["7"] != 0 {
		t.Fatalf("a settled escrow carries a failure status: %d", byEscrow["7"])
	}
	if byEscrow["9"] != http.StatusConflict {
		t.Fatalf("a draining escrow: got %d, want 409", byEscrow["9"])
	}
	if byEscrow["404"] != http.StatusNotFound {
		t.Fatalf("an unregistered escrow: got %d, want 404", byEscrow["404"])
	}
}

// settleBarrierGrace lets a sequential run finish and be judged on its peak rather than hang.
const settleBarrierGrace = 200 * time.Millisecond

// settleBarrier holds every settle it sees until the batch has the expected number of them in flight at
// once, which tells a parallel run from a sequential one without timing guesses.
type settleBarrier struct {
	mu        sync.Mutex
	inFlight  int
	peak      int
	target    int
	reached   chan struct{}
	closeOnce sync.Once
}

func newSettleBarrier(target int) *settleBarrier {
	return &settleBarrier{target: target, reached: make(chan struct{})}
}

func (b *settleBarrier) hold() {
	b.mu.Lock()
	b.inFlight++
	if b.inFlight > b.peak {
		b.peak = b.inFlight
	}
	if b.inFlight >= b.target {
		b.closeOnce.Do(func() { close(b.reached) })
	}
	b.mu.Unlock()

	select {
	case <-b.reached:
	case <-time.After(settleBarrierGrace):
	}

	b.mu.Lock()
	b.inFlight--
	b.mu.Unlock()
}

func (b *settleBarrier) peakInFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

func registeredDevshardRows(escrowIDs ...string) []store.DevshardRecord {
	records := make([]store.DevshardRecord, 0, len(escrowIDs))
	for _, escrowID := range escrowIDs {
		records = append(records, store.DevshardRecord{EscrowID: escrowID, Model: "qwen"})
	}
	return records
}

// Settling waits on a chain commit, so a batch that walks its list one escrow at a time is the defect
// the route exists to remove.
func TestABatchSettlesItsEscrowsAtTheSameTime(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = registeredDevshardRows("1", "2", "3", "4")
	barrier := newSettleBarrier(4)
	live.operations.onSettle = func(string) { barrier.hold() }

	response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle",
		`{"escrow_ids":["1","2","3","4"]}`, adminHeaders())

	if response.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s), want 200", response.Code, response.Body.String())
	}
	if peak := barrier.peakInFlight(); peak != 4 {
		t.Fatalf("settles in flight at once: got %d, want 4 -- the batch is settling one escrow after another", peak)
	}
}

// Building a settlement asks every host in the escrow's group to sign, so a batch that starts all of its
// escrows at once turns one operator call into a fan-out the hosts feel.
func TestABatchKeepsItsSettlementsWithinThePool(t *testing.T) {
	live := newHarness(t)
	escrowIDs := []string{"1", "2", "3", "4", "5", "6"}
	live.control.devshards = registeredDevshardRows(escrowIDs...)
	barrier := newSettleBarrier(len(escrowIDs))
	live.operations.onSettle = func(string) { barrier.hold() }

	response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle",
		`{"escrow_ids":["1","2","3","4","5","6"]}`, adminHeaders())

	if response.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s), want 200", response.Code, response.Body.String())
	}
	if peak := barrier.peakInFlight(); peak > settleBatchConcurrency {
		t.Fatalf("settles in flight at once: got %d, want at most %d", peak, settleBatchConcurrency)
	}
	if answer := decodeBatchSettle(t, response.Body.Bytes()); answer.Settled != len(escrowIDs) {
		t.Fatalf("settled: got %d, want %d -- the pool must queue the rest, not drop them", answer.Settled, len(escrowIDs))
	}
}

// The single-escrow route refuses a body-supplied force, and a list must not become the way around it.
func TestABatchBodyCannotBuyItsWayPastTheBusyCheck(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = registeredDevshardRows("9")
	live.escrows.busy["9"] = true

	response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle",
		`{"escrow_ids":["9"],"force":true}`, adminHeaders())

	answer := decodeBatchSettle(t, response.Body.Bytes())
	if len(answer.Results) != 1 || answer.Results[0].Status != http.StatusConflict {
		t.Fatalf("a busy escrow: got %+v, want one 409", answer.Results)
	}
	if calls := live.operations.recordedCalls(); slices.Contains(calls, "settle") {
		t.Fatalf("a body-supplied force reached the settle path: %v", calls)
	}
}

// Force is one flag over the whole list, as the query parameter the single-escrow route already reads.
func TestAForcedBatchCrossesTheBusyCheck(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = registeredDevshardRows("9")
	live.escrows.busy["9"] = true
	live.operations.settle = chain.SettleEscrowResult{EscrowID: 9, TxHash: "TX9"}

	response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle?force=true",
		`{"escrow_ids":["9"]}`, adminHeaders())

	answer := decodeBatchSettle(t, response.Body.Bytes())
	if answer.Settled != 1 || len(answer.Results) != 1 || answer.Results[0].TxHash != "TX9" {
		t.Fatalf("a forced settle: got %+v, want escrow 9 settled", answer)
	}
}

// A settlement is irreversible, so a list naming an escrow twice must not broadcast for it twice.
func TestABatchSettlesARepeatedEscrowOnce(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = registeredDevshardRows("7")

	response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle",
		`{"escrow_ids":["7"," 7 ","7"]}`, adminHeaders())

	answer := decodeBatchSettle(t, response.Body.Bytes())
	if len(answer.Results) != 1 {
		t.Fatalf("results: got %+v, want one entry", answer.Results)
	}
	if settles := slices.Contains(live.operations.recordedCalls(), "settle"); !settles {
		t.Fatal("the escrow was never settled")
	}
	if got := len(live.operations.recordedCalls()); got != 1 {
		t.Fatalf("settle calls: got %d, want 1", got)
	}
}

func TestABatchRefusesAListItCannotAnswerFor(t *testing.T) {
	oversized := make([]string, settleBatchLimit+1)
	for index := range oversized {
		oversized[index] = strconv.Itoa(index)
	}
	oversizedBody, err := json.Marshal(SettleDevshardsRequest{EscrowIDs: oversized})
	if err != nil {
		t.Fatalf("building the oversized body: %v", err)
	}
	testCases := []struct {
		name string
		body string
	}{
		{name: "no body at all", body: ""},
		{name: "an empty list", body: `{"escrow_ids":[]}`},
		{name: "blanks only", body: `{"escrow_ids":["  "]}`},
		{name: "more escrows than one call settles", body: string(oversizedBody)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			live := newHarness(t)
			response := live.request(t, http.MethodPost, "/v1/admin/devshards/settle", testCase.body, adminHeaders())
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status: got %d (%s), want 400", response.Code, response.Body.String())
			}
			if calls := live.operations.recordedCalls(); len(calls) != 0 {
				t.Fatalf("a refused list reached the settle path: %v", calls)
			}
		})
	}
}
