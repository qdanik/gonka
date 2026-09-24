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

// Test flow:
//  1. Register two devshards (escrows "7" and "9"), mark "9" busy, and set the settle result for escrow 7.
//  2. POST a batch settle for escrows "7", "9", and an unregistered "404".
//  3. Assert the response is 200 with settled=1 and failed=2 across 3 results.
//  4. Assert escrow 7's result carries its transaction hash and settler with no failure status.
//  5. Assert escrow 9 answers 409 (busy) and escrow 404 answers 404 (unregistered).
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

// settleBarrier tracks how many settles are in flight at once, to tell a parallel run from a sequential one.
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

// Test flow:
//  1. Register 4 devshards and wire a `settleBarrier` that tracks concurrent settles into the operations' onSettle hook.
//  2. POST a batch settle naming all 4 escrows.
//  3. Assert the response is 200.
//  4. Assert the barrier's peak in-flight count reached 4, so the batch settled them concurrently rather than one after another.
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

// Test flow:
//  1. Register 6 devshards and wire the same concurrency-tracking barrier.
//  2. POST a batch settle naming all 6 escrows.
//  3. Assert the response is 200 and the peak in-flight count never exceeded `settleBatchConcurrency`.
//  4. Assert all 6 escrows were reported settled, so the pool queues the rest rather than dropping them.
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

// Test flow:
//  1. Register a busy escrow "9".
//  2. POST a batch settle for it with `force: true` in the body.
//  3. Assert the single result answers 409.
//  4. Assert no "settle" call was recorded, so a body-supplied force never reaches the settle path.
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

// Test flow:
//  1. Register a busy escrow "9" and set its settle result.
//  2. POST a batch settle for it with `force=true` as a query parameter.
//  3. Assert the escrow was settled and its result carries the expected transaction hash.
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

// Test flow:
//  1. Register escrow "7".
//  2. POST a batch settle naming escrow "7" three times, with whitespace variants.
//  3. Assert the response carries exactly one result.
//  4. Assert exactly one "settle" call was recorded.
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

// Test flow:
//  1. Build an oversized escrow-ID list one entry past `settleBatchLimit`.
//  2. Define a table of request bodies, varying across no body at all, an empty list, blanks only, and the oversized list.
//  3. For each case, POST it to the batch settle route on a fresh harness.
//  4. Assert the response is 400.
//  5. Assert no "settle" call was recorded, so a refused list never reaches the settle path.
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
