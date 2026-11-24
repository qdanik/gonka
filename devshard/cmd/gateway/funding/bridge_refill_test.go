package funding

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/liquidity"
)

func starvedTemp(id string) EscrowState {
	return EscrowState{ID: id, Temp: true, Label: 8, FullCost: testFullCost, Money: liquidity.Escrow{Free: 0, Starved: true}}
}

func drainedBridgeWindow(change func(state *ModelState)) ModelState {
	return modelWith(func(state *ModelState) {
		state.Escrows = []EscrowState{starvedTemp("t1"), starvedTemp("t2"), starvedTemp("t3"), starvedTemp("t4")}
		state.Counted, state.TempCount, state.MaxUnsettled = 4, 4, 20
		state.DemandPeak, state.MoneyShort, state.InBridgeWindow = 1_000_000, true, true
		change(state)
	})
}

// Test flow:
//  1. Table-driven: four drained temps of the bridge label with routing marking money short and demand above liquidity, in the bridge window with the case's bucket and admission state.
//  2. Plan.
//  3. Assert the plan holds a positive capacity shortfall, creates and retires nothing itself, and asks the bridge for the capacity shortfall net of open temp creates and held to the room and the bucket, in any PoC mode, or for nothing where the window is shut.
func TestTheBridgeWindowAsksForTheCapacityShortfallHeldToTheBucket(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		change func(state *ModelState)
		want   func(capacity int) int
	}{
		{name: "a full bucket", change: func(*ModelState) {}, want: func(capacity int) int { return min(capacity, bucketCapacity) }},
		{name: "one token left", change: func(state *ModelState) { state.BucketTokens = 1 }, want: func(int) int { return 1 }},
		{name: "an empty bucket", change: func(state *ModelState) { state.BucketTokens = 0 }, want: func(int) int { return 0 }},
		{name: "outside the bridge window", change: func(state *ModelState) { state.InBridgeWindow = false }, want: func(int) int { return 0 }},
		{name: "an epoch PoC", change: func(state *ModelState) { state.RequestsBlocked, state.InEpochPoC = true, true }, want: func(capacity int) int { return min(capacity, bucketCapacity) }},
		{name: "requests blocked outside a confirmation PoC", change: func(state *ModelState) { state.RequestsBlocked = true }, want: func(capacity int) int { return min(capacity, bucketCapacity) }},
		{name: "no room under max unsettled", change: func(state *ModelState) { state.MaxUnsettled = 4 }, want: func(int) int { return 0 }},
		{name: "room for one", change: func(state *ModelState) { state.MaxUnsettled = 5 }, want: func(int) int { return 1 }},
		{name: "open temp creates cover the shortfall", change: func(state *ModelState) { state.PendingTempCreates = 100 }, want: func(int) int { return 0 }},
		{name: "one open temp create", change: func(state *ModelState) { state.PendingTempCreates = 1; state.BucketTokens = 5 }, want: func(capacity int) int { return min(capacity-1, 5) }},
		{name: "a confirmation PoC", change: func(state *ModelState) { state.RequestsBlocked, state.InConfirmationPoC = true, true }, want: func(capacity int) int { return min(capacity, bucketCapacity) }},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := drainedBridgeWindow(testCase.change)

			decision := Plan(state)

			require.Positive(t, decision.Shortfalls.Capacity, "capacity shortfall")
			require.Equal(t, testCase.want(decision.Shortfalls.Capacity), decision.BridgeRefill, "bridge refill")
			if state.InBridgeWindow {
				require.Empty(t, decision.Creates, "creates in the bridge window")
				require.Empty(t, decision.Retires, "retires in the bridge window")
			}
		})
	}
}

// Test flow:
//  1. Plan a bridge window whose temps are drained, then the same model with its temps full and routing marking nothing short.
//  2. Assert the drained pool asks for temps, and the full pool asks for none.
func TestAFullTempPoolAsksTheBridgeForNothing(t *testing.T) {
	t.Parallel()
	drained := Plan(drainedBridgeWindow(func(*ModelState) {}))
	full := Plan(drainedBridgeWindow(func(state *ModelState) {
		state.Escrows = []EscrowState{fullEscrow("t1", 1_000_000), fullEscrow("t2", 1_000_000)}
		state.Counted, state.DemandPeak, state.MoneyShort = 2, 0, false
	}))

	require.Positive(t, drained.BridgeRefill, "drained pool refill")
	require.Zero(t, full.BridgeRefill, "full pool refill")
}

// Test flow:
//  1. Table-driven: a bridge window with temp_count 1 and two full-context slots, holding the case's temps of the bridge label, none of them money short.
//  2. Plan.
//  3. Assert the bridge asks for the temps the temp count still lacks as full temps, so a full temp of the bridge label or an older label's full temp is counted correctly, and never for the slots the temp count does not cover.
func TestTheBridgeRefillsNoMoreFullTempsThanTheTempCount(t *testing.T) {
	t.Parallel()
	fullTemp := func(id string, label int64) EscrowState {
		escrow := fullEscrow(id, 1_000_000)
		escrow.Temp, escrow.Label = true, label
		return escrow
	}
	testCases := []struct {
		name    string
		escrows []EscrowState
		want    int
	}{
		{name: "a full temp of the bridge label", escrows: []EscrowState{fullTemp("t1", 8)}, want: 0},
		{name: "no temp", escrows: nil, want: 1},
		{name: "a starved temp", escrows: []EscrowState{starvedTemp("t1")}, want: 1},
		{name: "a full temp of an older label", escrows: []EscrowState{fullTemp("t1", 7)}, want: 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := modelWith(func(state *ModelState) {
				state.Escrows, state.Counted = testCase.escrows, len(testCase.escrows)
				state.TempCount, state.FullContextSlots, state.MaxUnsettled = 1, 2, 20
				state.InBridgeWindow, state.BridgeLabel, state.BucketTokens = true, 8, bucketCapacity
			})

			decision := Plan(state)

			require.Equal(t, testCase.want, decision.BridgeRefill, "bridge refill")
		})
	}
}
