package metrics

import (
	"context"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/store"
)

func newLimitsHarness(clock *time.Time) (*limits.GatewayLimiter, *limits.Capacity, *limits.ParticipantLimiter) {
	limiter := limits.NewGatewayLimiter(limits.GatewayConfig{MaxConcurrent: 8, MaxInputTokens: 4000})
	participants := limits.NewParticipantLimiter(limits.ParticipantConfig{
		Initial: 4, Max: 16, AfterFailures: 1,
		BaseOpen: time.Minute, MaxOpen: time.Minute,
	}, func() time.Time { return *clock })
	capacity := limits.NewCapacity(participants.Available)
	return limiter, capacity, participants
}

// servingCapacity is the composition root's wrapper reduced to what a collector reads: production
// applies the operator's relaxed-mode override before asking for the ratio, and these tests are never
// blocked, so the answer is always the unblocked one.
type servingCapacity struct{ *limits.Capacity }

func (c servingCapacity) ScaleFactor(model string) float64 {
	return c.Capacity.ScaleFactor(model, false)
}

func TestTheLimitsCollectorReportsTheConfiguredCapsBeforeAnyTraffic(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	limiter, capacity, participants := newLimitsHarness(&clock)
	telemetry := New()
	telemetry.Register(NewLimitsCollector(LimitsSources{
		Limiter: limiter, Capacity: servingCapacity{capacity}, Participants: participants,
		Models: func() []string { return []string{"qwen"} },
	}))

	expectGauge(t, telemetry, "devshard_gateway_inflight_requests", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_inflight_input_tokens", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_effective_max_concurrent_requests", labels{}, 8)
	expectGauge(t, telemetry, "devshard_gateway_effective_max_input_tokens_in_flight", labels{}, 4000)
	expectGauge(t, telemetry, "devshard_gateway_participants_tracked", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_participants_exhausted", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_inflight_requests_by_model", labels{"model": "qwen"}, 0)
	expectSeriesCount(t, telemetry, "devshard_gateway_participant_window_size", 0)
}

func TestTheLimitsCollectorMatchesTheLimiterAfterTraffic(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	limiter, capacity, participants := newLimitsHarness(&clock)
	telemetry := New()
	telemetry.Register(NewLimitsCollector(LimitsSources{
		Limiter: limiter, Capacity: servingCapacity{capacity}, Participants: participants,
		Models: func() []string { return []string{"qwen"} },
	}))

	if err := limiter.AcquireForModel(context.Background(), "qwen", 250, limits.ModelCapacity{ScaleFactor: 0.5}); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	expectGauge(t, telemetry, "devshard_gateway_inflight_requests", labels{}, 1)
	expectGauge(t, telemetry, "devshard_gateway_inflight_input_tokens", labels{}, 250)
	expectGauge(t, telemetry, "devshard_gateway_inflight_requests_by_model", labels{"model": "qwen"}, 1)
	expectGauge(t, telemetry, "devshard_gateway_inflight_input_tokens_by_model", labels{"model": "qwen"}, 250)
	expectGauge(t, telemetry, "devshard_gateway_limiter_queue_depth", labels{"model": "qwen"}, 0)
	// The unlabelled pair reports what was configured; the caps a model is actually judged against are
	// per model, because overrides and capacity weights apply per model.
	expectGauge(t, telemetry, "devshard_gateway_effective_max_concurrent_requests", labels{}, 8)
	expectGauge(t, telemetry, "devshard_gateway_effective_max_input_tokens_in_flight", labels{}, 4000)
	expectGauge(t, telemetry, "devshard_gateway_enforced_max_concurrent_requests_by_model", labels{"model": "qwen"}, 4)
	expectGauge(t, telemetry, "devshard_gateway_enforced_max_input_tokens_by_model", labels{"model": "qwen"}, 2000)
}

func TestTheLimitsCollectorReportsCapacityPerModel(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	limiter, capacity, participants := newLimitsHarness(&clock)
	capacity.Update(chain.PhaseSnapshot{
		CurrentWeightsByModel: map[string]map[string]float64{"qwen": {"gonka1a": 30, "gonka1b": 20}},
		FullWeightsByModel:    map[string]map[string]float64{"qwen": {"gonka1a": 60, "gonka1b": 40}},
	})
	telemetry := New()
	telemetry.Register(NewLimitsCollector(LimitsSources{
		Limiter: limiter, Capacity: servingCapacity{capacity}, Participants: participants,
		Models: func() []string { return []string{"qwen"} },
	}))

	expectGauge(t, telemetry, "devshard_gateway_capacity_total_weight_by_model", labels{"model": "qwen"}, 50)
	expectGauge(t, telemetry, "devshard_gateway_capacity_baseline_weight_by_model", labels{"model": "qwen"}, 100)
	expectGauge(t, telemetry, "devshard_gateway_capacity_scale_by_model", labels{"model": "qwen"}, 0.5)
}

// Routing on the membership-share fallback serves requests and looks like health; the gauge is the
// only thing that separates it from routing on real chain weights.
func TestTheLimitsCollectorReportsWhileEscrowScoringRunsOnTheMembershipFallback(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	limiter, capacity, participants := newLimitsHarness(&clock)
	capacity.SetEscrowMembership("7", map[string]float64{"gonka1a": 0.5, "gonka1b": 0.25})
	telemetry := New()
	telemetry.Register(NewLimitsCollector(LimitsSources{
		Limiter: limiter, Capacity: servingCapacity{capacity}, Participants: participants,
		Models: func() []string { return []string{"qwen"} },
	}))

	if weight := capacity.EscrowWeight("7", "qwen"); weight != 0.75 {
		t.Fatalf("EscrowWeight with no chain weights = %v, want the 0.75 membership share: this test never reached the fallback", weight)
	}
	expectGauge(t, telemetry, "devshard_gateway_capacity_weights_unobserved_by_model", labels{"model": "qwen"}, 1)

	capacity.Update(chain.PhaseSnapshot{
		CurrentWeightsByModel: map[string]map[string]float64{"qwen": {"gonka1a": 30, "gonka1b": 20}},
		FullWeightsByModel:    map[string]map[string]float64{"qwen": {"gonka1a": 60, "gonka1b": 40}},
	})

	if weight := capacity.EscrowWeight("7", "qwen"); weight != 20 {
		t.Fatalf("EscrowWeight after the chain reported = %v, want 20: scoring is still on the fallback", weight)
	}
	expectGauge(t, telemetry, "devshard_gateway_capacity_weights_unobserved_by_model", labels{"model": "qwen"}, 0)
}

func TestTheLimitsCollectorReportsAnOpenCutoffAsExhausted(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	limiter, capacity, participants := newLimitsHarness(&clock)
	telemetry := New()
	telemetry.Register(NewLimitsCollector(LimitsSources{
		Limiter: limiter, Capacity: servingCapacity{capacity}, Participants: participants,
		Models: func() []string { return nil },
	}))

	participants.OnResult("gonka1down", "qwen", limits.TransportFault)

	host := labels{"participant_key": "gonka1down", "model": "qwen"}
	expectGauge(t, telemetry, "devshard_gateway_participants_tracked", labels{}, 1)
	expectGauge(t, telemetry, "devshard_gateway_participants_exhausted", labels{}, 1)
	expectGauge(t, telemetry, "devshard_gateway_participant_window_size", host, 4)
	expectGauge(t, telemetry, "devshard_gateway_participant_window_inflight", host, 0)
	expectGauge(t, telemetry, "devshard_gateway_participant_breaker_state",
		labels{"participant_key": "gonka1down", "model": "qwen", "state": "open"}, 1)
	expectGauge(t, telemetry, "devshard_gateway_participant_breaker_state",
		labels{"participant_key": "gonka1down", "model": "qwen", "state": "closed"}, 0)
}

func TestThePerfCollectorMatchesTheTracker(t *testing.T) {
	configuration := config.Defaults()
	clock := time.Unix(1700000000, 0)
	tracker := perf.NewTracker(config.NewHolder(&configuration), func() time.Time { return clock })
	telemetry := New()
	telemetry.Register(NewPerfCollector(tracker))

	expectSeriesCount(t, telemetry, "devshard_gateway_host_ejected", 0)

	tracker.RecordSample(perf.Sample{ParticipantKey: "gonka1host", Model: "qwen", Responsive: true})
	tracker.Acquire("gonka1host")

	expectGauge(t, telemetry, "devshard_gateway_host_ejected", labels{"participant_key": "gonka1host", "model": "qwen"}, 0)
	expectGauge(t, telemetry, "devshard_gateway_host_inflight_requests", labels{"participant_key": "gonka1host"}, 1)

	// Unmeasured hosts publish no series at all, so a scrape never shows an invented zero.
	expectSeriesCount(t, telemetry, "devshard_gateway_host_seconds_per_output_token", 0)

	for range 10 {
		tracker.RecordSample(perf.Sample{ParticipantKey: "gonka1host", Model: "qwen", Responsive: true, TimePerOutputToken: 25 * time.Millisecond})
	}

	expectGauge(t, telemetry, "devshard_gateway_host_seconds_per_output_token",
		labels{"participant_key": "gonka1host", "model": "qwen"}, 0.025)
}

type fixedEscrows struct{ states []registry.EscrowState }

func (f fixedEscrows) Snapshot() []registry.EscrowState { return f.states }

func TestTheRegistryCollectorReportsEveryPublishedEscrow(t *testing.T) {
	telemetry := New()
	telemetry.Register(NewRegistryCollector(RegistrySources{
		Escrows: fixedEscrows{states: []registry.EscrowState{
			{ID: "7", Model: "qwen", Accepting: true, InFlight: 3, Participants: []string{"gonka1a", "gonka1b"}},
			{ID: "9", Model: "qwen", Accepting: false, InFlight: 0, Participants: []string{"gonka1a"}},
		}},
		EscrowWeight: func(escrowID, _ string) float64 {
			if escrowID == "7" {
				return 42
			}
			return 0
		},
		Available:          func(participant, _ string) bool { return participant != "gonka1b" },
		DrainCloseFailures: func() int64 { return 4 },
	}))

	expectGauge(t, telemetry, "devshard_runtime_active", labels{"devshard_id": "7", "model": "qwen"}, 1)
	expectGauge(t, telemetry, "devshard_runtime_active", labels{"devshard_id": "9", "model": "qwen"}, 0)
	expectGauge(t, telemetry, "devshard_runtime_active_requests", labels{"devshard_id": "7", "model": "qwen"}, 3)
	expectGauge(t, telemetry, "devshard_gateway_escrow_weight", labels{"devshard_id": "7"}, 42)
	expectGauge(t, telemetry, "devshard_gateway_escrow_blocked_participants", labels{"devshard_id": "7", "model": "qwen"}, 1)
	expectGauge(t, telemetry, "devshard_gateway_escrow_participant_limited", labels{"devshard_id": "7", "model": "qwen"}, 1)
	expectGauge(t, telemetry, "devshard_gateway_escrow_blocked_participants", labels{"devshard_id": "9", "model": "qwen"}, 0)
	expectGauge(t, telemetry, "devshard_gateway_escrow_participant_limited", labels{"devshard_id": "9", "model": "qwen"}, 0)
	expectCounter(t, telemetry, "devshard_gateway_escrow_drain_close_failures_total", labels{}, 4)
}

func TestTheRegistryCollectorIsSilentOnAnEmptyRegistry(t *testing.T) {
	telemetry := New()
	telemetry.Register(NewRegistryCollector(RegistrySources{Escrows: fixedEscrows{}}))

	expectSeriesCount(t, telemetry, "devshard_runtime_active", 0)
	expectSeriesCount(t, telemetry, "devshard_gateway_escrow_weight", 0)
}

type fixedPhases struct{ snapshot chain.PhaseSnapshot }

func (f fixedPhases) Snapshot() chain.PhaseSnapshot { return f.snapshot }

func TestTheChainCollectorMatchesThePublishedSnapshot(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	telemetry := New()
	telemetry.Register(NewChainCollector(fixedPhases{snapshot: chain.PhaseSnapshot{
		BlockHeight:            904,
		EpochSwitchBlockHeight: 1000,
		EpochIndex:             12,
		EpochPhase:             chain.EpochPhasePoCValidate,
		RequestsBlocked:        true,
		BlockReason:            chain.BlockReasonPoC,
		MaxNonce:               19800,
		LastUpdatedAt:          clock.Add(-3 * time.Second),
		LastError:              "dial tcp: refused",
	}}, func() time.Time { return clock }))

	expectGauge(t, telemetry, "devshard_gateway_chain_block_height", labels{}, 904)
	expectGauge(t, telemetry, "devshard_gateway_chain_epoch_switch_block_height", labels{}, 1000)
	expectGauge(t, telemetry, "devshard_gateway_chain_epoch_index", labels{}, 12)
	expectGauge(t, telemetry, "devshard_gateway_chain_max_nonce", labels{}, 19800)
	expectGauge(t, telemetry, "devshard_gateway_chain_requests_blocked", labels{}, 1)
	expectGauge(t, telemetry, "devshard_gateway_chain_snapshot_age_seconds", labels{}, 3)
	expectGauge(t, telemetry, "devshard_gateway_chain_snapshot_healthy", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_chain_epoch_phase", labels{"phase": "PoCValidate"}, 1)
	expectGauge(t, telemetry, "devshard_gateway_chain_epoch_phase", labels{"phase": "Inference"}, 0)
	expectGauge(t, telemetry, "devshard_gateway_chain_block_reason", labels{"reason": "poc"}, 1)
	expectGauge(t, telemetry, "devshard_gateway_chain_block_reason", labels{"reason": "none"}, 0)
}

func TestTheChainCollectorReportsTheZeroSnapshot(t *testing.T) {
	clock := time.Unix(1700000000, 0)
	telemetry := New()
	telemetry.Register(NewChainCollector(fixedPhases{}, func() time.Time { return clock }))

	expectGauge(t, telemetry, "devshard_gateway_chain_block_height", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_chain_requests_blocked", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_chain_snapshot_age_seconds", labels{}, 0)
	expectGauge(t, telemetry, "devshard_gateway_chain_snapshot_healthy", labels{}, 1)
	expectGauge(t, telemetry, "devshard_gateway_chain_block_reason", labels{"reason": "none"}, 1)
}

type fixedLedger struct{ stats store.LedgerStats }

func (f fixedLedger) Stats() store.LedgerStats { return f.stats }

// A row the ledger sheds under load never reaches the request history, and the only place that loss is
// visible is the exposition: nothing else in the process reports it.
func TestTheAccountingCollectorReportsEveryRowTheLedgerLost(t *testing.T) {
	telemetry := New()
	telemetry.Register(NewAccountingCollector(fixedLedger{
		stats: store.LedgerStats{Written: 91, Dropped: 7, Failed: 2, SweepFailed: 3},
	}))

	expectCounter(t, telemetry, "devshard_gateway_accounting_rows_written_total", labels{}, 91)
	expectCounter(t, telemetry, "devshard_gateway_accounting_rows_lost_total", labels{"cause": "shed"}, 7)
	expectCounter(t, telemetry, "devshard_gateway_accounting_rows_lost_total", labels{"cause": "write_failed"}, 2)
	expectCounter(t, telemetry, "devshard_gateway_accounting_retention_sweeps_failed_total", labels{}, 3)
}
