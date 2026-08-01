package metrics

import (
	"devshard/cmd/gateway/engine"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// The request-level "failure" has no engine counterpart: attempts report failed, requests failure.
	outcomeFailure = "failure"

	pathKindInference = "inference"
	// statusNoCode is legacy's label for an attempt that failed without an upstream status.
	statusNoCode = "0"

	// hiddenFailureSeverity is the only severity legacy ever emitted.
	hiddenFailureSeverity = "protected"

	reasonNone             = "none"
	reasonNoAttempts       = "no_attempts"
	reasonEscrowMissing    = "escrow_missing"
	reasonBalanceExhausted = "balance_exhausted"
)

var latencyBuckets = prometheus.ExponentialBuckets(0.01, 2, 12)

// RaceRecorder satisfies the engine's metrics hook; every family here derives from one race outcome.
// See gateway-operations.md, "Metrics".
type RaceRecorder struct {
	attemptsStarted   *prometheus.CounterVec
	attemptsTerminal  *prometheus.CounterVec
	attemptFailures   *prometheus.CounterVec
	noWinnerAttempts  *prometheus.CounterVec
	userVisibleWins   *prometheus.CounterVec
	transportErrors   *prometheus.CounterVec
	requests          *prometheus.CounterVec
	criticalFailures  *prometheus.CounterVec
	hiddenFailures    *prometheus.CounterVec
	escalations       *prometheus.CounterVec
	timeoutActions    *prometheus.CounterVec
	inferenceTimeouts *prometheus.CounterVec
	carryOverflows    *prometheus.CounterVec

	receiptSeconds  *prometheus.HistogramVec
	firstContent    *prometheus.HistogramVec
	prefillPerToken *prometheus.HistogramVec
	totalAttempt    *prometheus.HistogramVec
}

func NewRaceRecorder(telemetry *Metrics) *RaceRecorder {
	recorder := &RaceRecorder{
		attemptsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_attempts_started_total",
			Help: "Total gateway attempts dispatched by participant, model, role, and start reason.",
		}, []string{"participant_key", "model", "role", "reason"}),
		attemptsTerminal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_attempts_terminal_total",
			Help: "Total gateway attempts by terminal outcome and visibility.",
		}, []string{"participant_key", "model", "role", "outcome", "visibility"}),
		attemptFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_attempt_failures_total",
			Help: "Total failed gateway attempts by bounded failure reason and visibility.",
		}, []string{"participant_key", "model", "role", "reason", "visibility"}),
		noWinnerAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_no_winner_attempts_total",
			Help: "Total attempts whose output could be given to nobody, by participant, model, and reason.",
		}, []string{"participant_key", "model", "reason"}),
		userVisibleWins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_user_visible_wins_total",
			Help: "Total user-visible winning responses by participant and model.",
		}, []string{"participant_key", "model"}),
		transportErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_participant_transport_errors_total",
			Help: "Total participant-bound request errors by participant, model, request kind, and upstream status.",
		}, []string{"participant_key", "model", "path_kind", "status"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_requests_total",
			Help: "Total gateway chat requests by model, user-visible outcome, and bounded reason.",
		}, []string{"model", "outcome", "reason"}),
		criticalFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_critical_user_failures_total",
			Help: "Total critical user-visible gateway failures by model and bounded reason.",
		}, []string{"model", "reason"}),
		hiddenFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_user_requests_with_hidden_failure_total",
			Help: "Total successful user requests that hid a gateway-visible attempt failure.",
		}, []string{"model", "severity", "reason"}),
		escalations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_escalation_decisions_total",
			Help: "Total escalation-policy decisions by reason.",
		}, []string{"reason"}),
		timeoutActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_timeout_actions_total",
			Help: "Total nonce timeout-vote actions by participant, model, kind, action, and reason.",
		}, []string{"participant_key", "model", "kind", "action", "reason"}),
		inferenceTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_inference_timeouts_total",
			Help: "Total handled inference timeouts by reason.",
		}, []string{"reason"}),
		carryOverflows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_stream_carry_overflow_total",
			Help: "Total SSE reassembly buffers that overflowed their carry budget.",
		}, []string{"participant_key", "model"}),
		receiptSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "devshard_gateway_participant_receipt_seconds",
			Help:    "Time from inference send until receipt confirmation by participant and model.",
			Buckets: latencyBuckets,
		}, []string{"participant_key", "model"}),
		firstContent: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "devshard_gateway_participant_first_content_seconds",
			Help:    "Time from inference send until first content by participant and model.",
			Buckets: latencyBuckets,
		}, []string{"participant_key", "model"}),
		prefillPerToken: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "devshard_gateway_participant_prefill_seconds_per_input_token",
			Help:    "Receipt-to-first-content time divided by input tokens, by participant and model.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		}, []string{"participant_key", "model"}),
		totalAttempt: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "devshard_gateway_participant_total_attempt_seconds",
			Help:    "Total inference attempt time by participant and model.",
			Buckets: latencyBuckets,
		}, []string{"participant_key", "model"}),
	}
	telemetry.Register(recorder.collectors()...)
	return recorder
}

func (r *RaceRecorder) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		r.attemptsStarted, r.attemptsTerminal, r.attemptFailures, r.noWinnerAttempts,
		r.userVisibleWins, r.transportErrors, r.requests, r.criticalFailures, r.hiddenFailures,
		r.escalations, r.timeoutActions, r.inferenceTimeouts, r.carryOverflows,
		r.receiptSeconds, r.firstContent, r.prefillPerToken, r.totalAttempt,
	}
}

func (r *RaceRecorder) RecordRace(outcome engine.RaceOutcome) {
	model := metricLabel(outcome.Model, labelUnknown)
	firstFailure := ""
	for _, attempt := range outcome.Attempts {
		labels := outcome.Labels(attempt)
		participant := metricLabel(labels.Participant, labelUnknown)
		role := metricLabel(labels.Role, labelUnknown)

		if !attempt.SendTime.IsZero() {
			r.attemptsStarted.WithLabelValues(participant, model, role, metricLabel(attempt.StartReason, role)).Inc()
		}
		r.attemptsTerminal.WithLabelValues(participant, model, role, labels.Outcome, labels.Visibility).Inc()

		if labels.Outcome == engine.AttemptOutcomeFailed {
			reason := metricLabel(labels.Reason, labelUnknown)
			r.attemptFailures.WithLabelValues(participant, model, role, reason, labels.Visibility).Inc()
			if status, upstream := transportStatus(attempt.Terminal); upstream {
				r.transportErrors.WithLabelValues(participant, model, pathKindInference, status).Inc()
			}
			if firstFailure == "" {
				firstFailure = reason
			}
		}
		switch labels.Visibility {
		case engine.VisibilityWinner:
			r.userVisibleWins.WithLabelValues(participant, model).Inc()
		case engine.VisibilityNoWinner:
			r.noWinnerAttempts.WithLabelValues(participant, model, metricLabel(labels.Reason, labelUnknown)).Inc()
		}
		r.observeAttemptLatency(participant, model, outcome.InputTokens, attempt)
	}
	r.escalations.WithLabelValues(metricLabel(outcome.Decision, labelUnknown)).Inc()
	r.recordRequest(model, outcome, firstFailure)
}

func (r *RaceRecorder) recordRequest(model string, outcome engine.RaceOutcome, firstFailure string) {
	if outcome.Succeeded {
		r.requests.WithLabelValues(model, engine.AttemptOutcomeSuccess, reasonNone).Inc()
		if firstFailure != "" {
			r.hiddenFailures.WithLabelValues(model, hiddenFailureSeverity, firstFailure).Inc()
		}
		return
	}
	reason := raceFailureReason(outcome, firstFailure)
	r.requests.WithLabelValues(model, outcomeFailure, reason).Inc()
	r.criticalFailures.WithLabelValues(model, reason).Inc()
}

func raceFailureReason(outcome engine.RaceOutcome, firstFailure string) string {
	switch {
	case outcome.Lifecycle.EscrowMissing:
		return reasonEscrowMissing
	case outcome.Lifecycle.BalanceExhausted:
		return reasonBalanceExhausted
	case len(outcome.Attempts) == 0:
		return reasonNoAttempts
	}
	return metricLabel(firstFailure, labelUnknown)
}

// transportStatus maps a terminal back to the upstream status the host answered with, reporting
// statusNoCode when none was recovered. See gateway-operations.md, "Cardinality rules".
func transportStatus(terminal engine.Terminal) (string, bool) {
	if status, recovered := engine.StatusFor(terminal); recovered {
		return strconv.Itoa(status), true
	}
	switch terminal {
	case engine.TerminalRejected, engine.TerminalDialFailure, engine.TerminalStreamTruncated,
		engine.TerminalUnexpectedEOF, engine.TerminalStalled:
		return statusNoCode, true
	}
	return "", false
}

func (r *RaceRecorder) observeAttemptLatency(participant, model string, inputTokens uint64, attempt engine.AttemptOutcome) {
	if seconds := elapsedSeconds(attempt.SendTime, attempt.ReceiptTime); seconds > 0 {
		r.receiptSeconds.WithLabelValues(participant, model).Observe(seconds)
	}
	if seconds := elapsedSeconds(attempt.SendTime, attempt.FirstToken); seconds > 0 {
		r.firstContent.WithLabelValues(participant, model).Observe(seconds)
	}
	if prefill := elapsedSeconds(attempt.ReceiptTime, attempt.FirstToken); prefill > 0 && inputTokens > 0 {
		r.prefillPerToken.WithLabelValues(participant, model).Observe(prefill / float64(inputTokens))
	}
	if seconds := elapsedSeconds(attempt.SendTime, attempt.Completed); seconds > 0 {
		r.totalAttempt.WithLabelValues(participant, model).Observe(seconds)
	}
}

func (r *RaceRecorder) RecordTimeout(event engine.TimeoutEvent) {
	participant := metricLabel(event.Participant, labelUnknown)
	model := metricLabel(event.Model, labelUnknown)
	reason := metricLabel(event.Reason, reasonNone)
	r.timeoutActions.WithLabelValues(
		participant, model, metricLabel(event.Kind, labelUnknown), metricLabel(event.Action, labelUnknown), reason,
	).Inc()
	if event.Action == engine.TimeoutActionCompleted || event.Action == engine.TimeoutActionFailed {
		r.inferenceTimeouts.WithLabelValues(reason).Inc()
	}
}

func (r *RaceRecorder) RecordClassifyOverflow(participant, model string) {
	r.carryOverflows.WithLabelValues(metricLabel(participant, labelUnknown), metricLabel(model, labelUnknown)).Inc()
}
