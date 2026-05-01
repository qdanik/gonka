# Join Observability Roadmap

## Purpose

This roadmap turns the current observability comparison into an execution plan.

The default implementation is the current join stack:

- `Prometheus`
- `Grafana`
- `Loki`
- `Promtail`
- `Jaeger`
- `otel-collector`
- `cAdvisor`

## Working Rules

1. Keep the current join stack as the source of truth.
2. Improve dashboards and instrumentation before considering backend replacement.
3. Separate dashboard-only work from instrumentation work.
4. Treat architecture migration as a later decision, not a near-term one.

## Related Documents

- `docs/observability/observability-overview.md` — canonical current-state overview
- `docs/observability/observability-design.md` — current join design and boundaries
- `docs/observability/dashboard-catalog.md` — current dashboard set mapped to personas and operator questions

## Phase 1 — Canonicalize Current Observability ✓ COMPLETE

### Status

Phase 1 completed May 4, 2026. Current status:
- ✓ `docs/observability/observability-overview.md` — canonical current-state
- ✓ `docs/observability/observability-design.md` — stack-first alignment
- ✓ `docs/observability/observability-roadmap.md` — repo-local 7-phase backlog
- ✓ `docs/observability/dashboard-catalog.md` — dashboard→persona→question mapping

### Goal

Make the current observability story accurate and easy to understand.

### Deliverables

- Canonical current-state overview for the repo
- Clear explanation of the current join stack

### Tasks

- Document the current stack topology and data flow.
- Describe existing dashboards and their purpose.
- Describe the current `decentralized-api` and `devshard` telemetry surface.

### Priority

- `P0`

## Phase 2 — Productize Existing Dashboards 🔄 IN PROGRESS

### Status

Phase 2 started May 4, 2026. Current progress:
- ✓ Analyzed all 5 dashboards (8–16K each, 30 total panels)
- ✓ Identified 3 duplicate panels (API Active Operations, API Duration p95, API Error Rate)
- ✓ Created 3 personas-first dashboards:
  - `service-health-overview.json` (10 panels, service-operator persona)
  - `request-debug-logs.json` (9 panels, request-investigator persona)
  - `infrastructure-resources.json` (5 panels, infrastructure-operator persona)
- ✓ Added resource metrics (Container CPU, Memory) to service overview
- ✓ Merged logs from inference-drilldown into request debug
- ✓ Removed duplicates from infrastructure dashboard
- ✓ Updated dashboard-catalog.md with new structure
- 🔄 Remaining: Deploy and test new dashboards in Grafana UI

### Goal

Turn the current dashboards into a clean operator-facing surface.

### Deliverables

- Dashboard set organized around real user questions
- Consistent dashboard naming, filters, and navigation
- Clear split between overview, request flow, logs, and resources

### Tasks

- Define dashboard personas:
  - network operator
  - service operator
  - request investigator
- Normalize the dashboard set around those personas.
- Make sure each dashboard answers a clear operational question.
- Remove redundant or low-signal panels.

### Priority

- `P0`

## Phase 3 — Improve Correlation Across Logs, Metrics, and Traces

### Goal

Make one request easy to follow end-to-end.

### Deliverables

- Better request-centric drilldown
- Better trace links from logs
- Better structured log fields for operational queries

### Tasks

- Enrich logs with stable request fields where missing:
  - `inference_id`
  - `requester`
  - `model`
  - `trace_id`
  - `executor`
  - `validator`
- Improve Loki parsing and labels where safe.
- Strengthen Grafana derived fields and trace links.
- Refine request drilldown around `inference_id` and `requester_address`.

### Priority

- `P0`

## Phase 4 — Add Missing Telemetry For Critical Components

### Goal

Close the largest visibility gaps in the current system.

### Deliverables

- First-class `inference-chain` observability
- Bridge observability
- Better visibility into versioned routing when needed

### Tasks

- Add `inference-chain` metrics for:
  - block height
  - block time
  - validator participation
  - consensus lag or halt indicators
  - tx success and failure visibility
- Add bridge metrics and dashboards:
  - pending operations
  - confirmation lag
  - error rate
  - retry behavior
- Add versiond request or proxy metrics if routing becomes a regular operational concern.

### Priority

- `P0`

## Phase 5 — Add Network Actor Dashboards

### Goal

Expose the health of network actors, not only services.

### Deliverables

- Executor health dashboard
- Validator health dashboard
- ML node health dashboard
- Participant or version health dashboard

### Tasks

- Add per-executor latency and failure metrics.
- Add per-validator validation latency and outcome metrics.
- Add ML node availability and performance metrics.
- Add version comparison views for devshard-related traffic.
- Add participant-centric views if the audience extends beyond join operators.

### Priority

- `P1`

## Phase 6 — Add Host-Level Visibility Only If Needed

### Goal

Add host or VM visibility only if container metrics are insufficient.

### Deliverables

- Optional Node Exporter integration
- Optional host-level dashboard
- Platform caveat documentation

### Tasks

- Decide whether host-level metrics are actually needed.
- If yes, add Node Exporter to the join stack.
- Add a host-level resource dashboard.
- Document macOS or Docker Desktop caveats if host-level metrics are introduced.

### Priority

- `P1`

## Phase 7 — Add Alerting And SLOs

### Goal

Move from passive observability to active operations.

### Deliverables

- Prometheus alerting rules
- Service SLO definitions
- Runbook entry points from alerts into dashboards

### Tasks

- Define latency and error-rate SLOs.
- Add alerts for:
  - chain halt or lag
  - sustained bridge failures
  - validation degradation
  - service unavailability
  - abnormal latency growth
- Connect alerts to the right dashboards and drilldowns.

### Priority

- `P1`

## Phase 8 — Review Architecture Only If The Current Stack Becomes Limiting

### Goal

Keep architecture migration strictly need-driven.

### Deliverables

- Explicit decision record on whether backend changes are necessary
- No stack migration without concrete justification

### Tasks

- Reassess the stack only if one of the following becomes true:
  - Prometheus retention or scale is insufficient
  - collector sprawl becomes hard to operate
  - Jaeger no longer satisfies trace workflows
  - remote-write or long-term metrics storage becomes necessary
- Compare current stack against any replacement on:
  - migration cost
  - operator usability
  - performance
  - compatibility
  - maintenance burden

### Priority

- `P2`

## Priority Summary

### `P0`

- Canonicalize current observability docs.
- Productize existing dashboards.
- Improve logs, metrics, and traces correlation.
- Add missing telemetry for `inference-chain` and bridge.

### `P1`

- Add actor-centric dashboards.
- Add host-level telemetry if it is actually needed.
- Add alerting and SLOs.

### `P2`

- Revisit alternative backends only if the current stack proves insufficient.

## Immediate Next Slice

The next practical slice should be:

1. Keep the docs truthful and current.
2. Normalize the dashboard set around user questions.
3. Improve request correlation through better labels, log fields, and trace links.
4. Define the first missing metrics for `inference-chain` and bridge.