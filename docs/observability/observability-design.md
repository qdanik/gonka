# Join Observability Design

## Scope

This document explains how the join observability stack works today and why each piece exists.

It is specifically about the join deployment overlay in `deploy/join` and the operational surface we want to support right now.

It is not a migration plan to alternative backends. The current stack is the implementation.

## Design Goals

The join design is built around four practical goals:

1. Make request movement across `decentralized-api`, `versiond`, and `devshardd` observable.
2. Make service health and request rates visible without attaching debuggers to containers.
3. Let operators move from logs to traces from one UI.
4. Avoid hardcoded per-version scrape configuration for devshards.

## Components

### Jaeger

Jaeger receives OTLP traces from the application services and exposes the trace UI.

Why it exists:

- request tracing is the cleanest way to show where a public request was handed off
- `versiond` proxy spans are useful only if they can be inspected next to API and devshard spans

### Prometheus

Prometheus scrapes local metrics inside the join network.

Why it exists:

- `decentralized-api` and `devshardd` already expose metrics that are useful for request debugging
- service and request rate panels are much cheaper to reason about than raw logs for trend questions

### Grafana

Grafana is the operator entrypoint.

Why it exists:

- the join stack needs one place where metrics, logs, and traces can be inspected together
- dashboards are useful only if they can also drill into real request identifiers and trace links

### Loki and Promtail

Promtail ships Docker container logs into Loki.

Why they exist:

- join debugging still relies heavily on structured service logs
- logs need to sit next to metrics and traces rather than in a separate workflow
- Grafana can derive Jaeger links directly from `trace_id` fields in log lines

Implementation note:

Promtail currently follows the host Docker log files and Docker metadata. This is a deployment detail of the current join overlay, not a statement about the long-term collection model.

### cAdvisor

cAdvisor exports container resource metrics.

Why it exists:

- not every join problem is in request logic
- container pressure often explains symptoms that look like application faults

## Data Flows

### Trace Flow

1. A client request enters `decentralized-api`.
2. `decentralized-api` starts the public request span and emits structured request logs.
3. If the request crosses the versioned path, `versiond` creates a proxy request span and forwards trace context.
4. `devshardd` extracts that context, starts its own request span, and emits request logs and metrics.
5. All spans are exported to Jaeger over OTLP.

Result:

- one request can be followed across the real hand-off boundaries we operate today

### Metrics Flow

1. `decentralized-api` exposes Prometheus metrics directly.
2. `devshardd` exposes Prometheus metrics directly.
3. `versiond` exposes a Prometheus HTTP service-discovery endpoint for active devshard versions.
4. Prometheus scrapes the discovered targets and stores the samples locally.

Why the `versiond` discovery endpoint matters:

- active devshard versions can change over time
- Prometheus should discover them from the route table instead of relying on static port lists

### Log Flow

1. Join containers write structured logs to Docker stdout/stderr.
2. Promtail reads those logs, attaches Docker metadata such as compose service labels, and pushes them to Loki.
3. Grafana queries Loki for service logs, request logs, and drill-down views.
4. Grafana derives Jaeger trace links from `trace_id` fields found in log lines.

## Why The Dashboards Are Structured This Way

### Service Health Overview

Top-level view for the service operator persona.

Answers:

- Are API and devshard request rates healthy?
- Are latencies (p95) within acceptable range?
- Are error rates rising on either side?
- Is container resource pressure contributing to degradation?

### Request Debug & Logs

Request-centric view for the investigator persona.

Answers:

- What logs exist for a given `inference_id` or `requester_address`?
- Are errors correlated across API, versiond, and devshard?
- What was the runtime log volume in the time window?

### Infrastructure & Resources

Infrastructure health view.

Answers:

- Are Prometheus and cAdvisor healthy?
- Are containers within CPU and memory bounds?

### Inference Drilldown

Request-centric log filtering.

Filters by `inference_id` and `requester_address` across API and devshard logs.

### System Logs

Generic log surface for fast inspection by compose service.

Exists because not every debugging path starts from a request identifier.

## Current Gaps

The current design is useful, but it is intentionally incomplete.

The biggest remaining gaps are:

- first-class `inference-chain` observability
- validator and consensus health visibility
- bridge-specific metrics and dashboards
- richer correlation across logs, metrics, and traces
- actor-centric views for executors, validators, ML nodes, and participants
- alerting and SLOs

These are the next steps for the current stack. They should be addressed before any broad backend migration is considered.

## Why This Matters To The Project

This work improves the project in three concrete ways:

### Faster failure explanation

The team can move from “something failed” to “this service, this request path, this version, this runtime symptom” much faster.

### Safer rollout and routing visibility

Because `versiond` participates in observability, version routing stops being a blind spot.

### Better bridge between code changes and runtime behavior

The current services already expose useful structured behavior. The join stack makes that behavior inspectable in one place instead of scattered across terminals and container logs.

## Boundaries And Next Steps

The current design is intentionally local and pragmatic.

Good next steps after this stage are:

1. keep dashboard queries aligned with the real log topology as services evolve
2. extend request-level correlation where new services join the path
3. add missing telemetry for `inference-chain`, bridge, and network actors
4. introduce alerting only after dashboard baselines stabilize
5. decide later which parts of this join design should graduate into broader environment standards