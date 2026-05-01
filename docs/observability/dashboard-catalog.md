# Join Dashboard Catalog

## Purpose

This document explains the current Grafana dashboard set in operator terms.

It is meant to answer three questions:

1. Which dashboard should I open first?
2. Which dashboard answers which operational question?
3. Which dashboards are already useful today, and which ones still need refinement?

The dashboard catalog follows the current join stack and does not assume a migration to a different observability backend.

## Personas

### Service Operator

This persona wants to monitor whether the join environment is healthy and whether core services are degraded.

Primary questions:

- Are requests flowing?
- What are the latencies for API and devshard?
- Are error rates rising?
- Is degradation caused by resource pressure or application logic?

### Request Investigator

This persona wants to follow one request or one inference through the system.

Primary questions:

- Where did this request go?
- Which logs match this inference?
- Which trace belongs to this request?
- Did the request fail in API, routing, or devshard execution?

### Infrastructure Operator

This persona wants to monitor system health, container resources, and observability stack status.

Primary questions:

- Is the observability stack (Prometheus, cAdvisor) healthy?
- Are containers approaching CPU or memory limits?
- Is degradation related to resource saturation?

## Dashboard Map

| Dashboard | Persona | Main Question | File |
|---|---|---|---|
| **Service Health Overview** | service-operator | Is request flow healthy? What are latencies and errors? | `service-health-overview.json` |
| **Request Debug & Logs** | request-investigator | What happened to this request? Where are the logs? | `request-debug-logs.json` |
| **Infrastructure & Resources** | infrastructure-operator | Is the system resource-constrained? Are collectors healthy? | `infrastructure-resources.json` |
| **Inference Drilldown** | request-investigator | Logs filtered by inference_id or requester_address | `inference-drilldown.json` |
| **System Logs** | any | Raw container logs by service | `system-logs.json` |

## Dashboard Details

### Service Health Overview

**File**

- `deploy/join/observability/grafana/dashboards/service-health-overview.json`

**Persona**

- `service-operator` — wants to monitor API and devshard health, rates, latencies, errors

**Primary use**

- Top-level request-flow health check
- Spot latency increases or error rate rises
- Correlate API and devshard performance
- Understand resource pressure and container health simultaneously

**Current panels (10 total)**

*System Health (row 1):*
- `Decentralized API Up`
- `Active Devshard Versions`

*Service Metrics (row 2):*
- `API Active Operations`
- `Devshard Active Operations`

*Latencies (row 3):*
- `API Duration p95`
- `Devshard Duration p95`

*Errors (row 4):*
- `API Error Rate`
- `Devshard Error Rate`

*Infrastructure (row 5):*
- `Container CPU Usage`
- `Container Memory Working Set`

**What it answers well today**

- Is traffic flowing?
- Are latencies within acceptable range?
- Are error rates rising?
- Is one component significantly slower than the other?
- Is resource pressure contributing to degradation?
- Are all devshard versions active?

**Current gaps**

- No visibility into validator/consensus latency
- No bridge health or pending transaction metrics
- No per-executor or per-ML-node breakdown
- No model-specific latency trends

### Request Debug & Logs

**File**

- `deploy/join/observability/grafana/dashboards/request-debug-logs.json`

**Persona**

- `request-investigator` — wants to follow a single request through all system components

**Primary use**

- Primary dashboard for debugging a specific inference
- Correlate request rates with error logs
- Search by inference_id, requester, or error pattern
- Link metrics anomalies to runtime events

**Current panels (9 total)**

*Request Flow (row 1):*
- `API Request Rate`
- `Devshard Request Rate`

*Request Logs (row 2):*
- `API Inference Request Logs`
- `Devshard Request Logs By Requester`

*Runtime Insights (row 3):*
- `API and Versiond Error Logs`
- `Devshard Runtime Logs By Inference`
- `Matching Log Volume`

*Additional Logs (row 4):*
- `Versiond Logs`
- `Versiond and Devshard Runtime Logs`

**What it answers well today**

- Which requests failed?
- What do the logs say about this requester?
- Are there correlated errors across services?
- What was the error volume in the time window?
- Which Versiond logs are relevant?

**Current gaps**

- Log fields are not yet structured (no `inference_id`, `model`, `executor` labels)
- Trace IDs not yet linked to logs
- Cannot easily filter by model or executor
- No unified "request journey" view across metrics, logs, and traces

### Infrastructure & Resources

**File**

- `deploy/join/observability/grafana/dashboards/infrastructure-resources.json`

**Persona**

- `infrastructure-operator` — wants to monitor system health, container resources, and collector status

**Primary use**

- Diagnose infrastructure pressure (CPU, memory)
- Monitor observability stack health (Prometheus, cAdvisor)
- Correlate container behavior with application degradation

**Current panels (5 total)**

*Observability Stack Health (row 1):*
- `Prometheus Up`
- `cAdvisor Up`
- `Decentralized API Up`

*Container Resources (row 2):*
- `Container CPU Usage`
- `Container Memory Working Set`

**What it answers well today**

- Is the observability stack healthy?
- Are containers approaching CPU or memory limits?
- Is degradation related to resource saturation?
- Are all services responsive?

**Current gaps**

- No host-level metrics (disk, network I/O)
- No per-container memory pressure breakdown
- No long-term capacity trends
- No Node Exporter integration

### Inference Drilldown

**File**

- `deploy/join/observability/grafana/dashboards/inference-drilldown.json`

**Persona**

- `request-investigator` — wants to narrow logs to a specific request without switching dashboards

**Primary use**

- Filter API and devshard logs by `inference_id`
- Filter by `requester_address`
- Check runtime log volume around a specific request window

**What it answers well today**

- Which API logs match this inference?
- Which devshard request logs correspond to this requester?
- What was the log volume around this event?

**Current gaps**

- Log fields not yet structured (no explicit `inference_id`, `executor`, `validator` labels)
- No trace linking from log lines to Jaeger spans

### System Logs

**File**

- `deploy/join/observability/grafana/dashboards/system-logs.json`

**Persona**

- any operator who wants fast unfiltered log access

**Primary use**

- Browse raw container logs by compose service
- Spot errors without a specific request identifier in hand

**What it answers well today**

- What is this service currently logging?
- Are there recent errors in any container?

## Recommended Operator Entry Flow

### Service operator checking overall health:

1. Open **Service Health Overview**.
2. Review service rates, latencies (p95), and error rates.
3. Check container resources at the bottom to rule out infrastructure pressure.
4. If a specific metric is wrong, move to **Request Debug & Logs** for log context.

### Request investigator debugging a specific failure:

1. Open **Inference Drilldown** if you have an `inference_id` or `requester_address`.
2. Open **Request Debug & Logs** for broader log and rate context.
3. Use Grafana trace links from log lines to jump to Jaeger.

### Infrastructure operator checking resource health:

1. Open **Infrastructure & Resources**.
2. Verify Prometheus and cAdvisor are up.
3. Check container CPU and memory trends.
4. Cross-reference with **Service Health Overview** if application degradation is suspected.

## Current Refinement Backlog

The current dashboard set is already useful, but the next refinements should focus on:

1. Better request correlation across logs, metrics, and traces.
2. Structured log field extraction (inference_id, requester_address, model, executor).
3. First-class `inference-chain` visibility.
4. Bridge and actor-level dashboards.
5. Optional host-level visibility only if container-level metrics prove insufficient.
