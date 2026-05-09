# Observability Design

This document describes the current observability design for the join stack and the staged dashboard/metrics changes. Older observability docs were removed because they described a dashboard layout and signal model that no longer matches the code.

## Goals

- Give operators three focused Grafana entry points: participant state, network/setup state, and devshard runtime state.
- Keep application instrumentation local to the service that owns the behavior.
- Keep Prometheus collectors pure: collectors expose snapshots, while provider packages assemble snapshots from application state.
- Keep observability out of consensus-critical state transitions. Tracing, metrics, and dashboards must not affect chain behavior.
- Prefer existing join observability services: Prometheus, Grafana, Loki, Promtail, Jaeger, and the OTEL collector.

## Local Stack

The local join observability stack is provisioned under `deploy/join/observability` and is expected to run with the existing join compose setup.

| Component | Purpose |
| --- | --- |
| Prometheus | Scrapes decentralized-api and devshard metric endpoints. |
| Grafana | Provisions the three dashboards from `deploy/join/observability/grafana/dashboards`. |
| Loki | Stores container logs collected by Promtail. |
| Promtail | Collects join container logs and attaches compose labels such as `compose_project` and `compose_service`. |
| Jaeger | Stores and displays OpenTelemetry traces. |
| OTEL Collector | Receives OTLP traffic and fans out traces/metrics to the configured backends. |

## Runtime Services

### decentralized-api

`decentralized-api` owns public/admin API instrumentation and exports its Prometheus metrics through the existing metrics handler in `decentralized-api/observability`.

It currently exposes:

- inference operation counters, duration histograms, error counters, and token counters;
- participant snapshot metrics through `observability/participant`;
- setup report metrics through `observability/setupreport`;
- approved devshard version info via the devshard approved versions collector.

The participant collector follows a two-package pattern:

- `observability/participant` defines the Prometheus collector and snapshot shape;
- `observability/participantprovider` reads application state and builds the snapshot.

The setup report collector follows the same pattern:

- `observability/setupreport` defines metric descriptors and snapshot shape;
- `observability/setupreportprovider` adapts `admin.GetCachedReport()` into a Prometheus snapshot.

This separation prevents collector packages from importing high-level application dependencies and avoids import cycles.

### devshardd

`devshardd` owns devshard HTTP request metrics, lifecycle metrics, queue/mempool gauges, and ML inference execution metrics.

The staged inference metrics are:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `devshard_inference_total` | counter | `result`, `escrow_id` | Completed or failed ML engine executions. |
| `devshard_inference_execution_duration_seconds` | histogram | `escrow_id` | Duration of the `engine.Execute` call. |
| `devshard_inference_tokens_total` | counter | `token_type`, `escrow_id` | Input and output tokens processed by the inference engine. |

`devshard/observability/inference_trace.go` starts an OpenTelemetry span around each ML inference execution. Successful executions record token counts and duration. Failed executions mark the span as an error and increment the failed inference counter.

### Embedded devshard routes

Lazy devshard session routes are wrapped with `srv.ObservabilityMiddleware`, so HTTP request duration/error metrics are emitted for payload handling. The route-level request metrics are useful in the Devshard Details dashboard for separating engine latency from HTTP and queue behavior.

## OpenTelemetry

OpenTelemetry is opt-in and controlled by service-specific enable flags plus shared endpoint configuration.

| Env var | Used by | Meaning |
| --- | --- | --- |
| `DAPI_OTEL_ENABLED` | decentralized-api | Enables dapi OpenTelemetry initialization. |
| `DEVSHARD_OTEL_ENABLED` | devshardd | Enables devshard OpenTelemetry initialization. |
| `OTEL_ENDPOINT` | both | OTLP gRPC endpoint, usually the OTEL collector. |
| `OTEL_HEADERS` | both | Optional OTLP headers. |

`decentralized-api/observability.BuildProviders` builds reusable trace and metric providers without setting them globally. This is used by `decentralized-api/cmd/devshardd/main.go` so the devshard binary can share the same exporter setup while using `service.name="devshardd"`.

When devshard receives injected providers through `devshard/observability.WithOtelProviders`, it sets the global tracer/meter providers and skips building its own exporters. When no endpoint is configured, devshard falls back to Prometheus-only metrics.

## Prometheus Metrics

### Setup Report Metrics

The setup report collector exports the cached admin setup report as numeric Prometheus gauges:

| Metric | Meaning |
| --- | --- |
| `decentralized_api_setup_overall_status` | Encoded overall report status: `2=PASS`, `1=UNAVAILABLE`, `0=FAIL`. |
| `decentralized_api_setup_check_status{check_id}` | Encoded status for each setup check. |
| `decentralized_api_block_height` | Latest chain block height from the `block_sync` check details. |
| `decentralized_api_seconds_since_block` | Seconds since the latest observed block. |
| `decentralized_api_setup_checks_passed` | Number of passed setup checks. |
| `decentralized_api_setup_checks_failed` | Number of failed setup checks. |
| `decentralized_api_setup_checks_unavailable` | Number of unavailable setup checks. |

The source report is still generated by `/admin/v1/setup/report`; the collector reads the latest cached report. This means the metrics depend on the cache being populated by the admin report path. If the report has never been generated, the collector currently emits no setup-report metrics.

### Participant Metrics

Participant metrics are exported from the participant snapshot collector and are consumed by the Participant Details dashboard:

| Metric | Used for |
| --- | --- |
| `decentralized_api_participant_info` | Participant identity, validator key, epoch status, chain status, and phase. |
| `decentralized_api_participant_model_status` | Model coverage and delegation state. |
| `decentralized_api_participant_epoch_rewarded_gnk` | Rewarded GNK for recent epochs. |
| `decentralized_api_participant_mlnode_effective_weight` | ML node effective weight by node/model/status. |

### Devshard Runtime Metrics

Devshard runtime metrics are consumed by the Devshard Details dashboard:

| Metric | Used for |
| --- | --- |
| `devshard_inference_total` | Inference throughput and success/failure rate. |
| `devshard_inference_execution_duration_seconds_bucket` | p50/p95/p99 engine execution latency. |
| `devshard_inference_tokens_total` | Input/output token throughput. |
| `devshard_lifecycle_interruption_total` | Execution interruption rate by reason. |
| `devshard_validation_queue_depth` | Pending validation queue depth. |
| `devshard_mempool_size` | Devshard unsigned transaction queue size. |
| `devshard_request_duration_seconds_bucket` | HTTP request latency by route. |
| `devshard_request_errors_total` | HTTP request error rate by route/status. |
| `decentralized_api_devshard_approved_version_info` | Approved devshard versions observed by dapi. |

## Dashboards

Only three dashboards should remain provisioned for this observability slice.

### Participant Details

File: `deploy/join/observability/grafana/dashboards/participant-details.json`

Purpose: show local participant identity and business state from decentralized-api participant metrics.

Main panels:

- Participant Identity
- Model Coverage And Delegation
- Rewarded GNK Last 5 Epochs
- ML Nodes Overview

### Network Details

File: `deploy/join/observability/grafana/dashboards/network-details.json`

Purpose: show setup report status and block sync health.

Main panels:

- Overall Status
- Checks Passed / Failed / Unavailable
- Individual setup checks
- Validator in Set visual status
- MLNode Checks rollup
- Block Height
- Seconds Since Last Block

The `validator_in_set` panel is visually treated as informational in Grafana, but the backend setup report currently still includes all failed checks in `OverallStatus` and `FailedChecks`.

### Devshard Details

File: `deploy/join/observability/grafana/dashboards/devshard-details.json`

Purpose: combine devshard inference health, traces, API behavior, approved versions, and log drilldown in one dashboard.

Main sections:

- Inference Health
- Errors & Traces
- API Metrics
- Log Drilldown

The dashboard uses Prometheus for metrics, Jaeger for failed inference traces, and Loki for API/devshard log drilldowns.

## Logs

Grafana log panels query Loki with compose labels. Current dashboard queries focus on:

- API inference request logs from `compose_service="api"`;
- devshard-tagged runtime logs emitted through the API container;
- optional drilldown by `inference_id` and `requester_address` dashboard variables.

## Operational Notes

- Run `jq empty deploy/join/observability/grafana/dashboards/*.json` after editing dashboards.
- Run `go build ./...` in `decentralized-api` and `devshard` after changing instrumentation.
- Run `gofmt` on Go files after observability changes.
- Keep dashboard count small and task-oriented; avoid reintroducing broad overview dashboards unless they answer a concrete operator workflow.
- Keep provider assembly outside collector packages when application dependencies are needed.
