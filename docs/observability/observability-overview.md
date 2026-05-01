# Join Observability Overview

## What This Document Describes

This document describes the current observability surface for the join deployment.

It is the canonical current-state overview for the repository's join stack.

The current join observability surface covers the request path we already operate today:

- traces for request hand-off across `decentralized-api`, `versiond`, and `devshardd`
- metrics for API and devshard request behavior plus container-level health
- logs in Grafana so request logs and runtime logs can be inspected without jumping between containers
- one operator entrypoint in Grafana, with Jaeger linked from logs through `trace_id`

This is intentionally not a final production observability platform. It is a practical operator surface for the join environment that makes the current system explainable.

## Why The Project Needs It

Without this layer, join failures are mostly debugged from raw Docker logs and ad hoc checks. That is slow for exactly the cases that matter to the project:

- a request enters the public API but disappears somewhere between transfer and executor routing
- a devshard version rollout changes behavior and we need to see which version is actually serving traffic
- the system looks unhealthy but the real issue is in container pressure, request latency, or a single noisy component
- developers need to prove whether a problem is in application code, proxying, routing, or local infrastructure

The goal is not “more charts”. The goal is shorter time to explanation.

## What Is Included In The Join Stack

The current join observability overlay adds these components:

- `Jaeger` for distributed tracing
- `Prometheus` for local metrics scraping and retention
- `Grafana` as the main UI
- `Loki` for container log storage
- `Promtail` for shipping Docker container logs into Loki
- `cAdvisor` for container resource metrics

The current dashboards include:

- `Service Health Overview` — request flow, latencies, error rates, and resource pressure
- `Request Debug & Logs` — request-centric log search and runtime log correlation
- `Infrastructure & Resources` — container resources and observability stack health
- `Inference Drilldown` — log filtering by `inference_id` and `requester_address`
- `System Logs` — generic container log surface

On the application side:

- `decentralized-api` exports request traces and Prometheus metrics
- `devshardd` exports request traces and Prometheus metrics
- `versiond` exports a minimal tracing surface for the HTTP proxy path
- `versiond` also exposes Prometheus service discovery for active devshard versions



## Why This Shape Makes Sense

This design follows the real join topology instead of inventing a larger platform up front.

- `decentralized-api` is where public inference requests enter and are classified
- `versiond` is where version routing decisions happen
- `devshardd` is where request execution and local runtime behavior become visible
- `Prometheus` can already scrape the services we control
- `Loki` can already aggregate the container logs we actually use for debugging

That gives us correlated signals on the request path we care about most, without pretending we already solved long-term retention, multi-tenant security, or production ingestion policy.

## What Operators Get From It

With the join stack enabled, an operator should be able to answer these questions quickly:

- Did the request reach the public API?
- Was it classified as transfer or executor work?
- Which service handled it next?
- Which devshard version served it?
- Did logs and traces for the same request line up through `trace_id`?
- Was the problem application-level or container-level?

That is the immediate value to the project: join becomes inspectable instead of opaque.

## What This Does Not Claim Yet

This join stack does not try to be the final production observability architecture.

It does not yet solve:

- authenticated ingestion
- multi-node or multi-tenant retention strategy
- long-term storage economics
- deep Cosmos SDK and chain-internal observability
- a generalized observability platform for all environments

It also does not yet fully solve:

- bridge-first observability
- validator and consensus health dashboards
- strong end-to-end request correlation across every component
- actor-centric views for executors, validators, ML nodes, and participants
- proactive alerting and SLOs

This document is intentionally narrower. It explains:

- what we are adding right now in the join deployment
- why it is useful immediately
- how the current pieces fit together
- what this work is supposed to de-risk for the next stage