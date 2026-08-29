# `perf` — what each host has been doing

Per-host history, and the one verdict derived from it that routing honours.

## What it owns

- **Samples and windows** (`sample.go`, `host.go`) — decayed success and failure counts, and the first-content quantile the escalation ladder measures a host against.
- **Ejection** (`ejection.go`) — Envoy-style outlier detection: a host far worse than its peers is taken out of the rota, capped so ejection can never remove more than a fraction of the hosts serving a model.
- **In-flight load** (`inflight.go`) — what each host is carrying right now.
- **Capability refusals** (`capability.go`) — counts of what a host's build refused: an unsupported protocol version, a tool call it does not implement, a context length it will not take, plus the smallest context it has admitted to.

## Boundaries worth knowing

- **Capability refusals are counted, never routed on.** They are reported so an operator knows what to fix; nothing here withholds a host from the rota over one. A version refusal in particular would retire a host for good, because a gateway serves one protocol version for its whole life.
- **Ejection is capped by a floor of available hosts**, so it cannot empty a model.
- **Every restart starts clean.** No ejections, no counts, every window at its initial value — a deliberate divergence from the legacy gateway, argued in [`docs/gateway-non-goals.md`](../docs/gateway-non-goals.md).
