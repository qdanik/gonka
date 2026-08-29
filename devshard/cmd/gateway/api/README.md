# `api` — the HTTP boundary

The only package that speaks HTTP to a client, and the one that decides what a failure looks like from outside.

## What it owns

| File | What it holds |
| --- | --- |
| `routes.go`, `server.go` | the route table and the server; every route names its own label and whether the operator kill switch exempts it |
| `middleware.go` | authentication, body limits, request ids |
| `admission.go` | the limiter slot and the token budget a request takes before it is raced |
| `stream.go` | `clientStream`: the writer the race streams through, and the fold for a non-streaming client |
| `buffer_budget.go` | the ceiling on the bytes every non-streaming reply holds at once |
| `cache.go` | the response cache, keyed on everything that varies the answer |
| `errors.go` | which error becomes which status |
| `admin*.go` | the operator surface: escrows, overrides, quarantine, accounting resets |
| `capture.go`, `inspect.go` | sampled request capture for diagnosis |
| `dispatch.go`, `timeouts.go` | the adapters that join a registry session to the race and to the vote path |

## Boundaries worth knowing

- **Our own limiter is 429; the shard being full is 503.** A quota the caller exceeded is theirs to slow down; no room on the shard is not.
- **A reply is folded as it arrives**, so a non-streaming request holds the answer being assembled rather than the stream it came from. Past the process-wide ceiling it is refused with 503 rather than held until the kernel intervenes.
- **The status is chosen from the assembled body, not before it.** A body the assembler could not fold is answered as a failure, not as a 200 carrying an error object.
- **`clientStream` is written by attempt goroutines after the handler has returned**, so every path through it takes the lock and stops once closed.
- **The admin surface is disabled entirely when no admin key is set** — including escrow creation.

## Read next

- [`docs/gateway-request-lifecycle.md`](../docs/gateway-request-lifecycle.md) — one request from the socket to the settled nonce.
- [`docs/gateway-operations.md`](../docs/gateway-operations.md) — every route and every knob.
