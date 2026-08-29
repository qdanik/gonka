# `scheduler` — which escrow, which host, which nonce

A chat request needs a nonce, and a nonce is bound to a host by `nonce % groupSize`. This package decides which escrow to spend from, and what to do when the nonce it is handed belongs to a host that cannot serve.

## What it owns

- **Escrow choice** (`scheduler.go`) — a load score over the live escrows, scaled by the chain's own host weights.
- **One actor per escrow** (`dispatcher.go`, `dispatcher_queue.go`) — the only goroutine that advances that escrow's nonce stream, so two requests can never take the same nonce.
- **The match decision** (`match.go`) — for the nonce just offered, either serve a waiting request, hold it briefly for a compatible one, burn it, or decline.
- **Burns** (`ghost.go`) — a nonce that will serve nobody, named by the reason it was burned, because an operator reads the reason to decide what to do.
- **Divergence blocks** (`divergence.go`) — a host whose post-state-root disagrees with the escrow gets one catch-up replay; after that it is blocked for that escrow.

## What it does not own

It does not dispatch. It hands out an assignment and the [`engine`](../engine/) sends the request. It does not decide whether a burn is charged to the host — that is [`burns`](../burns/).

## Boundaries worth knowing

- **Five host gates, all of which waiting can clear**: excluded, proof-of-compute-required, throttled, ejected, state-blocked. A capability refusal is counted, never routed on.
- **Predicates are frozen for the whole drain.** Reading them live lets a host look usable to the sweep that kept a waiter and unusable to the binding that would serve it — which burns a nonce every turn, forever.
- **The divergence block and the spent replay outlive the escrow's actor.** Reaping an idle dispatcher is idleness, not resolution.
- **A burn decided before the session could commit has no nonce to name**, and is reported without one.

## Read next

- [`docs/gateway-routing-and-nonces.md`](../docs/gateway-routing-and-nonces.md) — the drain, the gates, the hold, and every burn reason.
