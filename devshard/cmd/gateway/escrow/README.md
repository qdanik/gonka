# `escrow` — the escrow's life

An escrow is funds on chain plus a group of hosts. This package creates one, keeps it honest across restarts and epoch boundaries, and closes it.

## What it owns

| File | What it holds |
| --- | --- |
| `manager.go` | the lifecycle: create, activate, drain, retire |
| `rotation.go`, `commitments.go` | replacing an escrow across the proof-of-compute boundary, and the intent recorded before the transaction so a crash cannot lose it |
| `settlement.go` | closing an escrow and paying what its hosts earned |
| `depletion.go` | noticing the funds will not cover the work in flight |
| `checker.go`, `dedup.go` | crash-recovery reconciliation: what the chain holds versus what this process recorded |
| `breaker.go` | refusing to keep creating escrows when creation keeps failing |

## Boundaries worth knowing

- **Intent is written before the transaction, not after.** An interrupted rotation is recoverable only if the record of what was attempted survives the interruption.
- **An escrow parks before it is checked for busy, not after.** Otherwise a nonce can be committed between the two.
- **Settlement waits for the votes its nonces owe.** Concluding while one is in flight pays for work the chain has not yet judged.

## Read next

- [`docs/gateway-escrow-lifecycle.md`](../docs/gateway-escrow-lifecycle.md) — the states, the transitions, and what each one costs.
