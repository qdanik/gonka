# `warmup` — teaching a new escrow to its own group

An escrow is published to the chain before its hosts know it exists. The first real request would discover that the hard way, one host at a time.

## What it owns

- **`Prober`** — on publication, spends one nonce on a probe so every host in the group learns the escrow and catches up on its diff chain. Built only when `warm_new_escrows` is on, and `nil` otherwise.
- **The settlement of that nonce** (`settle.go`). The probe's nonce is committed outside the scheduler, so nothing else would ever settle it: the escrow would pay the reserve and the host would escape the miss. A refused probe votes its own timeout.

## What it does not own

It is not a health check and not a scheduler. It runs once per escrow, on publication.

## Boundaries worth knowing

- **Late binding is deliberate.** The registry exists only after the warmup it publishes to, and the vote path only after the sessions the race shares with it — hence `Serve` and `Settle` rather than constructor arguments.
- **The probe's nonce is the gateway's own work, not a user's.** The ledger records it under its own terminal so it lands on neither side of a serving ratio.

## Read next

- [`docs/gateway-escrow-lifecycle.md`](../docs/gateway-escrow-lifecycle.md) — where publication sits in the escrow's life.
