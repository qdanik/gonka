# `warmup` — teaching a new escrow to its own group

An escrow is published to the chain before its hosts know it exists. Without a probe, the first real request pays that discovery one host at a time.

## What it owns

- **`Prober`** — on publication, spends one nonce on a probe so every host in the group learns the escrow and catches up on its diff chain. Built only when `warm_new_escrows` is on, and `nil` otherwise.
- **The settlement of that nonce** (`settle.go`). The probe's nonce is committed outside the scheduler, so nothing else would ever settle it: the escrow would pay the reserve and the host would escape the miss. A refused probe votes its own timeout.

## What it does not own

It is not a health check and not a scheduler. It runs once per escrow, on publication.

## Boundaries

- **Both dependencies bind after construction.** The registry exists only after the warmup it publishes to, and the vote path only after the sessions the race shares with it — hence `Serve` and `Settle` rather than constructor arguments.
- **The probe's nonce is the gateway's own work, not a user's.** The ledger records it under its own terminal so it lands on neither side of a serving ratio.
- **The probe's timeout kind is read from the receipt, the way the engine reads it.** A host that receipts and then hangs outlives `probeTimeout`, so the send errors and its reply never arrives; the receipt is therefore taken as it arrives. A kind fixed at `refused` files the expensive failure as the cheap one.
- **The escrow is opened in the ledger here, not left to the sweep.** The sweep opens escrows on its own schedule, which has not necessarily run yet — and this nonce is spent on a just-published escrow. An attempt the ledger refuses would lose the terminal that keeps the gateway's own nonce out of the host's record.
- **`EscrowPublished` is announced under the registry's lock**, so it must return without doing work.

## Read next

- [`docs/escrows.md`](../docs/escrows.md) — where publication sits in the escrow's life.
