# `nonces` — what feeds the ledger

[`accounting`](../accounting/) is a book of facts. This package is the only thing that writes to it.

## What it owns

- **`Recorder`** — opened by `Open` when nonce accounting is enabled, `nil` when it is not, so a disabled ledger costs nothing and every call site tolerates the nil.
- **Three live sources.** The race, through `RecordRace`; the scheduler's burns, through `RecordGhost`; the timeout votes, through `RecordTimeout`.
- **Two chain sources.** A per-escrow diff watcher that turns applied timeouts and validation verdicts into ledger entries, and a periodic sweep that reconciles finished nonces, host stats and open challenges with what the chain actually holds.
- **The HTTP listener and the Prometheus collectors** for the book it owns.

## What it does not own

It does not classify. Which counter a nonce lands in is `accounting`'s decision; this package only reports what happened, in the vocabulary the ledger admits.

## Boundaries worth knowing

- **A disabled ledger is a nil `*Recorder`, not a no-op object.** Every method tolerates a nil receiver, which is what keeps the enabled and disabled paths from diverging.
- **The sweep is the safety net, not the primary path.** Live events are recorded as they happen; the sweep exists because the gateway can miss one — a restart, a dropped diff — and the chain is the authority.

## Read next

- [`accounting/README.md`](../accounting/README.md) — the book this fills.
