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

## The judgements it does make

Not classification — that is `accounting`'s — but the four readings of a raw fact that have to happen before the fact can be reported at all:

- **The epoch a swept escrow is stamped with** is the one the escrow was *first seen in*, not the one it was created in, which the gateway never reads. With counters that start empty on every boot the two say the same thing: the epoch this ledger's numbers cover.
- **A slow receipt is measured from the dispatch, and only where both stamps exist.** An attempt that never got a receipt is a refusal, which the ledger already counts; calling it slow as well would report one failure twice.
- **Clock drift counts in either direction.** A host running ahead makes the gateway wait past a deadline that already passed; one running behind makes it vote on a nonce still being served.
- **A winner crowned after its client left gets its own terminal.** It still counts as work the host delivered, but it is not an answer anybody read — and without a name of its own, the population where the race outlived its client cannot be found in the ledger at all.

## Read next

- [`accounting/README.md`](../accounting/README.md) — the book this fills.
