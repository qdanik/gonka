# `chain` — all blockchain input and output

Every read from the network and every transaction the gateway signs passes through here. No other package talks to the chain.

## What it owns

- **The transaction client** (`txclient.go`, `tx_build.go`, `protoencode.go`, `grpc.go`) — build, sign, broadcast, confirm. A transaction is not assumed to have landed because it was accepted.
- **The phase observer** (`observer.go`, `observer_fetch.go`, `snapshot.go`) — polls the network's public API and publishes an immutable `PhaseSnapshot`: the epoch, its phase, which participants are preserved through proof-of-compute, and the host weights the capacity model scales by.
- **Escrow queries and settlement encoding** (`escrow_query.go`, `settlement.go`).
- **Protocol versions** (`versions.go`) — the gateway serves exactly one, fixed at build time.

## Boundaries worth knowing

- **The snapshot is immutable and published whole.** A reader never sees half an update, and never has to lock.
- **A wrong chain id invalidates every signature**, so it is validated at startup rather than discovered on the first broadcast.
- **Weights can be absent.** A model whose weights the chain has not reported falls back to membership share, and says so, rather than scoring as zero.

## What the chain observer provides

`observer.go` polls the network's public API on a fixed interval, folds the raw inputs into a `PhaseSnapshot`, and publishes it whole. It derives nothing beyond that fold — no scoring, no policy — so every consumer reads the same numbers.

A failed poll **republishes the previous snapshot with `LastError` set** rather than an empty one: a network hiccup must not read as "the epoch has no participants". `LastUpdatedAt` is how a reader tells fresh data from a held-over view.

Three fields have absent values that are load-bearing, and each chooses its direction deliberately:

| Field | Absent means | Read as |
| --- | --- | --- |
| `RequestsBlocked` | derived, never absent — `false` outside every PoC phase (`rawPoCBlockingState`) | requests admitted |
| `Preserved` / `PreservedByModel` | the chain holds no snapshot for this episode | **everyone** is preserved (`scheduler`, `pocPreserved`) — fail open, so a missing snapshot never empties routing |
| `MaxNonce` | the chain has not enabled devshard escrow params | the nonce gate falls back to `fallbackNonceCeiling` rather than to "no ceiling" |

Weights follow the same rule: `CurrentWeightsByModel` is preferred, `CurrentWeights` is the fallback, and a model with neither scores by membership share rather than as zero.

`versions.go` fetches per-node protocol versions with `versionsPollConcurrency` = 16 workers and a `versionsFetchTimeout` of 2 s each, which keeps one full pass inside the poll's freshness window even on a large network.

## Transaction encoding

Transactions are **unordered**: instead of a sequence number they carry a `timeout_timestamp` set `UnorderedTxTTL` (9 min) ahead of now. Inside that window a broadcast transaction can still land, so "not found" is never proof it failed — every reconciliation in [`escrow/`](../escrow/) waits out the window before concluding anything.

The three lookups mean three different things, and callers depend on the distinction:

| Call | `found`/`succeeded` false | `ErrTxNotFound` | error |
| --- | --- | --- | --- |
| `GetTxEscrowID` | committed but failed: no escrow will ever exist | the chain does not have it | transport failure — conclude nothing |
| `TxCommitted` | committed and rejected | the chain does not have it | transport failure — conclude nothing |
| `awaitTx` | — | a 404 here is "not indexed yet", not terminal | — |

Reading a transport failure as absence would rebuild and rebroadcast a settlement that already moved the money, so it is always returned as an error.

Both money-moving transactions take an `onPrepared` hook that records the precomputed hash **before** the irreversible broadcast; an error from it aborts the broadcast. They differ afterwards: `CreateEscrow` returns once the transaction is accepted, because its commitment row is reconciled later, while `SettleEscrow` waits for the commit — the caller destroys the means to retry, so the result has to mean "settled" and not "sent". A broadcast that is not confirmed in time returns `errUnconfirmed`, whose message says what the caller must not do: create another.
