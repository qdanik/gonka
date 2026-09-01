# `registry` — the live escrow set

Which escrows exist right now, what each serves, and who its hosts are.

## What it owns

- **The set itself** (`registry.go`, `escrow.go`) — escrow id to session, model, group, published or draining, behind a copy-on-write snapshot so a reader never blocks a writer.
- **Sessions** (`session.go`) — two kinds. A serving session has host clients and can dispatch; a read-only one rehydrates from local storage alone and can build a settlement but can neither serve nor finalize.
- **Membership and capacity** (`membership.go`, `views.go`) — the participant set each escrow contributes to the capacity model, and the in-flight count routing scores by.
- **Settlement handles** (`settlement.go`) — a retired escrow still resolves, because its committed nonces have no other settlement path.

## Boundaries

- **A retired escrow does not disappear.** Its nonces still owe votes, and the session that can post them outlives its routability.
- **The in-flight hold is taken with the nonce commit and refused once retired**, so an escrow cannot start work it will not be able to settle.

## The published set and its readers

`liveSet` is the published escrow set, replaced rather than mutated, so a reader takes it with one atomic load and never waits on a writer. `newLiveSet` orders each model's candidates by escrow id: the stable order the tie-break assumes.

`escrowEntry.inFlight` is shared by every published set the entry appears in, so a rotation never loses the count of requests already running against it.

`Snapshot` returns every published escrow in id order plus the retired ones still draining, and takes no lock at all — `publishDrainingLocked` republishes the draining view under the registry lock precisely so a scrape cannot wait on a retirement.

`RoutableSession` is the read-only handle the status routes read. It takes no in-flight count and is not the dispatch path: a race resolves its escrow through `Acquire`, which returns the session and its release together, so a handle cannot be held without the hold. `holdFor` is the scheduler's view of the same count, taken in the step that commits a nonce; it is bound to the entry rather than to its id, and re-checks that the published entry is still that entry, so a hold cannot land on a replacement published under the same id.

`SettlementSession` resolves the handle this process still holds, published or draining — asymmetric with routing's published-only lookup. See `rules.md`, "4. Routing and settlement read the escrow set asymmetrically".

## Publishing, retiring and draining

`Add` publishes one escrow for routing, refuses an id an earlier entry is still draining, and releases an unpublished session without flushing.

`openingLock` serializes opens of one escrow. A session is a SQLite file: two concurrent opens fail with `SQLITE_BUSY` before either reaches the already-published check, so the escrow an operator just created can report a failure while another caller serves it. The locks are kept for the life of the process — one small mutex per escrow id ever published — because removing one a waiter still holds would let the next caller take a fresh lock and race it.

`ErrDraining` refuses an escrow id whose earlier entry has not finished: that entry owns the nonces still awaiting votes and holds the storage they settle through.

`unpublish` takes an escrow out of routing and reports whether the caller owns its close. An entry stays in `draining` until the close finishes, whether or not requests were running, so `Add` refuses the id for the whole close rather than opening a second session over storage the first still holds. For the same reason `escrowEntry.close` reports whether the store was released: `Close` runs even when the flush failed, and an entry whose store is gone must stop refusing its own id.

`closeDraining` releases the session with the registry lock free. Closing flushes the snapshot, which takes the session lock, and a dispatch takes the session lock before the registry lock: holding the two in the opposite order here would wedge every later route and settlement behind one retirement. It also keeps the snapshot write and the storage close off the path of every pick.

`lastHoldDropped` reports that this release ended the final in-flight request of a retired escrow, so exactly one caller closes it: the count reaches zero once, and only a retired entry is in `draining`.

`DrainCloseFailures` counts the drained escrows whose flush or close failed. `Retire` and `Close` hand that error to their caller; here the request that kept the escrow open has already been answered and there is nobody left to return it to, so it is counted instead of lost.

`IsBusy` satisfies `escrow.SettlementSource`: a settlement must not claim funds while a request is still spending nonces on the escrow, including one still draining from an earlier session of it.

`Exhausted` reports an escrow routing declined as spent — out of nonces or out of deposit — to the rotation lifecycle, which is what replaces it.

## The two session kinds

`ServingSessions` opens a chain-backed session with host clients (`user.NewHTTPSession`) — the only kind that can dispatch. `ReadOnlySessions` rehydrates from local storage alone (`user.NewLocalSession`): no chain, no host clients, so it can build a settlement but can neither serve nor finalize.

A factory handed an escrow with no record must fail with an error wrapping `escrow.ErrUnknownEscrow`. Callers tell that apart from a load failure to answer 404 rather than 502, and a factory that returns its own error for a missing escrow turns "no such escrow" into "the gateway is broken".

Two `EscrowSession` methods have non-obvious contracts. `SealedInferences` counts what sealing has drained out of `SnapshotState().Inferences`, which holds only the live tail, so a reader without it mistakes that tail for the escrow's whole history. `UserSession` is the concrete handle the dispatch boundary needs, and one rehydrated read-only has no host clients, so sending through it is a bug.

## Nonces and ghost burns

`nonceStream.groupSize` is taken once, at construction: the group is fixed for the escrow's life, and asking the session takes the lock a nonce commit holds. `newNonceStream` is the only way to build one, so the cached size cannot disagree with the session's.

`errNonceDeclined` leaves the bound nonce unconsumed, so the next caller sees the same nonce.

`ghostPrompt` is the synthetic `MsgStart` a burned nonce commits: composed into the diff, never sent to a host. `GhostPrompt` exposes it because a vote raised for that nonce must carry it — a verifier checks the payload against the record's own prompt hash.

A ghost's `StartedAt` is seconds, like every other `StartedAt`: a verifier measures the refusal deadline as now-in-seconds minus this, so a millisecond stamp would keep that difference negative and the timeout on a burned nonce would be rejected every time. See `host/timeout.go`, `VerifyRefusedTimeout`.

## Membership and capacity

`slotCounts` counts the slots each participant occupies in one escrow: a validator holding several slots repeats in the per-slot key list.

`hostShares` is slots(participant, escrow)/totalSlots(participant), the normalised per-host multiplier `limits.Capacity` expects. `pushMembershipLocked` republishes every live escrow's share rather than only the one that changed, because the denominator moves with the set.

The two notification interfaces are both called from paths that must not block. `exhaustion` is called from the request path, once per spent candidate per pick, so an implementation must mark and return rather than do I/O. `publications` is called while the registry holds its lock, so an implementation must return without doing work of its own.

## Settlement and inspection

`Finalize` and `BuildSettlement` both satisfy `escrow.SettlementSource` and both handle a non-resident escrow, but they rehydrate it differently. Finalizing collects host signatures, so it needs a serving session; a settlement payload comes entirely from local storage, so a read-only one is enough — no chain access, no host clients.

`Inspect` resolves a session for reading alone. A live or draining escrow answers from its own session; one already retired is rehydrated from local storage, which is exactly when an operator asks these questions. The returned release closes a rehydrated session and does nothing for a resident one.

Before a payload leaves, `settlementUnverifiable` runs the chain's own check over it — recomputed state root, recovered signers, and weight against 2/3+1 — using the snapshot's group and warm keys, and against the same host stats the transaction will carry rather than the raw map, which may hold nil slots. It **warns and never blocks**: warm keys are populated lazily, so a refusal here can freeze a settlement the chain would have accepted.

## Read next

- [`docs/routing.md`](../docs/routing.md) — the escrow registry, where the nonce, the slot and the hold are taken, membership, and ghost burns.
- [`docs/capacity.md`](../docs/capacity.md) — what in-flight actually counts.
