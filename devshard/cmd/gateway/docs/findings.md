# Proposals outside the gateway

Three defects the gateway runs into and cannot fix inside `cmd/gateway`. Each is stated from the code that produces it, with the change that would resolve it and the reason that change is sound.

---

## 1. Sweep the execution timeouts nobody retries

| | |
|---|---|
| **Status** | Proposed. Not implemented. |
| **Scope** | `devshard/state`, `devshard/user`, one caller in `cmd/gateway/escrow` |

### Problem

A nonce a host receipted and never finished owes an execution timeout. `SettleTimeouts` is called once, from the end of the race that owned the nonce ([`engine/engine.go`](../engine/engine.go)). If the vote does not reach the threshold at that moment — verifiers unreachable, the group briefly short — nothing attempts it again, because the race that owned the nonce has ended and no other component knows the nonce exists.

The cost is not observability, it is money. In [`settleLiveRecordLocked`](../../../state/machine.go) a `StatusStarted` record settles as `ActualCost = ReservedCost`, credited to the executor slot: the escrow pays in full for an answer it never received. A timeout that does apply instead refunds `ReservedCost` to the balance and increments that executor's `Missed`, so the unposted vote also spares the host a stat it earned.

### Proposal

Add an enumeration of live `StatusStarted` ids to `state.StateMachine`; add a bounded sweep to `user.Session` that calls the existing `HandleTimeout` for each such nonce already past its execution deadline; drive it from the gateway's existing 15-second escrow tick ([`escrow/manager.go`](../escrow/manager.go)), beside `settlePending` and `checkMissing`.

### Why it works

`applyTimeout` ([`state/machine.go`](../../../state/machine.go)) has no wall-clock bound: it requires only that the record is still live and that its status matches the reason. `sealEligibleStatus` ([`state/seal.go`](../../../state/seal.go)) admits only `Finished`, `Validated`, `Invalidated` and `TimedOut`, so a `StatusStarted` record never auto-seals — neither the nonce gate nor the clock gate can reach it. It therefore stays settleable until the escrow itself settles, which leaves a window measured in hours, not minutes, for a retry to find the verifiers reachable.

The retry needs nothing carried over from the original request: `VerifyExecutionTimeout` ([`host/timeout.go`](../../../host/timeout.go)) decides from the verifier's own state and the executor, and takes no payload. The live `StatusStarted` records are the whole input.

A sweep cannot collide with a live race. The execution deadline is `ConfirmedAt + ExecutionTimeout` (32 minutes) and no attempt outlives `streamingHardTimeout` (20 minutes), so a nonce past its deadline is owned by nobody.

### Scope note

`StatusPending` is deliberately excluded. Its reserve is refunded at settlement either way, and `settleLiveRecordLocked` declines to increment `Missed` there on purpose, because state cannot distinguish user censorship from host absence. Sweeping it would assign blame the protocol chose not to assign.

---

## 2. Read the confirm stamp from the record, not from the cache

| | |
|---|---|
| **Status** | Proposed. Not implemented. |
| **Scope** | `devshard/user` |

### Problem

`Session.TimeoutDeadline` reads `confirmedAt` only from the in-memory `nonceStates` map. That map is written when a nonce is committed and when a response arrives, and it is empty after a restart. The committed record carries `ConfirmedAt` and survives.

With an empty map, a nonce a host already receipted yields reason `refused`. `applyTimeout` rejects a refused timeout against such a record — `reason=refused requires pending`. The vote is therefore not merely missed across a restart: it cannot be posted at all, and the nonce is guaranteed to settle at full reserve.

### Proposal

Prefer the committed record and fall back to the map, rather than the reverse. The record is the authority; the map is a cache of it.

### Why it works

`ConfirmedAt` is written in the same transition that sets `StatusStarted` ([`state/machine.go`](../../../state/machine.go)), from the executor's signed receipt, so any record the sweep in proposal 1 would enumerate carries it. A record without the stamp still reads as `refused` and is simply left alone, which is the safe direction: the chain would decline it.

---

## 3. Name the unapplied timeout at its source

| | |
|---|---|
| **Status** | Half implemented — the gateway side is done. |
| **Scope** | `devshard/user` |

### Problem

`HandleTimeout` has a path where the votes sufficed and the diff was sent, but the transaction did not land. `result.Applied` is false while the returned error is unwrapped — the same shape a settled vote returns. A caller separating the two by error shape reads the unsettled nonce as settled and records a vote that never reached the chain.

### Proposal

Wrap that return in `ErrTimeoutNotApplied`, the sentinel the insufficient-votes path already uses.

### Why it works

The gateway no longer depends on the error shape: `SettleTimeout` reads `result.Applied`, which is the fact itself, so the miscount is already gone. The wrap is what remains for the reason label, which is how an operator tells "the diff carried no timeout" from a failure to collect votes at all.

### Verification note

This path is not reachable cheaply in a test: it requires `sendPendingDiff` to succeed while returning a diff that carries no timeout transaction for that nonce. The wrap is correct by reading, and its consumer is covered, but the source-to-sentinel link itself is not pinned by a test.
