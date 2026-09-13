# Defects outside the gateway

Four defects the gateway runs into and cannot fix inside `cmd/gateway`. All four are now closed; each entry states the rule that replaced it and where that rule lives.

---

## 1. The execution timeouts nobody retried — closed

**What it was.** A nonce a host receipted and never finished owes an execution timeout, and `SettleTimeouts` was called once, from the end of the race that owned it ([`engine/engine.go`](../engine/engine.go)). A round that found no verifiers left the nonce owned by nobody, because no other component knew it existed.

The cost was money, not observability. In `settleLiveRecordLocked` ([`state/machine.go`](../../../state/machine.go)) a `StatusStarted` record settles as `ActualCost = ReservedCost`, credited to the executor slot: the escrow pays in full for an answer it never received, and the unposted vote also spares that host the `Missed` it earned.

**The rule now.** Every escrow tick scans its own live records for ones started, stamped and past their execution deadline by a grace, and re-votes them through the same `HandleTimeout`: [`state/started_deadline.go`](../../../state/started_deadline.go) scans, [`user/timeout_sweep.go`](../../../user/timeout_sweep.go) votes, [`registry/timeout_sweep.go`](../registry/timeout_sweep.go) walks the published escrows, and [`escrow/manager.go`](../escrow/manager.go) drives it off the tick. See [`race.md`](./race.md), "The vote nobody retried", for the three properties that keep it off the hot path.

**Why it is sound.** `applyTimeout` ([`state/machine.go`](../../../state/machine.go)) has no wall-clock bound: it requires only that the record is still live and that its status matches the reason. `sealEligibleStatus` ([`state/seal.go`](../../../state/seal.go)) admits only `Finished`, `Validated`, `Invalidated` and `TimedOut`, so a `StatusStarted` record never auto-seals and stays settleable until the escrow itself settles. The retry needs nothing carried over from the original request: `VerifyExecutionTimeout` ([`host/timeout.go`](../../../host/timeout.go)) decides from the verifier's own state and the executor, and takes no payload. A sweep cannot collide with a live race either: the execution deadline is `ConfirmedAt + ExecutionTimeout` (32 minutes) and no attempt outlives `streamingHardTimeout` (20 minutes), and the configured grace is added on top of that.

**Scope note.** `StatusPending` is excluded. Its reserve is refunded at settlement either way, and `settleLiveRecordLocked` declines to increment `Missed` there, because state cannot distinguish user censorship from host absence. Sweeping it would assign blame the protocol chose not to assign.

---

## 2. The confirm stamp read from the cache — closed

**What it was.** `Session.TimeoutDeadline` read `confirmedAt` only from the in-memory `nonceStates` map. That map is written when a nonce is committed and when a response arrives, and it is empty after a restart, while the committed record carries `ConfirmedAt` and survives.

With an empty map a nonce a host already receipted yielded reason `refused`, and `applyTimeout` rejects a refused timeout against such a record — `reason=refused requires pending`. The vote was not merely missed across a restart: it could not be posted at all, and the nonce was guaranteed to settle at full reserve.

**The rule now.** The committed record is the authority for the stamp and the map is a cache of it ([`user/session.go`](../../../user/session.go), `TimeoutDeadline`). A record without the stamp still reads as `refused`, which is the safe direction: the chain would decline anything else.

**Why it is sound.** `ConfirmedAt` is written in the same transition that sets `StatusStarted` ([`state/machine.go`](../../../state/machine.go)), from the executor's signed receipt, so every record the sweep enumerates carries it.

---

## 3. The unapplied timeout unnamed at its source — closed

**What it was.** `HandleTimeout` had a path where the votes sufficed and the diff was sent, but the transaction did not land. `result.Applied` was false while the returned error was unwrapped — the same shape a settled vote returns — so a caller separating the two by error shape read the unsettled nonce as settled.

**The rule now.** That return is wrapped in `ErrTimeoutNotApplied`, the sentinel the insufficient-votes path already uses ([`user/timeout_effect.go`](../../../user/timeout_effect.go), `timeoutSettledError`). The gateway itself never depended on the error shape — `SettleTimeout` reads `result.Applied`, which is the fact — so this closes the reason label rather than a miscount.

**Verification note.** The path is not reachable cheaply in a test: it requires `sendPendingDiff` to succeed while returning a diff that carries no timeout transaction for that nonce. The decision it feeds is pinned directly instead ([`user/timeout_effect_test.go`](../../../user/timeout_effect_test.go)).

---

## 4. The SSE event cap below the body a host may send — closed

**What it was.** The transport capped one SSE event at 1 MiB ([`transport/client.go`](../../../transport/client.go), `DefaultMaxSSEEventBytes`). The gateway always asks for logprobs, `top_logprobs` and token ids, and a host that does not stream writes its whole answer as one event, so a few thousand tokens passed the cap. The attempt was aborted as `response_too_large` after its nonce was committed, the honest host took the perf sample, and the attempt's answer was lost; the legacy fold kept its own copy of the same 1 MiB cap.

**The rule now.** The cap is `MaxJSONResponseBytes` (16 MiB), the largest body the JSON path already reads, and the legacy fold reads its scanner cap from the transport constant ([`cmd/devshardctl/stream_aggregate.go`](../../devshardctl/stream_aggregate.go)). See [`race.md`](./race.md), "Classification and reassembly", for why a complete event never reaches the attempt budget.

**Why it is sound.** No bound grew: the JSON path already buffers a body of that size ([`transport/client.go`](../../../transport/client.go), `readBoundedResponseBody`), and the carry budget charges only an unterminated tail, which the transport never hands over, because `writeSSELine` writes an event and its terminator in one write.
