# Proposal: replace the devshard gateway

| | |
|---|---|
| **Status** | Accepted and implemented. Recorded after the fact; outcomes are measured, not projected. |
| **Scope** | `devshard/cmd/devshardctl` → `devshard/cmd/gateway` |
| **Decision** | Full rewrite of internals behind an unchanged external HTTP API |
| **Supersedes** | Incremental refactoring of `devshardctl` |

## 1. Summary

The gateway that brokers inference between clients and race participants had accumulated a structure that blocked the roadmap and hid a repeating class of money defect. This proposal replaces its internals with a package-per-responsibility design, preserving the external HTTP contract so clients and participants are unaffected, and redesigning configuration, storage layout and the limiter model.

The rewrite is complete and serving mainnet traffic. Section 8 reports what it cost and what it produced against the criteria in section 7.

## 2. Problem

`devshardctl` grew through a chain of fixes (#1284 → #1289 → #1348 → #1427 → #1434 → #1435 → #1454/#1456) into a single `package main` of 111 files and ~41 000 lines.

Structural evidence:

| symptom | measurement |
|---|---|
| god-structs | `Gateway`: 24 fields, 5 mutexes; `inflight`: ~90 fields |
| function size | routinely 250–330 lines |
| layer violation | `Gateway.attach*` writes into `rt.proxy.redundancy.*` |
| global mutable state | including a `capacity_aware_limits` toggle read and never used |
| configuration | spread across three planes |
| hot path | two-mux internal re-dispatch per request |

**Business impact.** Host selection was implemented in three uncoordinated places — `reserveRuntimeForModel`, `sessionPicker.run`, `Redundancy.Decide`. Any routing feature, KV-cache affinity being the near-term one, required coordinated edits to three files that did not agree on what a host was. The roadmap item was therefore not schedulable.

**Correctness impact.** The same defect recurred eight times: a nonce is committed to the chain and now costs money; between the commit and the attempt to resolve it, either the state moves or a predicate lies; the nonce ends committed, unsettled, and owned by nobody. Eight repetitions of one shape indicates a structural cause, not eight independent mistakes.

## 3. Goals

1. Ownership boundaries the compiler enforces, so a routing change is local.
2. Behaviour preserved as explicit contracts and tests; implementation not preserved.
3. Bounded load on participants, the chain and the public API under failure.

## 4. Non-goals

- Changing the external HTTP API. Broker- and participant-facing routes and schemas stay.
- Building KV-cache affinity. An extension point only; the feature is a separate proposal.
- Preserving single-escrow mode, its top-level route aliases, or the legacy state layout.
- A latency or throughput target. Both are set by hosts and the chain, not by this code.

## 5. Options considered

**A. Incremental refactoring in place.** Extract packages from `devshardctl` while it keeps serving.
*Rejected.* The god-structs are the coupling: `inflight`'s ninety fields are read across every layer, so extraction cannot proceed without changing every caller anyway. The eight-times defect lives in that coupling and would survive a move.

**B. Port with cleanup — same design, better files.** Move the code into packages, keep the algorithms.
*Rejected.* It carries the predicates forward unexamined, and the predicates are what lied. It also carries three uncoordinated selection paths into three packages, leaving the roadmap blocked.

**C. Full rewrite of internals behind the existing API.** ← **selected**
Old tests and fixtures become behavioural contracts; a golden-parity harness covers request filtering. Cost is a second implementation to validate; benefit is that every predicate must be restated to be written, which is the mechanism that surfaces the defect class.

**D. Do nothing.** Live with the structure, fix defects as they appear.
*Rejected.* Eight repetitions with money at stake, and a blocked roadmap item.

## 6. Design decisions

| # | question | decision |
|---|---|---|
| 1 | compatibility | external HTTP API preserved; environment, configuration and state layout redesigned |
| 2 | KV-cache affinity | extension point (`AffinityHint`) only |
| 3 | single-escrow mode | dropped; one escrow is a pool of one, reachable at `/devshard/{id}/…` |
| 4 | configuration | immutable snapshot from `defaults ← environment ← admin overrides`, swapped atomically; no mutable globals |
| 5 | approach | full rewrite including the race engine |
| 6 | limiters | new designs: AIMD per-host window plus a soft circuit breaker, replacing fixed 30–60 minute quarantines |

Structure: thirteen packages with consumer-side interfaces, so a dependency that should not exist does not compile. The engine races attempts; the scheduler owns nonce-to-host assignment; limits owns admission; chain, escrow, store and filters own the rest.

## 7. Success criteria

1. External API unchanged — existing clients and participants require no change.
2. The eight-instance defect class cannot recur in the same form.
3. Routing changes become local to one package.
4. No regression in refusal behaviour under load.

## 8. Outcome

**Criteria 1–3 met.** The API is unchanged; the money-defect class is gated by types and tests, and one attempt to reproduce a member of it at the API layer failed to compile; host selection lives in `scheduler` alone.

**Criterion 4 exceeded, after production contradicted the projection.** Live traffic showed the admission window opening at four requests per participant, and three distinct participants standing behind sixteen escrow slots — the fleet was capped at twelve concurrent requests by the gateway's own defaults, not by the hosts.

| burst of 100 concurrent | before | after |
|---|---|---|
| served | 14 | **100** |
| rate-limit refusals | 51 | **0** |
| bad-gateway answers | 35 | **0** |

Five consecutive runs, non-overlapping ranges.

**Further defects found only in production**, each a case of one component knowing something another did not read:

- an escalation constant assumed 50 output tokens/second; measurement showed 25.7, so the model never governed and a floor decided instead;
- the quiet-host retry halved the client's token budget on the theory that half the tokens take half the time — the measured gap between a stuck host and a fresh one is thirteenfold, where halving buys two; it had cost a quarter of served requests a silently truncated answer;
- eight of 255 committed nonces produced no log line, no ledger row and no timeout vote, because an attempt that never reported was dropped from the race outcome that the vote plan reads.

**Cost.** The projected reduction in code slipped six times as defects were fixed: 30.4% → 28.4% → 26.9% → 25.1% → 23.7% → 22.5%. Final: ~20 600 production lines against 26 574, with 1.73 lines of test per line of code against 0.82.

**Regressions accepted.** Field-stripping of large responses allocates ~68 KB more per response: the path moved from a byte scan to a full parse, and that scan was the hole an escaped field name walked through. Latency percentiles rose once the halving was removed, because requests formerly refused instantly or answered at half length now complete in full.

## 9. Risks

| risk | mitigation |
|---|---|
| a second implementation diverges from observed behaviour | old tests kept as contracts; golden-parity harness for filters |
| cutover loses escrow state | state rebuilt from `GATEWAY_ESCROWS_JSON` and the admin import endpoint; the legacy database is not migrated |
| operators carry stale configuration | renamed variables fall back to their former names and log which one was read |
| defects reachable only under real load | staged rollout on one node with log review after each burst — the source of section 8's measurements |

## 10. Open items

- **KV-cache affinity.** Still an extension point. What can be promised is affinity to a *participant*; whether that reaches the same node is decided by that participant's own router.
- **Cross-shard coordination of the network concurrency share.** Unbuilt. Measurement shows it is not the binding constraint at current load.
