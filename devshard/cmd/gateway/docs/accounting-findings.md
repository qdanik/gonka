# Accounting findings

What each finding code means, what it points at, and what to check. The API carries only the code, the severity and the two numbers it was flagged on — `part` and `whole` — so an explanation is written once here instead of crossing the network with every response. `whole` is absent when the finding counts rather than measures a rate.

A finding is never raised below **20** nonces in its denominator: a rate off four attempts describes noise, not a host.

Nonces that never reached the host are excluded from every rate. A burn is this gateway's own decision, and counting it against the host would report our throttling as its failure.

## What the host answers for

| code | rate of | warning | critical |
|---|---|---|---|
| `execution_timeouts` | nonces acknowledged and never finished, over nonces that reached the host | 1% | 5% |
| `refusals` | nonces never acknowledged, over nonces that reached the host | 5% | 20% |
| `answers_unused` | finished answers nobody used, over answers delivered | 20% | — |
| `slow_receipts` | acknowledgements slower than 2.5 s, over answers acknowledged outside PoC | 10% | — |
| `slow_chunks` | answers that stalled mid-stream longer than 5 s, over answers delivered outside PoC | 10% | — |
| `slow_decode` | answers written at more than 40 ms per output token, over answers delivered outside PoC | 10% | — |
| `clock_drift` | receipts stamped more than 5 s from this gateway's clock | 5% | — |

**`execution_timeouts`** — the host answered with a receipt and then delivered nothing. This is the expensive failure: such a nonce is held to the chain's execution deadline, where a plain refusal frees it at the refusal deadline instead. It usually means work was accepted into a queue the host could not drain, so check whether the requested output length fits the hardware's decode rate.

**`refusals`** — the host did not take the work. The cheap failure, since the nonce frees at the refusal deadline, but it still spends the nonce. It points at capacity or reachability rather than at speed.

**`answers_unused`** — the host finished, but another had already answered the client. Losing races consistently points at throughput rather than availability: the work is correct and arrives too late to be worth anything.

**`slow_receipts`** — the receipt is the host's first sign of life, before any token is generated, so a slow one points at admission rather than at generation: the request waited to be picked up. The threshold sits just past the observed p99, so this stays silent unless a host is genuinely out of family.

**`slow_chunks`** — the host began answering and then went quiet between chunks. A client reads this as a hang rather than as slowness, and a long enough gap ends the attempt outright.

**`slow_decode`** — the host answered, and answered correctly, but wrote at a fraction of its peers' rate. Measured from the first content chunk, so the prompt it had to read is not charged to how fast it writes. Hosts cluster well under the threshold or well over it, so one flagged here is an outlier rather than a large model — though the threshold is absolute, and a model heavier than any yet served would flag every host running it.

**`clock_drift`** — the chain measures the execution deadline from the timestamp the host signs into its own receipt, so a drifted clock moves that deadline. Running ahead makes the network wait past a deadline that has already passed; running behind gets a nonce voted timed out while the answer is still being written. Check NTP on the host.

The offset is measured against the midpoint of the send-to-receipt round trip, not against dispatch, so the host is not charged for the outbound leg. Half a second is added back because the executor stamps whole seconds downward.

## What this gateway did

| code | rate of | warning | critical |
|---|---|---|---|
| `throttled_by_gateway` | assigned nonces burned without being sent | 10% | — |
| `failure_terminals` | nonces that reached the host and produced no usable answer | any | — |

**`throttled_by_gateway`** — this gateway stopped sending, so these nonces are its decision and not the host's failure. Its per-host window narrows after repeated failures and widens again as they stop, which makes this a consequence of the other findings rather than a fault of its own.

**`failure_terminals`** — how many failures reached the host at all. Which failure each was is in the `counters` array beside the finding: read `terminal` there, and `phase` of `poc` marks one that is expected, because the host was proving computation and could not serve.

## What needs reporting

| code | flagged on | warning | critical |
|---|---|---|---|
| `ledger_disagrees_with_chain` | nonces accounted beyond what the chain assigned | any | — |

**`ledger_disagrees_with_chain`** — this gateway accounted for more nonces than the chain says the slot was given, so one of the two is wrong. Worth reporting rather than acting on: no host behaviour produces this.

## Phase

Every rate above that names a host fault counts only what happened outside PoC, on both sides of the ratio. A host proving computation cannot serve, and charging it for that would make every participant look broken once an epoch.
