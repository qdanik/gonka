# Accounting findings

What each finding code means, what it points at, and what to check. The API carries only the code, the severity and the two numbers it was flagged on — `part` and `whole` — so an explanation is written once here instead of crossing the network with every response. `whole` is absent when the finding counts rather than measures a rate.

Findings also leave as `devshard_gateway_nonce_finding{code,severity,epoch,participant,model}`, valued at the rate that raised them. That series is how an alert reaches them: the JSON API is served only when `GATEWAY_NONCE_ACCOUNTING_LISTEN_ADDR` is set, and it is empty by default.

A finding is never raised below **20** nonces in its denominator: a rate off four attempts describes noise, not a host.

A failure this gateway caused is excluded from the host's rates as well. Its own phase transition ending an attempt, a vote round that reached no verdict, a missing poster and a long response that had already produced content are all ours, not the host's. A failure whose cause the ledger could not name still counts against the host: excusing the unclassified would empty the rates.

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
| `logprobs_not_token_ids` | answers whose logprob tokens name decoded text, over answers delivered | 0.1% | 1% |
| `chain_recorded_misses` | nonces the chain itself recorded as missed, over assigned | 1% | 5% |
| `chain_recorded_invalid` | nonces the chain itself recorded as invalid, over assigned | 1% | 5% |
| `challenges_unresolved` | validation challenges still without a verdict, over assigned | 1% | 5% |
| `timeouts_undecided` | timeout rounds that reached no verdict, over rounds that voted | 10% | 50% |

**`execution_timeouts`** — the host answered with a receipt and then delivered nothing. This is the expensive failure: such a nonce is held to the chain's execution deadline, where a plain refusal frees it at the refusal deadline instead. It usually means work was accepted into a queue the host could not drain, so check whether the requested output length fits the hardware's decode rate.

**`refusals`** — the host did not take the work. The cheap failure, since the nonce frees at the refusal deadline, but it still spends the nonce. It points at capacity or reachability rather than at speed.

**`answers_unused`** — the host finished, but another had already answered the client. Losing races consistently points at throughput rather than availability: the work is correct and arrives too late to be worth anything.

**`slow_receipts`** — the receipt is the host's first sign of life, before any token is generated, so a slow one points at admission rather than at generation: the request waited to be picked up. The threshold sits just past the observed p99, so this stays silent unless a host is genuinely out of family.

**`slow_chunks`** — the host began answering and then went quiet between chunks. A client reads this as a hang rather than as slowness, and a long enough gap ends the attempt outright.

**`slow_decode`** — the host answered, and answered correctly, but wrote at a fraction of its peers' rate. Measured from the first content chunk, so the prompt it had to read is not charged to how fast it writes. Hosts cluster well under the threshold or well over it, so one flagged here is an outlier rather than a large model — though the threshold is absolute, and a model heavier than any yet served would flag every host running it.

**`clock_drift`** — the chain measures the execution deadline from the timestamp the host signs into its own receipt, so a drifted clock moves that deadline. Running ahead makes the network wait past a deadline that has already passed; running behind gets a nonce voted timed out while the answer is still being written. Check NTP on the host.

The offset is measured against the midpoint of the send-to-receipt round trip, not against dispatch, so the host is not charged for the outbound leg. Half a second is added back because the executor stamps whole seconds downward.

**`logprobs_not_token_ids`** — a validator replays the answer from its logprob token ids, and decoded text cannot be replayed, so it votes the answer invalid and the host loses the reward. A host does this for every answer carrying logprobs or for none, which is why one flagged answer is already enough to report. Check the inference server's logprobs configuration.

**`chain_recorded_misses`, `chain_recorded_invalid`** — not this gateway's reading but the chain's own, carried back from settlement. These are the numbers that cost the host its reward, so they outrank anything measured here.

**`challenges_unresolved`** — a validation was challenged and never settled either way. The nonce stays disputed, and the rate matters more than the count: two disputes in thirty thousand nonces is noise, four hundred in nineteen thousand is a pattern.

**`timeouts_undecided`** — the round was raised and never reached a verdict, because votes could not be collected or too few arrived. A nonce that reaches no verdict cannot be raised again, so the host is never answerable for it — the counters show this even when nothing else does. Rounds this gateway skipped are excluded: they asked nobody.

## What this gateway did

| code | rate of | warning | critical |
|---|---|---|---|
| `throttled_by_gateway` | assigned nonces burned without being sent | 10% | — |
| `blocked_by_capability` | assigned nonces burned because the host refuses this kind of work | 1% | — |
| `blocked_by_state_divergence` | assigned nonces burned because the host's escrow state no longer matches the group's | 1% | — |
| `failure_terminals` | nonces that reached the host and produced no usable answer | any | — |

**`throttled_by_gateway`** — this gateway stopped sending, so these nonces are its decision and not the host's failure. Its per-host window narrows after repeated failures and widens again as they stop, which makes this a consequence of the other findings rather than a fault of its own.

**`blocked_by_capability`** — the host answers that it cannot serve this request at all: an unsupported protocol version, a tool call it does not implement, a context length it will not take. Every nonce assigned to it burns, so the rate climbs fast, and unlike throttling it is fixed on the host's side rather than by waiting.

**`blocked_by_state_divergence`** — the host returned a post-state-root that disagrees with the group's. It earns one replay of the retained chain first, because a host rolls its diff back on a mismatch and its state survives intact; this finding counts what it burned after that replay was spent. Read it against `blocked_by_capability`: a capability block says route around this build, a divergence block says eject this host from the escrow.

**`failure_terminals`** — how many failures reached the host at all. Which failure each was is in the `counters` array beside the finding: read `terminal` there, and `phase` of `poc` marks one that is expected, because the host was proving computation and could not serve.

## What needs reporting

| code | flagged on | warning | critical |
|---|---|---|---|
| `ledger_overcounted` | nonces accounted beyond what the chain assigned | any | — |
| `ledger_disagrees_with_chain` | the drift left between this ledger and the chain once overcounting is set aside | any | — |
| `reasons_unknown` | classified nonces carrying a reason this ledger could not name, over assigned | 5% | — |

**`ledger_overcounted`** — this gateway accounted for more nonces than the chain says the slot was given, so one of the two is wrong. Worth reporting rather than acting on: no host behaviour produces this.

**`ledger_disagrees_with_chain`** — what the two sides disagree on beyond that bug. Overcounting is subtracted first, so this is the ordinary drift an operator watches rather than the impossible case.

**`reasons_unknown`** — the ledger's own honesty check: how much of this host's traffic it filed under a reason it could not name. A rising share means the gateway's instrumentation is behind its behaviour, not that the host did anything.

## Phase

Every rate above that names a host fault counts only what happened outside PoC, on both sides of the ratio. A host proving computation cannot serve, and charging it for that would make every participant look broken once an epoch.
