# Gateway v6: weight-proportional load, no forgiveness for an overloaded host

Status: concept, no code. Base: branch `devshard/gateway/v6.0.0` at `f2369877`. Every file:line below was read at that commit. A statement marked (inferred) follows from the code plus the numbers but was not observed directly.

## 1. The question

Two participants of one model in one epoch, seen from one gateway over 32 escrows:

| | A (weight 58,036) | B (weight 53,493) |
|---|---|---|
| slots | 104 | 66 |
| nonces counted by the gateway | 56,635 | 24,126 |
| delivered | 15,849 (28.0%) | 13,691 (56.7%) |
| ghost | 36,820 (65.0%) | 4,658 (19.3%) |
| refused | 0 | 168 |
| in flight | 115 | 44 |
| missed on chain | 569 (1.0%) | 2,451 (10.2%) |
| inferences on chain | 56,066 | 21,675 |
| valid on chain | 55,950 (99.8%) | 21,001 (96.9%) |

A and B have near-equal weight, A draws 2.35 times the nonces, two thirds of A's nonces are ghosts, and A still shows 1% missed. The goal is a gateway that loads each host in proportion to its weight and does not absorb a host's overload at no cost to that host.

What would settle the explanation: the code paths that bind a nonce to a host, that burn it, and that settle it on chain. All three were read and they settle the main point. What would settle the remaining numbers: A's and B's ghost counts by reason, the per-escrow nonce totals, and the model group's weights and group size on chain. None of those is in the repository, so section 2.8 lists what stays open.

## 2. What the code does today

### 2.1 A nonce names its host, and slots come from weight

The executor of an inference is `group[inference_id % len(group)]` (`devshard/state/machine.go:1197-1198`; the gateway mirrors it in `devshard/cmd/gateway/quorum/quorum.go:7-9`). A session issues nonces in order, so the gateway never picks a host. It picks an escrow, and the escrow's next nonce is already bound to one slot.

The chain fills an escrow's slots by sampling each slot independently in proportion to the model group's validation weights (`inference-chain/x/inference/keeper/msg_server_create_devshard_escrow.go:45-81`, `inference-chain/x/inference/calculations/slots.go:54-90`). Slot share equals weight share only in expectation.

Within one escrow every slot receives the same number of nonces (`devshard/cmd/gateway/accounting/classify.go:100-112`, `assignedForSlot`). A host's nonce count is therefore `sum over escrows of slots(host, escrow) x laps(escrow)`. The only thing the gateway controls is how far each escrow advances.

### 2.2 Which escrow advances

A new request ranks the candidate escrows by `(expected burns, (in flight + queued) / escrow weight)`, lower first, ties by a process-wide counter (`devshard/cmd/gateway/scheduler/escrow_pick.go:152-154`, `:206-217`, `:231-244`). Expected burns is a walk from the escrow's cursor to the first slot whose host can take the request (`escrow_pick.go:351-390`). Escrow weight is the sum of the available hosts' chain weight times their slot share in that escrow (`devshard/cmd/gateway/limits/capacity.go:75-92`, `limits/weights.go:33-43`). No term in the rank compares a host's load with its weight share.

A hedge or retry of a race is pinned to the escrow the race started on (`devshard/cmd/gateway/engine/pick.go:25-39`, `:59-72`), so it takes that escrow's next nonces whatever hosts they name.

### 2.3 What happens to the nonce: serve, burn, probe, hold

For the bound host the dispatcher asks one ladder (`devshard/cmd/gateway/scheduler/match.go:96-121`): untrusted, absent from the session, proof-of-compute required, shadow, congestion (window full or cut-off), ejected, state diverged. Any block except shadow is a burn (`match.go:38-51`, `:186-194`). A host that passes the ladder but whose window refuses the request's tokens is also turned into a burn (`scheduler/dispatcher_queue.go:55-66`). After `max_consecutive_burns` burns in a row (default 2) a full window may be crossed by an overdraft; a cut-off is never crossed (`dispatcher_queue.go:48-62`, `limits/participant.go:183-204`). One request stops after `max_burns_per_request` burns (default 16) (`dispatcher_queue.go:77-83`). Defaults: `devshard/cmd/gateway/config/defaults.go:130-138`.

The three gateway-decided blocks come from the gateway's own view of the host:

- Window full: each host has an input and an output token window. Its floor is the concurrency the host's chain weight earns, `round(weight x requests_per_10000_weight / 10000)` requests at full size, 5 per 10,000 by default (`limits/pricing.go:77-106`, `config/defaults.go:46-58`). At the default A earns 29 and B 27. Above the floor the window grows by one request per good answer and is multiplied by 0.85, 0.70 or 0.50 on overload, upstream fault or a missed receipt or first-token deadline (`limits/reaction.go:64-95`, `limits/congestion.go:67-87`, `config/defaults.go:59-65`).
- Cut-off: three consecutive transport faults open it for 5 to 60 seconds with backoff (`limits/congestion.go:75-78`, `limits/reaction.go:127-146`, `config/defaults.go:66-70`).
- Ejected: five consecutive failures, or a failure rate of 15% over at least 20 attempts, for 30 to 60 seconds, capped at half the pool (`perf/ejection.go:38-58`, `perf/tracker.go:355-364`, `config/defaults.go:101-110`).

### 2.4 What a ghost is, and who pays for it

A burn commits a real start-inference with the prompt `.` and `max_tokens` equal to the state machine's floor of 1, and sends it to nobody (`scheduler/dispatcher.go:307-318`, `registry/session.go:18-22`, `:294-337`, `devshard/state/machine.go:55`). The gateway records the reason and does nothing else with the nonce: no send, no timeout vote.

The record stays pending. When the escrow settles, every record still pending or started is marked finished and its whole reservation is credited to the executor slot, with no miss (`devshard/state/machine.go:1759-1778`, called from `devshard/state/seal.go:441`). On chain the host is paid that cost plus an equal per-slot share of the fees (`inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go:101-122`), and its epoch statistics are computed as `completed = assigned per slot - missed`, `validated = completed - invalid` (`inference-chain/x/inference/keeper/devshard_host_stats.go:89-124`, fed by `msg_server_settle_devshard_escrow.go:219-237`).

So a ghost is not free for the host: it is in the host's favour. The payer of the escrow pays the floor reservation and the per-nonce fee. The bound host is paid the floor reservation and credited one completed, validated inference. The gateway's own routing document says the same about the reservation (`devshard/cmd/gateway/docs/routing.md:191`); the code agrees with it. `scheduler/ghost.go:3` calls a burn a "silent ghost probe", which is inaccurate: nothing is sent.

### 2.5 What turns a nonce into a miss

Only a timeout or error-miss transaction that gathers more accept weight than the vote threshold increments `Missed` (`devshard/state/machine.go:1596-1659`, `:1716-1724`). The gateway raises one only for an attempt it actually sent: refused when no receipt arrived, execution when a receipt arrived but no finish, error-miss when the stream carried a provable error (`devshard/cmd/gateway/engine/settle.go:63-71`, `:109-140`). Verifiers accept a refusal only after 60 seconds and after challenging the executor with the payload, and an execution timeout only 32 minutes after the receipt (`devshard/host/timeout.go:226-277`, `:343-391`, `devshard/types/config.go:17-21`).

Three paths end as settled work instead of a miss: a ghost (never voted), a sent nonce whose vote is skipped or fails, and a nonce whose executor holds so many slots of the group that the other slots cannot pass the threshold (`engine/settle.go:234-239`, `quorum/quorum.go:12-24`). The last two are paid the full reservation of the real request, not the floor.

### 2.6 The tracker categories, from the code

The gateway ledger files every committed nonce under one disposition (`devshard/cmd/gateway/accounting/vocabulary.go:9-16`, `accounting/classify.go:30-54`):

- ghost: a burn of 2.3, any reason; the reason is part of the counter key.
- delivered: `finished_used`, `finished_unused` and `finished_usage_unknown` together, a nonce whose finish reached the gateway, winner or loser.
- refused: `unfinished_refused`, a sent nonce with no receipt whose timeout round ended (`classify.go:87-97`).
- in flight: sent, not yet finished or timed out.
- missed and invalid: read from the escrow state's per-slot statistics, the same counters the chain settles (`accounting/tally.go:7-8`).
- inferences on chain: assigned minus missed, by the chain formula of 2.4. For A, 56,635 - 569 = 56,066; for B, 24,126 - 2,451 = 21,675. Both match.
- empty stream: a delivery reason of an attempt (`engine/vocabulary.go:157`); a finished empty stream is not voted (`engine/settle.go:93-94`).
- no verifiers: timeout rounds that ended `vote_collection_failed` (`accounting/vocabulary.go:48-54`).

### 2.7 Reading A and B

Subtract the ghosts and the picture changes. A's nonces that were actually dispatched or used for service are 56,635 - 36,820 = 19,815. B's are 24,126 - 4,658 = 19,468. The ratio is 1.02 against a weight ratio of 1.085. Real sends already track weight. (inferred) This is the weight-scaled window of 2.3 at work: both hosts are filled to roughly what their weight earns, 29 and 27 concurrent requests at default settings.

The 2.35 times in nonces has two factors. A holds 1.58 times the slots of B (104 against 66). And A's slots each drew 545 nonces against 366 for B's, a factor of 1.49. (inferred) The second factor is the burns themselves: every burn advances its escrow by one nonce, so an escrow with many slots of a blocked host runs through more nonces per served request, and all its slots are counted for them. A is by definition present in every escrow that is heavy in A.

Of A's 56,066 inferences on chain, 36,820 (65.7%) are ghosts: nonces A never received, each settled as completed and validated, each paid the floor reservation. A's 569 misses are 2.9% of the 19,815 nonces that could have missed. B's 2,451 are 12.6% of its 19,468. A's 1% is low because two thirds of its denominator cannot miss.

The asymmetry is therefore not that A is chosen more often. A holds more slots than B, the gateway's window admits about the same real load to both, and the surplus of A's slots is burned in A's favour. B, with fewer slots, has less surplus, so more of its nonces are real sends, and a slow host turns real sends into misses.

### 2.8 What the code does not explain

- Why A has 104 slots and B 66 at near-equal weight. The sampler is weight-proportional per slot. Either the model group's validation weights differ from the displayed weights, or this is sampling variance. The group weights on chain would settle it.
- Which reason dominates A's ghosts (window full, cut-off, ejected, exclusion after a retry, proof-of-compute). The ledger keys ghosts by reason; the breakdown was not in the input. The design below treats the three gateway-decided reasons the same way, so it does not depend on the answer.
- The nonces that are neither delivered, ghost, refused nor in flight: 3,851 for A and 5,565 for B. They are execution timeouts, pending classification and service nonces (timeout, heartbeat, finalize) bound to the host's slots; the split was not in the input.
- TTFT means of 117 s and 825 s. The first-token ceiling is 30 s and the loser grace 600 s by default (`config/defaults.go:125-133`); no path read here produces a mean of 825 s, and the measurement itself was not checked.
- Whether any escrow makes A unvoteable. It needs A to hold at least `group size - threshold` slots of one group; the group size and the threshold factor are chain parameters not read here.

## 3. Design

### 3.1 Rules

1. A host's share is its chain weight share for the model. Slots are how the share is delivered; the gateway corrects slot deviations through the one lever it has, which escrow advances.
2. A nonce bound to a host the gateway itself judged busy or broken is never a ghost. It is the host's work: it ends as a finish by that host or as a posted timeout vote.
3. A nonce a host does not accept counts as consumed share, and reduces the share the gateway offers next.
4. No new burn is introduced, and nothing is throttled by burning. Vote and verification load is bounded by pacing.
5. One gateway version talks to one host version, so no compatibility path. Switches end in `_ENABLED`.

### 3.2 Share ledger (cmd/gateway: `limits`, `accounting`)

Per host and model, over a rolling window (the existing 600 s half-life, `config/defaults.go:102`) and cumulatively per epoch, reset where host state is already reset at the epoch switch (`app/epoch_reset.go:39-48`):

- `offered`: nonces bound to the host while a compatible request was waiting.
- `accepted`: of those, real sends.
- `passed`: of those, not sent because of window full, cut-off, ejection or a spent chat budget.
- `served`: accepted sends that finished inside their deadlines.
- `weight_share`: the host's weight over the weight of all hosts in the model's live escrows, from the chain snapshot the limiter already reads (`limits/capacity.go:94-102`).
- `slot_share`: its slots over all slots of those escrows (`registry/registry.go:457`).

Two derived values: `health = accepted / offered`, floored at 0.25 so a host is always offered enough to recover, and `target = weight_share x health`, renormalised over the model. The ledger is published in the accounting report next to the dispositions. This part changes no behaviour and can ship first.

### 3.3 Assignment: rank by passes, then by deficit (cmd/gateway: `scheduler`)

`deficit(host) = target(host) x total offered - offered(host)`, clamped to plus or minus the host's weight-earned concurrency so a host returning from a cut-off catches up by at most one window. The escrow rank becomes `(passes to the first usable slot, -deficit of that slot's host, load ratio)`. The first key is today's expected burns under a new name; the walk already finds the serving slot and only has to return its host. The second key is new and replaces nothing that exists: among escrows that can serve at once, the request goes to the host furthest below its target.

Effect on slots: an escrow heavy in a host that is over its target advances less, so a sampling surplus such as 104 against 66 no longer becomes load. Effect on overload: a host that accepts 35% of what it is offered has its target cut to 35% of its weight share. With weight-entitled nonces `c` and capacity `K` below it, offered settles at `sqrt(c x K)` rather than at the slot share. For A, taking `c` as 1.085 times B's nonces (about 26,200) and `K` as today's 19,815, that is about 22,800 offered and 3,000 passed instead of 36,820. This is an illustration of the fixed point, not a forecast.

Fewer assigned nonces is a consequence the host feels on chain without any chain change: its completed count and its payout are both derived from assigned nonces.

### 3.4 Overload rule

A host is over its share and not serving it when `offered / total offered` is at or above its weight share and `passed + timed out` is a sustained fraction of `offered`. The signal is the pass itself: the gateway had work for the host on the host's own nonce and the host's window, cut-off or ejection state refused it. TTFT is not a second signal; it already feeds the window through the missed-deadline verdicts of 2.3. In-flight against share is the window floor of 2.3 and stays as it is: inside the concurrency its weight earns, a host is always sent real work.

Burst and sustained: one pass changes nothing by itself; `health` is an average with a 600 s half-life and a floor of 0.25, and the deficit is clamped to one window. The rule needs at least 20 offered nonces in the window, the volume the ejection rule already uses.

### 3.5 Consequence: a pass is a floor probe or an owed vote, never a ghost (cmd/gateway: `scheduler`, `shadow`, `engine`)

`match` returns a probe instead of a burn for window full, cut-off and ejected. The probe is the placeholder a ghost commits today, the `.` prompt with one output token, but it is sent to the bound host through the runner that already delivers shadow probes and settles their nonces (`scheduler/shadow.go:22-48`, `shadow/runner.go:58-78`).

- The host answers: it ran a one-token inference, the nonce is finished, the payer pays at most the floor, as today.
- The host gives no receipt in 60 s: a refused vote is posted, the host takes a miss, the floor reservation returns to the escrow.
- The host receipts and never finishes: an execution vote after 32 minutes, a miss.
- No send is possible (cut-off open, chat budget spent): the nonce goes straight to the owed-vote queue. The verifiers challenge the host with the payload themselves, so the host still gets its chance to receipt.

A probe takes no window tokens and feeds neither the window nor the failure rate; a transport fault on it counts toward the cut-off as any fault does.

Burns stay for the blocks that are not the gateway's judgement of load: proof-of-compute required, outside the trust list, state diverged, absent from the session, excluded by the request, abandoned, failed prepare. The forced send, the burn cap of a request (a probe counts toward it, the payer's cost being the same) and the attempt cap are unchanged.

### 3.6 Who pays

| Nonce bound to host H | Today | Proposed |
|---|---|---|
| H busy, H answers the probe | ghost: payer pays floor and fee, H paid floor and credited an inference for no work | payer pays at most floor and fee, H paid for a one-token inference it ran |
| H busy or broken, H does not answer | same ghost, H paid and credited | H takes a miss, floor refunded, payer pays the fee of the probe nonce and of the vote nonce |
| H over its target | escrow advances anyway, H credited | escrow advances less, H is assigned and credited fewer nonces |
| No escrow has a usable host | 503 with no nonce spent | unchanged |
| User | waits for the burn walk | waits for the same walk; no added attempt, no added latency |

The vote nonce is the new cost: the ledger charges a fee for every service nonce except finalize (`accounting/vocabulary.go:18-27`, `accounting/tally.go:79-82`), and it uses one position under the escrow's nonce cap.

### 3.7 Limits, per layer

- Probe sends: one small chat per pass. Bounded per host by a cap on unresolved probes, default the host's weight-earned concurrency. Past the cap a pass goes to the owed-vote queue without a send.
- Votes: paced by the existing `engine_max_concurrent_timeout_votes` (64, `config/defaults.go:132`) plus a new per-host budget per minute. Burst: a host that dies with a 20% slot share turns one nonce in five into an owed vote at once; the queue absorbs it. Sustained: votes are posted at the paced rate until the escrow settles.
- Residue: a vote still queued when its escrow settles is not posted, and that nonce settles as a ghost does today. The ledger counts it as `pass_unvoted` per host. This is the one place forgiveness remains, it is bounded by vote capacity, and it is reported instead of hidden.
- Nonce cap: every vote spends a nonce inside the 200-nonce margin the pick keeps below the hosts' cap (`docs/routing.md:23`). The per-host vote budget must be sized so that probes plus votes of one escrow lap fit the half of the margin reserved for timeout and validation diffs.
- Host peer budget: a host whose advertised chat budget is spent is not probed; it goes to the vote queue, which is verifier traffic and not this gateway's chats.

### 3.8 Layers

- `devshard/cmd/gateway`: all of 3.2 to 3.7. `limits` and `accounting` for the ledger, `scheduler` for the rank and the probe decision, `shadow` for delivery, `engine` for vote pacing, `config` and `env` for the switches.
- `gatewayprotocol`: no package or directory of that name exists at this commit. The wire between gateway and host is `devshard/user`, `devshard/host` and `devshard/transport`, and this design changes none of them.
- Shared devshard packages, optional and not required by the gateway part: a refusal vote that covers a list of nonces of one executor, which would shrink the residue of 3.7; a per-slot finished counter in the host statistics.
- Chain, out of the gateway's reach: the epoch statistics count `assigned - missed` as completed (`devshard_host_stats.go:95`), so a probe the host answers, and any residue, still count as inferences. Counting finished work needs that formula and the settlement payload to change. A cap on one validator's slots per group in the sampler would bound both the slot surplus and unvoteable groups. Lowering the execution timeout is a governance parameter and is the only way a host that answers slowly, but answers, takes a miss.

### 3.9 Switches

`GATEWAY_SHARE_LEDGER_ENABLED`, `GATEWAY_SHARE_DEFICIT_PICK_ENABLED`, `GATEWAY_BUSY_PROBE_ENABLED`, each with a settings key read per pick or per drain as `max_burns_per_request` is, all off by default until the harness run of section 5 passes. Tunables: `share_health_floor` (0.25), `busy_probe_max_unresolved_per_host` (0 means weight-earned concurrency), `busy_vote_budget_per_host_per_minute`.

## 4. Alternatives rejected

- Keep the ghost and post a refused vote on each. It reaches the same miss, but every pass costs a vote round across the group instead of one send, and the vote load is unbounded without the same pacing.
- Send the real request over the full window and let the host drown. The cost lands on the user (a slow attempt, one of two attempts spent) and on the payer (a late loser is paid in full, a timed-out one locks its reservation for 32 minutes). A slow host is paid more than under a ghost.
- Park the escrow until the host has room, never pass. Nonces are sequential, so every escrow stalls behind its slowest high-share host and the model's throughput falls to that host's pace divided by its slot share.
- Narrow the host's window below its weight floor when it is overloaded. Under today's ladder a narrower window is more burns, which pays the host more.
- Take the host off the trust list. Every nonce on it becomes a burn, the largest forgiveness available.
- Retire and recreate escrows until the slot layout matches weight. Each creation costs chain fees and locks funds, and the next sample deviates again.

## 5. Measurements

Before the change, to close 2.8: A's and B's ghosts by reason, nonces per escrow against A's slot count per escrow, and the model group's weights.

Acceptance, same model and a comparable epoch, switches on:

- Ghosts with reason window full, cut-off or ejected: zero for every host.
- Offered nonces per host within 10% of weight share over the epoch where the slot layout allows it; for A and B a ratio near 1.085 instead of 2.35.
- A's passes fall by an order of magnitude from 36,820, and each is filed as probe finished, probe missed or `pass_unvoted`. No nonce of A is pending at settlement for a gateway-decided reason.
- A's missed count rises exactly by its unanswered probes. If A answers every probe its missed rate stays near today's, and the report shows why: `accepted / offered` and `served / accepted` per host.
- B's sends, misses and miss rate do not move, because the paths that produced them are untouched.
- Payer cost per served request does not rise by more than the vote fees; p50 and p95 TTFT of the model do not rise.
- `pass_unvoted` is a small fraction of passes; if not, the vote budget or a list vote is the next step.

Reproduction of today's A and B, to be written as failing-then-passing tests:

- Scenario test in `devshard/cmd/gateway/scenarios`, which runs the composed gateway on fake time against a fake chain and real in-process hosts (`scenarios/README.md:3`). Two hosts of equal weight, the fake chain giving A 13 of 64 slots and B 8 across four 16-slot escrows. A answers fast but holds a fixed concurrency; B takes minutes to its first token. Assert today: more than half of A's nonces are ghosts, the settled host statistics show them as completed with no miss, A's nonces per slot exceed B's, real sends of A and B are within 10%. Assert after: the acceptance list above.
- Scheduler unit tests beside `burn_forecast_test.go` and `shadow_test.go`: the rank prefers the cursor host with the larger deficit; a full window, a cut-off and an ejection each yield a probe and no ghost; the forecast equals the passes the drain makes.
- Load harness in `devshard/testenv/loadtest`: a scenario with one capped host and one slow host and the existing `max_ghost_rate` assertion (`loadtest/scenario.go:261`). Whether the harness can set per-host weights and slot layouts was not checked.

## 6. Adjacent leads, not pursued

- Settlement waives all devshard misses of a participant with an active challenge (`msg_server_settle_devshard_escrow.go:227-228`, marked TODO).
- Fees are split equally per slot whatever the slot did (`msg_server_settle_devshard_escrow.go:101-122`).
- A sent nonce whose vote is skipped as unvoteable settles at the full reservation of the request (2.5).
- B performed no validations in the epoch while A performed 3,344.
