# Gateway v6: host load mode (off, weight, balance)

Status: implemented in `devshard/cmd/gateway` (setting, share ledger, rank, pass rule); the load-harness scenario of section 9 is pending. Base: branch `devshard/gateway/v6.0.0` at `f2369877`. Every file and line below was read at that commit. A statement marked (inferred) follows from the code plus the numbers but was not observed directly.

## 1. The problem

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

A and B have near-equal weight, A draws 2.35 times the nonces, two thirds of A's nonces are ghosts, and A still shows 1% missed. This document specifies one setting, `host_load_mode`, that decides how the gateway spreads load over hosts: `off` keeps today's behaviour, `weight` loads each host by its weight share, `balance` loads each host by its weight share scaled by how much of its offered load it serves.

## 2. What the code does today

### 2.1 A nonce names its host, and slots come from weight

The executor of an inference is `group[inference_id % len(group)]` ([`devshard/state/machine.go:1197-1198`](../../devshard/state/machine.go#L1197-L1198); the gateway mirrors it in [`quorum/quorum.go:7-9`](../../devshard/cmd/gateway/quorum/quorum.go#L7-L9)). A session issues nonces in order, so the gateway never picks a host. It picks an escrow, and the escrow's next nonce is already bound to one slot.

The chain fills an escrow's slots by sampling each slot in proportion to the validation weights of the model's epoch group, leaving out members under challenge ([`inference-chain/x/inference/keeper/msg_server_create_devshard_escrow.go:45-81`](../../inference-chain/x/inference/keeper/msg_server_create_devshard_escrow.go#L45-L81)). A member's weight in a model group is the sum of the proof-of-compute weights of its nodes for that model ([`inference-chain/x/inference/epochgroup/epoch_group.go:262-272`](../../inference-chain/x/inference/epochgroup/epoch_group.go#L262-L272), stored at [`inference-chain/x/inference/epochgroup/epoch_group.go:182-189`](../../inference-chain/x/inference/epochgroup/epoch_group.go#L182-L189)). Slot share equals weight share only in expectation.

Within one escrow every slot receives the same number of nonces ([`accounting/classify.go:100-112`](../../devshard/cmd/gateway/accounting/classify.go#L100-L112)). A host's nonce count is therefore `sum over escrows of slots(host, escrow) x laps(escrow)`. The only thing the gateway controls is how far each escrow advances.

### 2.2 Which escrow advances

A new request sorts the candidate escrows into money tiers (residue, funded, thin; regular escrows before reserves) and inside a tier ranks them by `(expected burns, (in flight + queued) / escrow weight)`, lower first, ties by a process-wide counter ([`scheduler/escrow_pick.go:95-106`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L95-L106), [`scheduler/escrow_pick.go:147-154`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L147-L154), [`scheduler/escrow_pick.go:175-246`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L175-L246)). Expected burns is a walk from the escrow's cursor to the first slot whose host can take the request; a shadow slot is stepped over and not counted ([`scheduler/escrow_pick.go:351-390`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L351-L390)). Escrow weight is the sum of the available hosts' weight times their slot share in that escrow ([`limits/capacity.go:75-92`](../../devshard/cmd/gateway/limits/capacity.go#L75-L92), [`limits/weights.go:33-43`](../../devshard/cmd/gateway/limits/weights.go#L33-L43)). No term compares a host's load with its weight share. A hedge or retry stays on the escrow its race started on ([`engine/pick.go:25-72`](../../devshard/cmd/gateway/engine/pick.go#L25-L72)).

### 2.3 What happens to the nonce

For the bound host the dispatcher asks one ladder ([`scheduler/match.go:96-121`](../../devshard/cmd/gateway/scheduler/match.go#L96-L121)): untrusted, absent from the session, proof-of-compute required, shadow, congestion (window full or cut-off), ejected, state diverged. Every block except shadow is a burn ([`scheduler/match.go:38-51`](../../devshard/cmd/gateway/scheduler/match.go#L38-L51), [`scheduler/match.go:186-194`](../../devshard/cmd/gateway/scheduler/match.go#L186-L194)). A host that passes the ladder but whose window refuses the request's tokens is also burned ([`scheduler/dispatcher_queue.go:55-66`](../../devshard/cmd/gateway/scheduler/dispatcher_queue.go#L55-L66)). After `max_consecutive_burns` burns in a row (default 2) a full window may be crossed by an overdraft; one request stops after `max_burns_per_request` burns (default 16) ([`scheduler/dispatcher_queue.go:41-83`](../../devshard/cmd/gateway/scheduler/dispatcher_queue.go#L41-L83), [`config/defaults.go:135-139`](../../devshard/cmd/gateway/config/defaults.go#L135-L139)).

The window is the gateway's measure of what a host can carry. Its floor is the concurrency the host's weight earns, `round(weight x requests_per_10000_weight / 10000)` full-size requests, 5 per 10,000 by default ([`limits/pricing.go:77-106`](../../devshard/cmd/gateway/limits/pricing.go#L77-L106), [`config/defaults.go:46-50`](../../devshard/cmd/gateway/config/defaults.go#L46-L50)): 29 for A and 27 for B. It grows by one request per good answer with no ceiling ([`limits/congestion.go:157-162`](../../devshard/cmd/gateway/limits/congestion.go#L157-L162), [`limits/window.go:47-53`](../../devshard/cmd/gateway/limits/window.go#L47-L53)) and is multiplied by 0.85, 0.70 or 0.50 on bad answers, but never below the floor ([`limits/reaction.go:121-125`](../../devshard/cmd/gateway/limits/reaction.go#L121-L125), [`limits/congestion.go:164-167`](../../devshard/cmd/gateway/limits/congestion.go#L164-L167)). So the size of the window can show that a host carries more than its weight earns and cannot show that it carries less.

### 2.4 A ghost pays the host

A burn commits a real start-inference with the prompt `.` and one output token and sends it to nobody ([`registry/session.go:18-22`](../../devshard/cmd/gateway/registry/session.go#L18-L22), [`registry/session.go:294-337`](../../devshard/cmd/gateway/registry/session.go#L294-L337), [`devshard/state/machine.go:54-55`](../../devshard/state/machine.go#L54-L55)). The record stays pending. When the escrow settles, every pending or started record is marked finished and its reservation is credited to the executor slot, with no miss ([`devshard/state/machine.go:1759-1778`](../../devshard/state/machine.go#L1759-L1778)). The chain then counts `completed = assigned per slot - missed` ([`inference-chain/x/inference/keeper/devshard_host_stats.go:89-124`](../../inference-chain/x/inference/keeper/devshard_host_stats.go#L89-L124)) and splits the fees per slot ([`inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go:101-122`](../../inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go#L101-L122)). A ghost is therefore a paid, completed, validated inference that the host never received. A miss comes only from a timeout vote on a nonce that was sent ([`engine/settle.go:234-239`](../../devshard/cmd/gateway/engine/settle.go#L234-L239) for the case no vote can pass).

### 2.5 Reading A and B

Without ghosts A has 56,635 - 36,820 = 19,815 nonces and B has 24,126 - 4,658 = 19,468, a ratio of 1.02 against a weight ratio of 1.085. Real sends already follow weight, (inferred) because the window floor is weight-scaled. A holds 1.58 times B's slots, and the surplus of A's slots is burned in A's favour: 65.7% of A's on-chain inferences are ghosts, which is why its missed rate is 1%. Against the nonces that could miss, A's rate is 2.9% and B's is 12.6%.

Not explained by the code: why A has 104 slots and B 66 at near-equal weight (the group weights on chain would settle it), which burn reason dominates A's ghosts, and whether A-heavy escrows advanced more than the model's average or B-heavy ones less (per-escrow nonce totals would settle it).

## 3. The setting

`host_load_mode` is a string with three values: `off`, `weight`, `balance`. It is a scheduler setting, it exists for the network and per model, and no boolean is added.

### 3.1 Where it plugs in

The gateway already has one mechanism for a setting that is global with a per-model override, and the new setting uses it without changing it:

| Layer | Existing mechanism | Addition |
|---|---|---|
| Environment and admin, global | `env.Tunables`, embedded in `env.Values` and `config.Overrides` and applied once per layer by `applyTunables` ([`env/tunables.go:3-51`](../../devshard/cmd/gateway/env/tunables.go#L3-L51), [`config/overrides.go:12-33`](../../devshard/cmd/gateway/config/overrides.go#L12-L33), [`config/build.go:13-16`](../../devshard/cmd/gateway/config/build.go#L13-L16), [`config/build.go:75-76`](../../devshard/cmd/gateway/config/build.go#L75-L76), [`config/build.go:125-136`](../../devshard/cmd/gateway/config/build.go#L125-L136)) | field `HostLoadMode *string` with tag `json:"host_load_mode,omitempty"`, applied with `overrideIfSet` beside `MaxBurnsPerRequest` |
| Environment name | `GATEWAY_*` scheduler names read in `env.Load` ([`env/env.go:289-292`](../../devshard/cmd/gateway/env/env.go#L289-L292)) | `GATEWAY_HOST_LOAD_MODE`, read with `readString` |
| Per model | `config.ModelSettings`, one optional field per model-scoped setting, spelled with the network key ([`config/model_settings.go:11-58`](../../devshard/cmd/gateway/config/model_settings.go#L11-L58), [`config/model_settings.go:92-143`](../../devshard/cmd/gateway/config/model_settings.go#L92-L143)) | field `HostLoadMode *string`, same tag, same `overrideIfSet` into `Scheduler.HostLoadMode` |
| Per model in the environment | `GATEWAY_MODELS_JSON`, the same document as the admin `models` key ([`config/models_build.go:72-83`](../../devshard/cmd/gateway/config/models_build.go#L72-L83)) | `{"<model id>": {"host_load_mode": "balance"}}` |
| Snapshot | `config.Scheduler` ([`config/config.go:193-197`](../../devshard/cmd/gateway/config/config.go#L193-L197)), default in `Defaults()` ([`config/defaults.go:135-139`](../../devshard/cmd/gateway/config/defaults.go#L135-L139)) | field `HostLoadMode string`, default `off`; constants `HostLoadModeOff`, `HostLoadModeWeight`, `HostLoadModeBalance` beside the `ModelAccess` values ([`config/config.go:243-258`](../../devshard/cmd/gateway/config/config.go#L243-L258)) |
| Reader | `Config.For(model)` returns the model's resolved copy, else the network one ([`config/model_settings.go:60-66`](../../devshard/cmd/gateway/config/model_settings.go#L60-L66)); the scheduler reads it per drain and per pick ([`scheduler/gates.go:86-101`](../../devshard/cmd/gateway/scheduler/gates.go#L86-L101)) | accessor `hostLoadMode(model)` beside `maxBurnsPerRequest`, read once per pick and once per drain |

Admin API: `GET`, `PUT` and `POST /v1/admin/settings` ([`api/routes.go:56`](../../devshard/cmd/gateway/api/routes.go#L56), [`api/admin.go:43-102`](../../devshard/cmd/gateway/api/admin.go#L43-L102)). A `PUT` is the whole document, a `POST` is a patch, and `null` clears a field or a model ([`config/overrides_merge.go:19-81`](../../devshard/cmd/gateway/config/overrides_merge.go#L19-L81)):

```json
{"host_load_mode": "weight", "models": {"<model id>": {"host_load_mode": "balance"}}}
```

### 3.2 Precedence

Lowest to highest: default `off`, `GATEWAY_HOST_LOAD_MODE`, admin `host_load_mode`, `GATEWAY_MODELS_JSON` for the model, admin `models.<id>.host_load_mode`. A model's value always wins over the global one, whichever layer set either, because `resolveModels` copies the finished network configuration and lays the model's environment block and then its admin block over it ([`config/models_build.go:45-48`](../../devshard/cmd/gateway/config/models_build.go#L45-L48)). A model that sets nothing inherits the network value.

### 3.3 Validation

- A value other than the three is refused in `validateScheduler`, beside the burn limits ([`config/validate.go:383-391`](../../devshard/cmd/gateway/config/validate.go#L383-L391)), with `host_load_mode: "<value>" is not off, weight or balance`, the wording `access` uses ([`config/validate.go:237-239`](../../devshard/cmd/gateway/config/validate.go#L237-L239)). The spelling is exact and lower-case in every layer. An empty string is refused; `null` is how a value is cleared.
- For a model the same rule runs on the model's resolved copy and the problem is prefixed `models[<id>].` ([`config/models_build.go:45-53`](../../devshard/cmd/gateway/config/models_build.go#L45-L53)).
- An admin write with a bad value is answered 400, nothing is stored and the running configuration stays, because `Reconfigure` builds and validates before it saves and swaps ([`app/operations.go:305-327`](../../devshard/cmd/gateway/app/operations.go#L305-L327)).
- A misspelt key is a 400 on an admin write ([`config/overrides.go:42-51`](../../devshard/cmd/gateway/config/overrides.go#L42-L51)) and an error in `GATEWAY_MODELS_JSON` ([`config/models_build.go:76-80`](../../devshard/cmd/gateway/config/models_build.go#L76-L80)). The read-back of stored settings accepts unknown keys ([`config/overrides.go:53-60`](../../devshard/cmd/gateway/config/overrides.go#L53-L60)), so a build without this setting that reads a stored `host_load_mode` ignores it and runs as `off`.
- `GATEWAY_HOST_LOAD_MODE` follows the strict grammar of the other `GATEWAY_*` scheduler variables: a bad value fails `Build`.

### 3.4 Hot reload

An admin write publishes a new snapshot through `Holder.Swap` ([`config/holder.go:22-37`](../../devshard/cmd/gateway/config/holder.go#L22-L37)). The scheduler reads the mode from the snapshot at each pick and each drain, so a change reaches running escrows without a restart; a drain in progress finishes under the mode it read. Probes and owed votes that were already committed are finished under any mode. The share ledger keeps counting across a mode change. An environment change needs a restart, as for every variable.

## 4. Shared by `weight` and `balance`

### 4.1 Which weight

`weight(h)` is `Capacity.ParticipantWeight(h, model)` ([`limits/capacity.go:94-102`](../../devshard/cmd/gateway/limits/capacity.go#L94-L102)): the lower of the full and the current per-model weight in the chain snapshot. The full per-model weight is the sum of the proof-of-compute weights of the host's nodes for that model ([`chain/participants_parse.go:162-194`](../../devshard/cmd/gateway/chain/participants_parse.go#L162-L194)), which is the same sum the chain stores as the member's validation weight in the model group and samples slots from (2.1). Reasons:

- It is the weight the chain used to hand out the slots, so the gateway corrects sampling deviation toward the sampler's own expectation.
- It is per model. The network-wide epoch weight sums a host's nodes over all models and would let weight earned on one model buy load on another.
- During a proof-of-compute phase the current view counts only preserved nodes, so a host's target follows the part of its weight that is serving, and a host with no preserved node has weight 0.
- It is the number the window floor is already priced from ([`app/capacity.go:38-54`](../../devshard/cmd/gateway/app/capacity.go#L38-L54)), so share and window agree.

`weight_share(h) = weight(h) / sum of weight(k)`, over the non-shadow hosts that hold at least one slot in an escrow of the model that accepts requests.

### 4.2 Share ledger

Per host and model, as decayed counts with the model's `perf_ewma_halflife_seconds` (600 s, [`config/defaults.go:101-110`](../../devshard/cmd/gateway/config/defaults.go#L101-L110)) and as epoch totals for reporting. Both are reset where host state is reset at the epoch boundary ([`app/epoch_reset.go:39-48`](../../devshard/cmd/gateway/app/epoch_reset.go#L39-L48)).

- `assigned`: every nonce the drain bound to one of the host's slots; warm-up and vote nonces are not drained and are not counted.
- `offered`: nonces the drain bound to the host while a request was waiting, abandoned and failed-prepare nonces excepted. It is the sum of the next four plus the sends still in flight.
- `served`: sent and finished inside the request's deadlines.
- `failed`: sent and ended in a refusal, a timeout or a transport fault. An attempt the gateway cancelled itself is counted in neither.
- `passed`: not sent because the window was full, the cut-off was open, the host was ejected or its chat budget was spent.
- `excused`: not sent for a reason that is not the host's load: proof-of-compute required, outside the trust list, absent from the session, state diverged, excluded by the request.
- `weight_share` (4.1) and `slot_share`, the host's slots over all slots of the same escrows.

The ledger counts in every mode, `off` included, because it changes no routing and is the only way to see target against actual before a mode is switched on.

### 4.3 A pass is a one-token probe or an owed vote, never a ghost

Under `weight` and `balance`, a nonce the gateway does not send because of its own judgement of the host (window full, cut-off, ejected, chat budget spent) is a pass. A pass commits the placeholder a ghost commits today, and the committed nonce is then delivered:

- `Advance` already returns a prepared nonce for a ghost ([`registry/session.go:294-337`](../../devshard/cmd/gateway/registry/session.go#L294-L337)). The probe runner's `Probe` needs only that prepared nonce and its params, sends on its own goroutine, and settles a failed send with a refused vote, or an execution vote when a receipt arrived ([`shadow/runner.go:78-138`](../../devshard/cmd/gateway/shadow/runner.go#L78-L138), [`warmup/settle.go:22-71`](../../devshard/cmd/gateway/warmup/settle.go#L22-L71)). A pass reuses this delivery with the placeholder as params.
- It differs from a shadow probe in four ways. It carries the `.` prompt and one token, not a repeated client body. It is bounded by the receipt timeout and the first-token ceiling instead of the attempt's hard timeout. It is filed under its own terminal `busy_probe`. It is capped per host, where the shadow runner starts one goroutine per probe with no cap ([`shadow/runner.go:84-90`](../../devshard/cmd/gateway/shadow/runner.go#L84-L90)).

Outcomes: the host answers and the nonce is a finished one-token inference; no receipt in 60 s and a refused vote gives the host a miss and returns the reservation; a receipt and no finish and an execution vote gives a miss after the execution timeout ([`devshard/types/config.go:17-21`](../../devshard/types/config.go#L17-L21)); no send is possible (cut-off open, chat budget spent, probe cap reached) and the nonce goes straight to the owed-vote queue, where the verifiers challenge the host themselves.

A probe takes no window tokens and feeds neither the window nor the failure rate; a transport fault on it counts toward the cut-off as any fault does. A pass counts toward `max_consecutive_burns` and `max_burns_per_request` as a shadow probe already counts toward the first ([`scheduler/dispatcher_queue.go:153-158`](../../devshard/cmd/gateway/scheduler/dispatcher_queue.go#L153-L158)), so the forced send and the 503 at the cap behave as today.

Burns stay, in every mode, for blocks that are not the gateway's judgement of load: proof-of-compute required, outside the trust list, state diverged, absent from the session, excluded by the request, abandoned, failed prepare.

### 4.4 Limits on passes

- Probes in flight per host: at most the host's weight-earned concurrency (29 for A). Above it a pass is an owed vote with no send.
- Vote rounds: the existing pacer, 10 rounds a second with a burst of 20 ([`warmup/pace.go:10-42`](../../devshard/cmd/gateway/warmup/pace.go#L10-L42)). The pacer starts a round at once when it would wait more than two minutes, so it is not a hard bound by itself.
- Hard bound, at the pick: an escrow carries at most 100 unresolved passes (probes in flight plus votes not yet posted), half of the 200-nonce margin the pick keeps under the nonce cap ([`scheduler/escrow_pick.go:17`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L17), [`scheduler/escrow_pick.go:397-426`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L397-L426)), so that the vote of every unresolved pass still fits under the cap. At the bound, a walk that needs another pass on that escrow is forecast unusable; the request takes another escrow, or is answered overloaded with no nonce spent when none is left. A drain that meets the bound answers its waiters busy, with no nonce spent, and their requests take another escrow. The starting value is to be confirmed by the harness run of section 9.
- Residue: a pass whose vote cannot be posted (no poster for the escrow, a failed vote round, or the gateway ending) settles as a ghost does today. The ledger counts it as `pass_unvoted` per host. The gateway never chooses this outcome; it is bounded by 100 per escrow and it is reported.

## 5. The modes

### 5.1 `off`

Nothing changes. The rank is `(expected burns, load)`, a busy host's nonce is a ghost, the payer pays the floor reservation and the fee, and the host is paid and credited an inference. The ledger counts and reports what `weight` would target.

### 5.2 `weight`

Target: `target(h) = weight_share(h)`.

Selection. For each host `surplus(h) = clamp(offered(h) - target(h) x total offered, -window(h), +window(h))`, in nonces over the decayed counts, where `window(h)` is the number of requests the host's two windows admit now (`min(free input tokens / input step, free output tokens / output step)`, free being the window less what is in flight, `ParticipantLimiter.RequestRoom`; 0 while cut off, unbounded for a host with no window). The walk of the forecast ([`scheduler/escrow_pick.go:351-390`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L351-L390)) already visits every slot up to the serving one; it sums the surplus of the hosts it crosses, passes and the serving host alike, shadow slots excluded. `pickRank` becomes `(burns, walk surplus, load)`, where the burns of a walk include its passes ([`scheduler/escrow_pick.go:147-154`](../../devshard/cmd/gateway/scheduler/escrow_pick.go#L147-L154)). Fewest burns and passes first keeps the payer's cost per request lowest; among equal walks the request takes the one that touches the hosts furthest below their target, so an escrow heavy in a host above its target advances less. Money tiers, reserves, the pinned escrow of a hedge and every other rule of the pick are unchanged. Under `off` the walk surplus is 0 for every escrow and the order is today's.

Unserved nonce: 4.3. The target does not move when a host passes or fails, so a host that cannot carry its share keeps being offered it and keeps answering probes or taking misses for the part it does not carry.

### 5.3 `balance`

Target: the weight share scaled by the part of its offers the host served.

- `serve_ratio(h) = served / (served + failed + passed)` over the decayed counts. It is 1 while the denominator is below the model's `perf_failure_rate_min_volume` (20). `excused` and in-flight nonces are in neither term.
- `capacity_factor(h) = clamp(serve_ratio(h), 0.25, 1)`.
- `target(h) = weight_share(h) x capacity_factor(h) / sum over k of weight_share(k) x capacity_factor(k)`.

Selection and the unserved nonce are as under `weight`, with this target.

How the window enters. The window is what turns an offer into `served` or `passed`: an offer the window admits can be served, an offer it refuses is a pass. Its size cannot be the input, because it never falls below the weight floor (2.3) and so reads the same for every host that carries less than its weight earns. The measured serving capacity is therefore what passed through the window and finished. A host inside its window with no failures has factor 1 and its full weight share.

Fixed point. A host with entitlement `c` nonces and capacity `K` below it is offered `x` with `x = c x K / x`, so `x = sqrt(c x K)`: the geometric mean of what its weight entitles it to and what it serves. It is offered less than its weight share and more than it serves, and the difference is the probing that lets the factor recover. The slope of the feedback at the fixed point is -1 and the factor is a 600 s average, so (inferred) the loop settles without a standing oscillation; the harness asserts it.

Cadence. Counters move on every bind and every outcome. Shares, factors and targets are recomputed once a second per model and when the epoch, the mode or the number of the model's accepting escrows changes, and are published as one table that a pick reads whole. Everything resets at the epoch boundary.

Bounds. The factor is in [0.25, 1], so a host is always offered enough to recover. After normalisation a target is between 0.25 and 4 times the weight share. The surplus is clamped to one window, so a host returning from a cut-off catches up by at most one window. The share is only the second key of the rank, so it never buys an extra pass.

### 5.4 Who pays

| Nonce bound to host H | `off` | `weight` and `balance` |
|---|---|---|
| H busy, answers the probe | ghost: payer pays floor and fee; H paid floor, credited an inference for no work | payer pays at most floor and fee; H paid for a one-token inference it ran |
| H busy or broken, does not answer | same ghost | H takes a miss; floor refunded; payer pays the fee of the pass nonce and of the vote nonce |
| H above its target | escrow advances anyway, H credited | escrow advances less; H is assigned, paid and credited fewer nonces |
| H serves less than it is offered | no effect on its share | `weight`: none. `balance`: target falls toward `sqrt(c x K)` |
| Escrow at its bound of unresolved passes | not applicable | no nonce spent; the request takes another escrow or a 503 |
| Vote still queued at settlement | not applicable | settles as a ghost, counted `pass_unvoted` |

The vote nonce is the new cost to the payer: one fee per unanswered pass.

## 6. Burst and sustained, per layer

`off` is today at every layer. For `weight` and `balance`:

| Layer | Burst | Sustained |
|---|---|---|
| Gateway admission and its queue ([`limits/gateway.go:308`](../../devshard/cmd/gateway/limits/gateway.go#L308)) | unchanged: a probe is the gateway's own request and takes no admission slot | unchanged |
| Escrow pick | one extra key; a pick reads one published table | escrows heavy in a host above target advance less; with every escrow at its pass bound the answer is a 503 with no nonce spent |
| Dispatcher drain | a pass counts as a burn toward the forced send, the request cap and the drain's burn budget | unchanged caps |
| Host window | a probe takes no tokens; real sends are admitted as today | `balance` lowers the offers to a host whose window keeps refusing; `weight` does not |
| Cut-off and ejection | no probe is sent into an open cut-off; the pass is an owed vote | a dead host turns each crossing of its slots into an owed vote, bounded by 100 per escrow |
| Host chat budget | a probe is one chat; a spent budget means an owed vote | the probe cap per host bounds the chats |
| Votes | paced at 10 rounds a second, burst 20 | throughput past a dead host is bounded by the vote pace: with a slot share `s` dead, (inferred) about `10 x (1 - s) / s` requests a second per gateway, 40 at `s = 0.2` |
| Nonce cap | each unanswered pass spends two nonces | the per-escrow bound keeps the votes under the cap |

## 7. Shadow members, proof-of-compute, challenges

- Shadow members are outside the share ledger and have no target: they hold no weight share, a walk steps over their slots without counting them, and their nonces are probed or held exactly as today ([`scheduler/match.go:108-110`](../../devshard/cmd/gateway/scheduler/match.go#L108-L110), [`scheduler/shadow.go:22-35`](../../devshard/cmd/gateway/scheduler/shadow.go#L22-L35)). No mode ghosts or skips a shadow nonce. The three host-state burns of a shadow host that exist today (absent, proof-of-compute, diverged; [`scheduler/match.go:101-107`](../../devshard/cmd/gateway/scheduler/match.go#L101-L107), [`scheduler/match.go:123-128`](../../devshard/cmd/gateway/scheduler/match.go#L123-L128)) are not changed by this setting.
- Proof-of-compute: a host that owes it is blocked by the chain's rule, not by the gateway's judgement. Its nonce stays a burn in every mode and is counted `excused`, so it does not lower the host's factor. Its weight is the preserved part (4.1), so targets are shared among the hosts that are serving.
- Epoch switch: the ledger and the targets reset with the host state. A host with no weight in the new snapshot has target 0.
- Challenges: the chain leaves a member under challenge out of the slots of new escrows and waives its devshard misses at settlement ([`inference-chain/x/inference/keeper/msg_server_create_devshard_escrow.go:50-55`](../../inference-chain/x/inference/keeper/msg_server_create_devshard_escrow.go#L50-L55), [`inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go:227-228`](../../inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go#L227-L228)). The gateway reads no challenge state for participants at this commit. Such a host is treated as any other in the escrows it still holds slots in, and a vote owed on it costs the payer a fee and yields no miss. Skipping those votes needs the challenge state in the chain snapshot.

## 8. Migration

The default is `off`, and with `off` every decision is the one made today; the ledger is the only addition and it routes nothing. Suggested order: ship with `off` and read the ledger for one epoch; set `models.<id>.host_load_mode` to `weight` on one model; compare target against actual and the payer's cost per served request; then choose `weight` or `balance` per model or globally. Going back is one admin write, and a build that does not know the key ignores a stored value (3.3). One gateway version talks to one host version, so there is no compatibility path, and nothing changes on the wire or on chain.

## 9. Metrics and tests

Accounting report: a `share` object on the participant record beside `capability` ([`accounting/types.go:101-119`](../../devshard/cmd/gateway/accounting/types.go#L101-L119), [`accounting/capability.go:5-20`](../../devshard/cmd/gateway/accounting/capability.go#L5-L20)) with `host_load_mode`, `weight`, `weight_share`, `slot_share`, `target_share`, `offered_share` (the actual), `surplus`, `serve_ratio`, `capacity_factor`, and the counts `assigned`, `offered`, `served`, `failed`, `passed`, `excused`, `probes_answered`, `probes_missed`, `votes_owed`, `pass_unvoted`. Prometheus, in the naming of the participant metrics ([`metrics/race.go:91-152`](../../devshard/cmd/gateway/metrics/race.go#L91-L152)): `devshard_gateway_participant_target_share`, `devshard_gateway_participant_offered_share` and `devshard_gateway_participant_passes_total{outcome}` by participant and model, and `devshard_gateway_host_load_mode` by model.

Unit tests, each on a fixture where every field is non-zero (weights, slots, windows, in flight, and all six ledger counts for every host), so that a formula that drops a term fails:

- Config: the key round-trips globally and per model; each layer wins in the order of 3.2; `null` clears; a bad value, an empty string and a misspelt key are refused with the texts of 3.3; a stored unknown key is tolerated.
- `off`: the rank and every decision equal today's on the same fixture; the ledger still counts.
- `weight`: targets equal weight shares; among equal passes the rank takes the walk with the lowest surplus; a full window, an open cut-off and an ejection each yield a pass and no ghost; proof-of-compute still burns and is `excused`; a shadow slot is neither counted nor targeted; the forecast equals the passes the drain makes; the escrow bound makes a walk unusable.
- `balance`: the factor for served, failed and passed mixes, the floor 0.25, the minimum volume, normalisation to 1, the 0.25 to 4 bounds, and convergence to `sqrt(c x K)` on a stepped clock without oscillation.

Load harness in `devshard/testenv/loadtest`: one scenario, run once per mode. Hosts A and B with weights 58,036 and 53,493, 32 escrows of 16 slots with 104 slots for A and 66 for B, A fast with a fixed concurrency and B slow to its first token (node profiles bound to a host, [`devshard/testenv/loadtest/scenario.go:169-180`](../../devshard/testenv/loadtest/scenario.go#L169-L180)). The harness cannot build this today: the mock chain gives every participant one weight ([`devshard/testenv/mockchain/adminface/phase.go:66-81`](../../devshard/testenv/mockchain/adminface/phase.go#L66-L81)) and every new escrow the template's slots ([`devshard/testenv/mockchain/txexec/exec.go:117-131`](../../devshard/testenv/mockchain/txexec/exec.go#L117-L131)), and the scenario schema has no field for either ([`devshard/testenv/loadtest/scenario.go:127-180`](../../devshard/testenv/loadtest/scenario.go#L127-L180)). It needs a per-participant weight and a per-escrow slot layout first. Assertions extend the existing ones ([`devshard/testenv/loadtest/scenario.go:259-262`](../../devshard/testenv/loadtest/scenario.go#L259-L262), [`devshard/testenv/loadtest/scenario.go:289-294`](../../devshard/testenv/loadtest/scenario.go#L289-L294)): under `off`, more than half of A's nonces are ghosts and real sends of A and B are within 10%; under `weight` and `balance`, no ghost with reason window full, cut-off or ejected, `offered_share` within 10% of `target_share` for A and B, `pass_unvoted` near zero, payer cost per served request not above `off` plus vote fees, and p50 and p95 time to first token not above `off`.

## 10. What each mode does to A and B

An illustration under stated assumptions, not a forecast: B's offered nonces stay at 24,126 under `weight`, the slot layout lets the targets be reached, each host keeps serving what it serves today (non-ghost nonces as the upper bound of its real sends, delivered as `served`), and normalisation over the other hosts is left out.

| | `off` (measured) | `weight` | `balance` |
|---|---|---|---|
| A offered | 56,635 | about 26,200 (1.085 x B) | about 20,400 (`sqrt(26,177 x 15,849)`) |
| A not sent | 36,820 ghosts | about 6,400 passes, no ghost | about 600 passes, no ghost |
| A inferences on chain | 56,066 | offered minus misses, about half of today | offered minus misses, about 36% of today |
| A missed | 569 | 569 plus its unanswered probes | 569 plus its unanswered probes |
| B offered | 24,126 | 24,126 | about 18,200 (`sqrt(24,126 x 13,691)`) |
| B not sent | 4,658 ghosts | 4,658 passes, no ghost | about 0 |
| A : B offered | 2.35 | 1.085 | about 1.12 |

Under `weight` the two hosts are assigned nonces in the ratio of their weights, and the part A does not carry becomes probes it must answer or misses. Under `balance` both give up share to hosts that serve everything they are offered: A because its window refuses, B because its sends fail. B's serve ratio today is 0.65 and A's is 0.30, and A's rises once it is no longer offered its slot surplus.

## 11. Alternatives rejected

- Keep the ghost and post a refused vote on each. Same miss, but every pass costs a vote round across the group instead of one send.
- Send the real request over the full window. The cost lands on the user and the payer, and a slow host is paid more than under a ghost.
- Park the escrow until the host has room. Nonces are sequential, so every escrow stalls behind its slowest host.
- Narrow the window below its weight floor. Under today's ladder a narrower window is more burns, which pays the host more.
- Use the window size as the capacity input of `balance`. It never falls below the weight floor (2.3).
- Three boolean switches. The three behaviours are not independent: a target without the pass rule still pays ghosts, and the pass rule without a target keeps the slot surplus.

## 12. Limits and adjacent leads

- A host that answers slowly but answers takes no miss from the gateway; that needs a shorter execution timeout, a governance parameter.
- The chain counts `assigned - missed` as completed ([`inference-chain/x/inference/keeper/devshard_host_stats.go:95`](../../inference-chain/x/inference/keeper/devshard_host_stats.go#L95)), so an answered probe and any `pass_unvoted` residue still count as inferences. Counting finished work needs a chain change.
- The pick cannot bring a host's share below its lowest slot share in any one escrow; a cap on one member's slots per group in the sampler would bound that.
- Settlement waives all devshard misses of a participant with an active challenge ([`inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go:227-228`](../../inference-chain/x/inference/keeper/msg_server_settle_devshard_escrow.go#L227-L228), marked TODO).
- A vote nonce is bound to a slot by the same modulo and counts as an assigned nonce of that slot's host.
- `scheduler/ghost.go` calls a burn a "silent ghost probe" ([`scheduler/ghost.go:3`](../../devshard/cmd/gateway/scheduler/ghost.go#L3)); nothing is sent.
