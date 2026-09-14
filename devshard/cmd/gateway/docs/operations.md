# Operating the gateway

One binary, one SQLite file, one HTTP port. Everything else — escrows, hosts, epochs — is read from the chain and rebuilt at boot. This document is what an operator needs: what is exposed, what can be changed at runtime, what the process does on the way up and down, and what to look at when it misbehaves.

## What is exposed

Three tiers, and the tier decides both who may call it and whether the kill switch applies:

| Route | Auth | Survives the kill switch |
| --- | --- | --- |
| `/v1/chat/completions` | per-model (below) | no |
| `/v1/models`, `/v1/status` | none — they list what is routable, nothing caller-specific | no |
| `/devshard/{id}/v1/chat/completions`, `.../models`, `.../status` | as above, pinned to one escrow | no |
| `/metrics` | none | yes |
| `/v1/requests/{id}` | admin | yes |
| `/devshard/{id}/v1/finalize`, `.../state`, `.../debug/*` | admin | yes |
| `/v1/admin/*`, `/v1/debug/rotation`, `/v1/debug/memstats` | admin | yes |

The `/devshard/{id}/…` prefix pins a request to one escrow instead of letting the scheduler choose — the recovery surface for an escrow that needs attention on its own.

### Who may call what

`api/routes.go`, `authorizeModel`. Three tiers per model, from `limits.model_access`:

| Tier | Who |
| --- | --- |
| `open` | anyone |
| `api_key` | a caller presenting one of `GATEWAY_API_KEYS` |
| `admin_only` | a caller presenting `GATEWAY_ADMIN_API_KEY` |

**Two behaviours to know before editing it:**

- An **empty** `model_access` map means every model is `open`. A **populated** one makes every model *not listed in it* `admin_only`. Adding your first entry silently closes every other model.
- With `GATEWAY_ADMIN_API_KEY` unset the whole admin surface answers **404, not 401** — the routes do not exist rather than rejecting the caller. A "route not found" on `/v1/admin/…` usually means the key is missing, not the path.

An admin key satisfies every tier, so admin calls never need a second key.

### The kill switch

`limits.disabled` (env `GATEWAY_DISABLED`, or the admin settings endpoint) stops serving clients while leaving `/metrics`, the admin surface and the recovery surface up — so a gateway can be taken out of service and still be inspected, settled and drained.

With `disabled_redirect_url` set it answers **308** with the new URL in both the header and the body; without one, **503** with `disabled_message`. The distinction matters to clients: a 308 is a permanent move, a 503 is "come back later".

## Configuration

Three layers, later wins:

1. **Defaults** — `config/config.go`, `Defaults()`. The only place a default lives.
2. **Environment** — read once at boot, in `env/` and nowhere else. `env.Load` returns *what is set* (a nil pointer is unset), so an unset variable can never overwrite a default with a zero.
3. **Admin overrides** — 42 fields (`config.Overrides`), written through `PUT /v1/admin/settings`, persisted in the store and reloaded at boot. These take effect without a restart: the config is an immutable snapshot swapped whole, and every reader loads it per request.

Parse failures are accumulated, so a boot reports **every** misconfigured variable at once rather than one per restart.

### Variable names

Each `GATEWAY_*` variable falls back to a `DEVSHARD_*` spelling from before the rename (`env/env.go`, `legacyNames`). An **empty** value counts as unset on both, so blanking a legacy variable does not resurrect it through the fallback.

Signing keys are addressed **by the name of the variable that holds them**, never by value: `escrows_json` and `rotation.models_json` carry `private_key_env`. Log lines and errors name the variable, never the key.

### The knobs that decide behaviour

| Variable | Default | What it decides |
| --- | --- | --- |
| `GATEWAY_PORT` | 8080 | the listening port |
| `GATEWAY_STORAGE_DIR` | `$HOME/.cache/gonka-gateway` | where `gateway.db` and the escrow storage live |
| `GATEWAY_MAX_CONCURRENT_REQUESTS` | 1536 | the hard admission ceiling; unset lets the weight model decide |
| `GATEWAY_ADMISSION_QUEUE_WAIT_MS` | 300000 | how long a request waits for a slot before 429 |
| `GATEWAY_ADMISSION_QUEUE_PER_SLOT` | 4 | how deep the queue is allowed to grow per slot |
| `GATEWAY_MAX_BUFFERED_RESPONSE_BYTES` | 512 MiB | **every** non-streaming reply being assembled, at once |
| `GATEWAY_CHAT_CACHE_MAX_BYTES` | 256 MiB | the response cache |
| `GATEWAY_DEFAULT_MAX_TOKENS` / `GATEWAY_MAX_TOKENS_CAP` | from `filters` | the output budget a request gets and may ask for |
| `GATEWAY_ROTATION_ENABLED` | false | whether the epoch bridge creates and retires escrows |
| `GATEWAY_ROTATION_SETTLEMENT_ENABLED` | false | whether retirement settles or only parks |
| `GATEWAY_ROTATION_PRE_POC_BLOCKS` | 300 | how early the bridge starts |
| `GATEWAY_WARM_NEW_ESCROWS` | true | whether a new escrow is taught to its group before serving |
| `GATEWAY_CHAIN_SNAPSHOT_MAX_AGE_SECONDS` | 60 | how stale the chain snapshot may be before requests are refused 503; `0` disables the gate |
| `GATEWAY_ENGINE_RECEIPT_TIMEOUT_MS` | 5 000 | receipt deadline; doubled above 100 000 input tokens |
| `GATEWAY_ENGINE_FIRST_TOKEN_FLOOR_MS` | 12 000 | lower bound on the first-token curve |
| `GATEWAY_ENGINE_FIRST_TOKEN_CEILING_MS` | 30 000 | upper bound, whatever the host's own p75 asks for |
| `GATEWAY_ENGINE_INTER_CHUNK_STALL_MS` | 30 000 | silence after first content before an attempt is stalled |
| `GATEWAY_ENGINE_LOSER_GRACE_MS` | 600 000 | how long a loser may keep running after the crown |
| `GATEWAY_NONCE_ACCOUNTING_ENABLED` | false | the per-nonce ledger and its own listener |
| `GATEWAY_NONCE_ACCOUNTING_RETENTION_EPOCHS` | 2 | how many epochs before the current one the nonce ledger keeps retired escrows; below 1 is refused while the ledger is on |
| `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` | 600 | how fast a host's history forgets |
| `GATEWAY_TIMEOUT_SWEEP_BUDGET_PER_TICK` | 8 | execution-timeout votes one tick may retry across every escrow; `0` turns the sweep off |
| `GATEWAY_TIMEOUT_SWEEP_GRACE_SECONDS` | 120 | how far past its deadline a nonce must be before the sweep claims it from its own race |
| `GATEWAY_POC_MODE` | off | `relaxed` keeps serving through proof-of-compute |

The full list is `env/env.go`; the full set of defaults is `config.Defaults()`. Neither is duplicated here — a table that drifts is worse than a pointer that does not.

## Boot

`lifecycle.go`, `serve`. The order is load-bearing:

1. the chain observer starts — nothing downstream can score a host before a snapshot exists;
2. the warmup prober starts;
3. `seedDevshards` applies `escrows_json` to the store — a seeded escrow that names no key variable is **refused**, not silently accepted;
4. `publishEscrows` opens a session per active escrow and publishes it for routing, bounded by `MaxConcurrentRuntimeBuilds` (16) so a large set does not open 200 sessions at once;
5. the escrow lifecycle manager starts its 15 s tick;
6. the store's write notifications start republishing escrows on change;
7. the nonce ledger starts;
8. the HTTP listener opens — **last**, so the first request meets a gateway that is fully assembled.

A failure in steps 3 or 4 shuts down cleanly rather than serving half-built.

## Shutdown

`lifecycle.go`, `shutdownOrder`. Ten steps, in this order, bounded by the grace period, with up to one more second from the journal step's floor:

| # | Step | Why here |
| --- | --- | --- |
| 1 | http server | stop taking new work first |
| 2 | races | in-flight requests finish, and the losers pay the votes they owe |
| 3 | dispatchers | nothing new reaches a host |
| 4 | escrow lifecycle | no rotation starts mid-drain |
| 5 | chain observer | nothing above still needs a snapshot |
| 6 | escrow sessions | **destroys state** the steps above may still use |
| 7 | journal | every producer above has stopped; it drains its queue into the ledger below, waiting for whatever budget remains and up to one more second when a drain above has spent it, and counts anything later |
| 8 | nonce accounting | after every emitter, so the final snapshot holds the counters the run ended with |
| 9 | store | every step above may still write to it |
| 10 | public API connections | every step above can still reach it; closing earlier just forces a re-dial |

`stopAll` runs every step **even after one fails**, except a step marked `needsQuiesced` (step 6): if anything above it failed, work may still be running, so closing the sessions would pull storage out from under it. That step is skipped and the skip is reported.

Each drain is bounded by the grace period but **not cancelled** by it — a step that runs out of time is reported as "abandoned with work still running" rather than killed mid-vote.

The `journal` step (7) is bounded the same way, but with a floor: it waits for its queue for whatever budget remains, and for up to one more second when a drain above it has spent the whole grace period, so its queue can still reach the ledger, and a close that outlasts both is reported as "abandoned with events still queued".

## Logs

The gateway writes a line for every event that **moves money, changes what it will serve, or is an operator's own doing** — and for very little else. Failures on the money path are not logged separately: each is returned as an error naming its own step (`resolving signer for escrow X`, `building settlement for escrow X`) and the escrow tick logs the joined result once. A success has no such carrier, which is why the successful transitions are the ones written down.

### The trace

Always on, with no level knob — a trace that ships off by default is not there for the incident that already happened, and switching it on afterwards cannot recover what was not written. Roughly five lines per request.

| Line | Carries |
| --- | --- |
| `nonce committed` (emitted by `engine/pick.go`, written by `journal/render_race.go`) | request, escrow, nonce, participant, slot, role, and why this attempt started |
| `attempt finished` (emitted by `engine/report.go`, written by `journal/render_race.go`) | the same identity, the terminal verdict, whether the nonce was finished, whether the host diverged on state |
| `nonce stranded` (emitted by `engine/race.go`, written by `journal/render_race.go`) | **Warn** — a committed nonce nobody will answer for; the shape every recurring settlement defect takes |

The journal's consumer writes these lines, so under load, or while the nonce ledger copies itself for a snapshot, it can drop `nonce committed` and `attempt finished` — counted in `devshard_gateway_journal_progress_dropped_total` and announced by `journal skipped progress lines` — while `nonce stranded` is refused only past the money ceiling.

A line the journal writes is stamped when the journal's consumer writes it, not when the event happened, so it can trail — for example, while the consumer waits for the nonce ledger's lock as `Book.Snapshot` copies the ledger. Journal lines keep their order among themselves. Lines written directly — the shared `devshard/user` session lines and the few lifecycle lines not yet routed through the journal — are stamped when they happen, so the two can interleave out of order. `duration_ms` and the other `*_ms` fields are measured at the event, and are the timings to trust.

Follow one request by grepping its request id; follow one nonce through commit, dispatch and verdict by grepping the nonce.

`attempt finished` carries the coordinator's reading of the attempt at the moment it completed: `racedTerminal` (`engine/report.go`) makes a backstopped attempt read `hard_timeout`, a stalled one `stalled`, and the winner `won`. Only a later `abandonedByHosts` reclassification, made once the whole race has finished, reaches the ledger without reaching this line.

### When a host stops taking work

Three mechanisms withhold work from a host, each on its own trigger, and each is a gauge in Prometheus. A gauge is sampled every 15 or 30 seconds while the first rung of two of them lasts 30 seconds and 5 seconds, so the shortest withholdings pass entirely between two scrapes. Each therefore also writes one line on the edge, and nothing in between: the volume follows the number of hosts and their own windows, never the request rate.

| Line | Trigger | Carries |
| --- | --- | --- |
| `host withheld from routing` (`perf/tracker.go`) | five failures in a row, or a failure rate from 15% over a volume from 20 | **Warn** — which trigger fired, the rung, the run length, the rate and its volume, and how long the withholding lasts |
| `host back in routing` (`perf/tracker.go`) | first sample after the withholding lapsed | the rung it decayed to |
| `host cut off after transport faults` (`limits/participant.go`) | three transport faults in a row, or one failed half-open probe | **Warn** — which of the two, the backoff depth, and how long the cut-off lasts |
| `host back after its cut-off` (`limits/participant.go`) | the probe answered | the backoff depth it decayed to |
| `host denied the crown` (`engine/engine.go`) | three content-free answers in a row | **Warn** — the strike count. The host keeps drawing nonces and starts a second attempt beside itself, so this is a spend, not only a quality signal |
| `host crowned again` (`engine/engine.go`) | one answer with content | — |

One more line belongs to the same family, on the money side rather than the routing one: `execution timeouts swept` (`escrow/manager.go`), written by the escrow tick only when the sweep found nonces to re-vote, carrying how many were due, applied and failed. Silence means nothing was owed.

### The request record

`request finished` (`journal/render_request.go`), one line per completed race: Info when it went out clean, Warn when it did not. It answers what a finished request can no longer be asked:

| Field | What it settles |
| --- | --- |
| `escrow` | the escrow the race ran on; absent when no escrow was picked |
| `host` / `hosts` | `host` is the crowned attempt's host; with nobody crowned, `hosts` lists every host tried, comma-separated; a race that ran no attempt carries neither |
| `bytes` | how much actually reached the client, counted at the socket — the strip rewrites events on the way out |
| `terminated` | whether the SSE terminator went with them; without it a client waits out its own timeout on a reply it already has |
| `outcome` | `served`, `failed_mid_stream`, or `failed_before_first_byte` — the last distinguishes a reply the client can retry from one it cannot |
| `deliver_error` | the failure that reached the client instead of the last bytes: the case no status code can express, because a stream commits 200 on its first byte |
| `nonce_finished` | whether the crowned attempt's nonce closed on the chain. A served request with `false` here answered the client and left the escrow owing a vote for that nonce, which is the shape [issue #1387](https://github.com/gonka-ai/gonka/issues/1387) reported from the other side: the legacy gateway called the whole request failed for it. Absent when nobody was crowned |

The record carries no request or response body — capture files exist for that, sampled and bounded. Its `error` field is truncated at 256 bytes, and that is not tidiness: a host error with no message renders its raw upstream payload as the error text, so an untruncated field would write a whole SSE event, generated tokens included, once per failed request.

`gateway limiter turned a request away` sits on the journal's progress lane rather than this line's money lane, so it can be skipped when the progress backlog is full — the exact condition a refusal storm creates. A skipped line is counted in `devshard_gateway_journal_progress_dropped_total`; every refusal, logged or not, is still counted in `devshard_gateway_limit_rejections_total`.

### Lines that mean something happened

| Line | Why it matters |
| --- | --- |
| `escrow created` / `escrow settled` | the moments funds were committed and left; the settle line is the audit record |
| `settled escrow record dropped` | that row named the only key able to settle the escrow — removal is irreversible |
| `escrow recovered from commitment` | a create landed while the gateway was down, so `escrow created` never ran; the escrow exists in no other line |
| `commitment cleared` | a creation intent was abandoned, with the reason — one of which (`transaction created no escrow`) means the transaction *did* commit |
| `escrow gone from chain, taken out of service` | `escrow retired` also fires for settlement parking, so this is the only line carrying the cause |
| `escrow depleted with no replacement configured` | capacity left the fleet and nothing replaces it |
| `nonce burned for nobody` (`journal/render_money.go`) | a committed nonce that will serve nobody, with the escrow and the reason, and under `burned_during_request` the request it was spent during |
| `a host stopped mid-answer: reply served, not cached` (`journal/render_request.go`) | the reply reached the client whole and never reached a terminal `finish_reason`, so nothing replays it. The gateway writes the SSE terminator itself, so nothing else names a truncated answer — but only a reply the cache would otherwise have stored gets here: with `chat_cache_max_bytes` at 0, for a body past the per-entry bound, or when the client had already left, a truncated answer still passes unnamed |
| `escrow stopped burning nonces at its budget` | the escrow now queues callers rather than spending on requests it cannot serve |
| `host blocked for state divergence` (`journal/render_race.go`) | the block does not lift while the process runs and no metric exposes it — "why is this host never picked" is answerable only here |
| `chain snapshot stale` / `chain snapshot recovered` | written on the **edge** only; a failed refresh keeps routing on the previous participants until the last poll that read the epoch and the participants passes `chain_snapshot_max_age_seconds`, after which requests are refused 503. The nonce-ceiling and preserved-set reads fall back within the poll and do not hold that clock back |
| `admin request failed` / `admin request refused` (`api/errors.go`) | the operator mutation lines are written on the successful path only, so a failed operator action would otherwise be invisible |

Admin lines carry the action and its subject, **never the request body** — an override payload can hold the admin key. An unkeyed call on an operator route is refused 401 and written down: that is the shape an intrusion attempt takes.

## Metrics

`/metrics`, Prometheus: 74 gateway families beside the Go runtime and process collectors, and nine more when the nonce ledger is on — the `devshard_gateway_nonces_*` gauges, `devshard_gateway_nonce_facts_rejected_total` and `devshard_gateway_nonce_finding` (see [accounting.md](./accounting.md)). Grouped by the question they answer:

| Question | Series |
| --- | --- |
| is the gateway serving | `devshard_gateway_requests_total`, `devshard_http_request_duration_seconds`, `devshard_gateway_attempts_terminal_total{visibility="user_visible_winner"}` |
| is it hiding failures | `devshard_gateway_requests_total{outcome="failure"}`, `devshard_gateway_user_requests_with_hidden_failure_total`, `devshard_gateway_attempt_failures_total{visibility="no_winner"}` |
| is it admitting or refusing | `devshard_gateway_limit_rejections_total`, `devshard_gateway_limiter_queue_depth`, and `devshard_gateway_inflight_requests_by_model` against `devshard_gateway_enforced_max_concurrent_requests_by_model`, the cap after overrides and capacity scaling (`devshard_gateway_effective_max_concurrent_requests` is the configured cap before either) |
| how are the hosts | `devshard_gateway_participant_*` (receipt, first content, inter-chunk, transport errors), `devshard_gateway_host_ejected`, `devshard_gateway_participant_window_size` |
| is money leaking | `devshard_gateway_ghost_nonces_burned_total`, `devshard_gateway_nonce_holds_total`, `devshard_gateway_timeout_actions_total`, `devshard_gateway_burn_budget_exhausted_total` |
| is the chain view healthy | `devshard_gateway_chain_snapshot_healthy`, `devshard_gateway_chain_snapshot_age_seconds`, `devshard_gateway_chain_epoch_phase`, `devshard_gateway_chain_requests_blocked` |
| is memory bounded | `devshard_gateway_buffered_response_bytes`, `devshard_gateway_cache_bytes`, `devshard_gateway_capture_bytes_held` |
| is the ledger keeping up | `devshard_gateway_accounting_rows_written_total`, `devshard_gateway_accounting_rows_lost_total`, `devshard_gateway_accounting_retention_sweeps_failed_total` |
| is the journal keeping up | `devshard_gateway_journal_money_refused_total`, `devshard_gateway_journal_progress_dropped_total`, `devshard_gateway_journal_late_events_total` |

`devshard_gateway_chain_snapshot_healthy` is the one to alert on first: with a stale snapshot every score, weight and preserved-set decision below it is being made on old data.

### Cardinality rules

Route labels are **templated** (`/devshard/{id}/…`), never per-escrow, so cardinality does not grow with the escrow set. `/v1/admin/devshards/import` reports under the `/v1/admin/devshards/{id}` label so it lands in the same panel. A recorder keeps every label value it writes non-empty (`metrics/labels.go`, `metricLabel`): an empty label silently merges unrelated series, and a status with no recoverable code reports as `0` (`metrics/race.go`, `statusNoCode`) rather than as blank. A collector passes its source's values through unchanged, so `devshard_gateway_nonces_by_disposition`, for one, carries an empty `ghost_reason`, `timeout_action` or `timeout_reason` on a series those facts do not apply to.

### Metric changes

A dashboard or alert outside this repository may query a family this gateway does not emit; each row names the one to query instead.

| Not emitted | Query instead |
| --- | --- |
| `devshard_gateway_no_winner_attempts_total` | `devshard_gateway_attempt_failures_total{visibility="no_winner"}`; an answer that arrived complete and reached nobody is `devshard_gateway_attempts_terminal_total{visibility="no_winner",outcome="success"}` |
| `devshard_gateway_user_visible_wins_total` | `devshard_gateway_attempts_terminal_total{visibility="user_visible_winner"}` |
| `devshard_gateway_critical_user_failures_total` | `devshard_gateway_requests_total{outcome="failure"}` |
| `devshard_inference_timeouts_total` | `devshard_gateway_timeout_actions_total{action=~"completed\|failed"}` |
| `devshard_gateway_escrow_participant_limited` | `devshard_gateway_escrow_blocked_participants > bool 0` |
| `devshard_gateway_escalation_decisions_total` | `devshard_gateway_attempts_started_total{role="speculative"}` by `reason`; that family carried the race's start plan, never what triggered an escalation |

The gateway emits neither label nor the value below, so a selector that names one matches nothing:

- `devshard_gateway_participant_transport_errors_total` has no `path_kind`: every error it counts is an inference request.
- `devshard_gateway_user_requests_with_hidden_failure_total` has no `severity`: every hidden failure it counts is on a protected request.
- `outcome="due"` on `devshard_gateway_timeout_sweep_total`: `applied` plus `failed` is what a tick found, short of it only on a tick that shutdown cut off mid-round.

Participant-labelled race series — `devshard_gateway_attempts_*`, `devshard_gateway_attempt_failures_total`, `devshard_gateway_timeout_actions_total`, `devshard_gateway_stream_carry_overflow_total` and every `devshard_gateway_participant_*` family except the window and breaker gauges — are deleted once their participant and model go unwritten for `perf_host_staleness_seconds`. A host that returns afterwards starts from fresh counters, which `rate()` reads as a reset.

`devshard_gateway_participant_window_size`, `devshard_gateway_participant_window_inflight` and `devshard_gateway_participant_breaker_state` stop reporting a pair the participant limiter forgot on the same window, and `devshard_gateway_participants_tracked` counts only the pairs it still holds.

## Reading the gateway's state

| To see | Call |
| --- | --- |
| what is routable, and the config in force | `GET /v1/admin/state` |
| the effective settings including overrides | `GET /v1/admin/settings` |
| every escrow row the process owns | `GET /v1/admin/devshards` |
| rotation's last outcome per model | `GET /v1/debug/rotation` |
| one escrow's protocol state | `GET /devshard/{id}/v1/state` |
| what happened to one request's nonces | `GET /v1/requests/{id}` |
| hosts the gateway distrusts | `GET /v1/admin/suspicious-hosts` |
| why one host is or is not taking work | `GET /v1/admin/hosts` |

### What a host answer carries

`GET /v1/admin/hosts` joins the two snapshots that already exist, one row per participant and model. Nothing is computed for it: these are the same values the gauges carry, asked for on demand rather than waited for at the next scrape.

| Field | Where it comes from |
| --- | --- |
| `ejected`, `degraded`, `inflight`, `decode_seconds_per_token` | the performance tracker: whether routing withholds this host, whether it would without the pool-wide cap, and how fast it decodes |
| `window`, `window_inflight`, `cutoff`, `backoff_count`, `available` | the participant limiter: the AIMD window, what is in flight against it, the breaker's state, how deep its backoff has gone, and whether the host would be admitted right now |
| `context_limit`, `version_refusals`, `tool_refusals`, `context_refusals` | what this host's build refused |
| `suspicious` | whether an operator pinned it |

`backoff_count` is the answer to "was this host cut off once or eleven times": it is what makes the next cut-off a minute long instead of five seconds. `degraded` beside a false `ejected` means the pool-wide cap is withholding the withholding, because too many of that model's hosts are failing at once.

## When something is wrong

| Symptom | Look at |
| --- | --- |
| every request 429 | `limiter_queue_depth` and `effective_max_concurrent_requests` — the weight model may have scaled capacity to almost nothing after an epoch switch |
| every request 503 with no model listed | the chain snapshot is stale, or the escrow set is empty — check `chain_snapshot_healthy` and `/v1/admin/devshards` |
| 503 on a healthy-looking gateway | `buffered_response_bytes` at the ceiling: non-streaming replies are holding the whole budget |
| a model returns 403 for everyone | `model_access` was populated and this model was not listed |
| the admin surface 404s | `GATEWAY_ADMIN_API_KEY` is unset |
| burns climbing | `ghost_nonces_burned_total` by reason — see [accounting.md](./accounting.md) |
| shutdown reports "abandoned with work still running" | a host stopped answering and the drain hit the grace period; the votes it owed were not paid |

## Where to change what

| To change | Go to |
| --- | --- |
| a default | `config/config.go`, `Defaults()` |
| which variables are read | `env/env.go` — and nowhere else |
| what an admin may override at runtime | `config.Overrides` |
| a route or its auth tier | `api/routes.go`, `routes()` |
| what shuts down when | `lifecycle.go`, `shutdownOrder` |
| a metric or its labels | `metrics/` |
