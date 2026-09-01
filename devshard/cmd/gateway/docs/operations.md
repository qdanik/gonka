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
3. **Admin overrides** — 34 fields (`config.Overrides`), written through `PUT /v1/admin/settings`, persisted in the store and reloaded at boot. These take effect without a restart: the config is an immutable snapshot swapped whole, and every reader loads it per request.

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
| `GATEWAY_ENGINE_FIRST_TOKEN_FLOOR_MS` | 1 000 | lower bound on the first-token curve |
| `GATEWAY_ENGINE_FIRST_TOKEN_CEILING_MS` | 30 000 | upper bound, whatever the host's own p75 asks for |
| `GATEWAY_ENGINE_INTER_CHUNK_STALL_MS` | 30 000 | silence after first content before an attempt is stalled |
| `GATEWAY_ENGINE_LOSER_GRACE_MS` | 600 000 | how long a loser may keep running after the crown |
| `GATEWAY_NONCE_ACCOUNTING_ENABLED` | false | the per-nonce ledger and its own listener |
| `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` | 600 | how fast a host's history forgets |
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

`lifecycle.go`, `shutdownOrder`. Nine steps, in this order, bounded as a whole by the grace period:

| # | Step | Why here |
| --- | --- | --- |
| 1 | http server | stop taking new work first |
| 2 | races | in-flight requests finish, and the losers pay the votes they owe |
| 3 | dispatchers | nothing new reaches a host |
| 4 | escrow lifecycle | no rotation starts mid-drain |
| 5 | chain observer | nothing above still needs a snapshot |
| 6 | escrow sessions | **destroys state** the steps above may still use |
| 7 | nonce accounting | after every emitter, so the final snapshot holds the counters the run ended with |
| 8 | store | every step above may still write to it |
| 9 | public API connections | every step above can still reach it; closing earlier just forces a re-dial |

`stopAll` runs every step **even after one fails**, except a step marked `needsQuiesced` (step 6): if anything above it failed, work may still be running, so closing the sessions would pull storage out from under it. That step is skipped and the skip is reported.

Each drain is bounded by the grace period but **not cancelled** by it — a step that runs out of time is reported as "abandoned with work still running" rather than killed mid-vote.

## Logs

The gateway writes a line for every event that **moves money, changes what it will serve, or is an operator's own doing** — and for very little else. Failures on the money path are not logged separately: each is returned as an error naming its own step (`resolving signer for escrow X`, `building settlement for escrow X`) and the escrow tick logs the joined result once. A success has no such carrier, which is why the successful transitions are the ones written down.

### The trace

Always on, with no level knob — a trace that ships off by default is not there for the incident that already happened, and switching it on afterwards cannot recover what was not written. Roughly five lines per request.

| Line | Carries |
| --- | --- |
| `nonce committed` (`engine/pick.go`) | request, escrow, nonce, participant, slot, role, and why this attempt started |
| `attempt finished` (`engine/report.go`) | the same identity, the terminal verdict, whether the nonce was finished, whether the host diverged on state |
| `nonce stranded` (`engine/race.go`) | **Warn** — a committed nonce nobody will answer for; the shape every recurring settlement defect takes |

Follow one request by grepping its request id; follow one nonce through commit, dispatch and verdict by grepping the nonce.

`attempt finished` carries the terminal **the attempt itself reported**. A goroutine sees only its own cancellation, so the coordinator reclassifies at the end of the race — an attempt that outlived the backstop becomes `hard_timeout`, a host that went silent mid-stream becomes `stalled` — while the line still reads `client_cancelled`, because that is what the attempt saw.

### The request record

`request finished` (`api/finish.go`), one line per completed race: Info when it went out clean, Warn when it did not. It answers what a finished request can no longer be asked:

| Field | What it settles |
| --- | --- |
| `bytes` | how much actually reached the client, counted at the socket — the strip rewrites events on the way out |
| `terminated` | whether the SSE terminator went with them; without it a client waits out its own timeout on a reply it already has |
| `outcome` | `served`, `failed_mid_stream`, or `failed_before_first_byte` — the last distinguishes a reply the client can retry from one it cannot |
| `deliver_error` | the failure that reached the client instead of the last bytes: the case no status code can express, because a stream commits 200 on its first byte |

The record carries no request or response body — capture files exist for that, sampled and bounded. Its `error` field is truncated at 256 bytes, and that is not tidiness: a host error with no message renders its raw upstream payload as the error text, so an untruncated field would write a whole SSE event, generated tokens included, once per failed request.

### Lines that mean something happened

| Line | Why it matters |
| --- | --- |
| `escrow created` / `escrow settled` | the moments funds were committed and left; the settle line is the audit record |
| `settled escrow record dropped` | that row named the only key able to settle the escrow — removal is irreversible |
| `escrow recovered from commitment` | a create landed while the gateway was down, so `escrow created` never ran; the escrow exists in no other line |
| `commitment cleared` | a creation intent was abandoned, with the reason — one of which (`transaction created no escrow`) means the transaction *did* commit |
| `escrow gone from chain, taken out of service` | `escrow retired` also fires for settlement parking, so this is the only line carrying the cause |
| `escrow depleted with no replacement configured` | capacity left the fleet and nothing replaces it |
| `nonce burned for nobody` (`observers.go`) | a committed nonce that will serve nobody, with the escrow and the reason |
| `escrow stopped burning nonces at its budget` | the escrow now queues callers rather than spending on requests it cannot serve |
| `host blocked for state divergence` (`engine/report.go`) | the block does not lift while the process runs and no metric exposes it — "why is this host never picked" is answerable only here |
| `chain snapshot stale` / `chain snapshot recovered` | written on the **edge** only; a failed refresh keeps routing on the previous participants until the last poll that read the epoch and the participants passes `chain_snapshot_max_age_seconds`, after which requests are refused 503. The nonce-ceiling and preserved-set reads fall back within the poll and do not hold that clock back |
| `admin request failed` / `admin request refused` (`api/errors.go`) | the operator mutation lines are written on the successful path only, so a failed operator action would otherwise be invisible |

Admin lines carry the action and its subject, **never the request body** — an override payload can hold the admin key. An unkeyed call on an operator route is refused 401 and written down: that is the shape an intrusion attempt takes.

## Metrics

`/metrics`, Prometheus, ~75 series, plus the `devshard_gateway_nonces_*` family when the nonce ledger is on (see [accounting.md](./accounting.md)). Grouped by the question they answer:

| Question | Series |
| --- | --- |
| is the gateway serving | `devshard_gateway_requests_total`, `devshard_http_request_duration_seconds`, `devshard_gateway_user_visible_wins_total` |
| is it hiding failures | `devshard_gateway_critical_user_failures_total`, `devshard_gateway_user_requests_with_hidden_failure_total`, `devshard_gateway_no_winner_attempts_total` |
| is it admitting or refusing | `devshard_gateway_limit_rejections_total`, `devshard_gateway_limiter_queue_depth`, `devshard_gateway_effective_max_concurrent_requests`, `devshard_gateway_inflight_requests` |
| how are the hosts | `devshard_gateway_participant_*` (receipt, first content, inter-chunk, transport errors), `devshard_gateway_host_ejected`, `devshard_gateway_participant_window_size` |
| is money leaking | `devshard_gateway_ghost_nonces_burned_total`, `devshard_gateway_nonce_holds_total`, `devshard_gateway_timeout_actions_total`, `devshard_gateway_burn_budget_exhausted_total` |
| is the chain view healthy | `devshard_gateway_chain_snapshot_healthy`, `devshard_gateway_chain_snapshot_age_seconds`, `devshard_gateway_chain_epoch_phase`, `devshard_gateway_chain_requests_blocked` |
| is memory bounded | `devshard_gateway_buffered_response_bytes`, `devshard_gateway_cache_bytes`, `devshard_gateway_capture_bytes_held` |
| is the ledger keeping up | `devshard_gateway_accounting_rows_written_total`, `devshard_gateway_accounting_rows_lost_total`, `devshard_gateway_accounting_retention_sweeps_failed_total` |

`devshard_gateway_chain_snapshot_healthy` is the one to alert on first: with a stale snapshot every score, weight and preserved-set decision below it is being made on old data.

### Cardinality rules

Route labels are **templated** (`/devshard/{id}/…`), never per-escrow, so cardinality does not grow with the escrow set. `/v1/admin/devshards/import` reports under the `/v1/admin/devshards/{id}` label so it lands in the same panel. Every label value is kept non-empty (`metrics/labels.go`): an empty label silently merges unrelated series, and a status with no recoverable code reports as `statusNoCode` rather than as blank.

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
