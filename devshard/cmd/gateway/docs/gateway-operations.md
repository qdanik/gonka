# Devshard gateway — operations

Everything an operator interacts with: routes, configuration, metrics, and what the process does when it starts, is reconfigured, or is asked to stop.

## Routes

One mux, no wrapper above it. Methods are enforced inside each handler, which sets `Allow` and returns a JSON 405. Wrapping order is metrics → kill switch → admin authentication → handler.

Two properties are attached per route: `admin` (requires the bearer admin key) and `alwaysOn` (exempt from the operator kill switch).

### Client

| Route | Methods | Access | Always on |
|---|---|---|---|
| `GET /v1/models` | GET, HEAD | public | no |
| `POST /v1/chat/completions` | POST | per-model tier | no |
| `GET /v1/status` | GET, HEAD | public | no |
| `GET /metrics` | GET, HEAD | public | **yes** |
| `POST /devshard/{id}/v1/chat/completions` | POST | per-model tier | no |
| `GET /devshard/{id}/v1/models` | GET, HEAD | public | no |
| `GET /devshard/{id}/v1/status` | GET, HEAD | public | no |
| `/` (catch-all) | any | public | yes — 404 JSON |

The per-model tier is `open`, `api_key` or `admin_only`. A pinned chat request whose escrow the gateway no longer routes is refused (404) rather than served from another escrow, so the `X-Devshard-ID` a caller asked for is the one it gets.

### Per-escrow recovery surface

All admin, all always-on. An operator reaches for these precisely when something is wrong, so the kill switch must not be what hides them. They resolve through the *settlement* lookup, so a draining escrow still answers — and a draining escrow is usually the one in trouble.

`GET|POST /devshard/{id}/v1/finalize`, `GET /devshard/{id}/v1/state`, `GET /devshard/{id}/v1/debug/state`, `GET /devshard/{id}/v1/debug/inferences`, `GET /devshard/{id}/v1/debug/pending`, `GET /devshard/{id}/v1/debug/signatures`.

### Operator

All admin, all always-on.

| Route | Methods | Purpose |
|---|---|---|
| `/v1/requests/{id}` | GET, HEAD | One request's accounting row. Admin-only because the row names the escrow and the participant and carries no caller to authorise against. |
| `/v1/admin/state` | GET | Gateway state overview. |
| `/v1/admin/settings` | GET, PUT, POST | Read and replace admin overrides; validated, persisted, then applied. |
| `/v1/admin/devshards` | GET, POST | List and register escrows. |
| `/v1/admin/devshards/import` | POST | Import an existing escrow, copying its session storage. |
| `/v1/admin/devshards/{id}` | DELETE | Remove an escrow record. |
| `/v1/admin/devshards/{id}/activate` | POST | Resume routing to it. |
| `/v1/admin/devshards/{id}/deactivate` | POST | Stop routing to it. |
| `/v1/admin/devshards/{id}/settle` | POST | Settle it now. 409 while it is still busy. |
| `/v1/admin/devshards/{id}/participants` | GET | Its participant group. |
| `/v1/admin/escrows` | POST | Create an escrow on chain. |
| `/v1/admin/suspicious-hosts` | GET, POST, DELETE | The manual never-trust list. |
| `/v1/admin/participants/unquarantine` | POST | Clear breaker state for one participant. |
| `/v1/admin/accounting/reset/{epoch}` | POST | Drop the nonce ledger's records for one epoch. |
| `/v1/debug/perf` | GET | Tracked hosts as the picker sees them, beside the escalation settings they are judged against. |
| `/v1/debug/rotation` | GET | Rotation status per model and role. |
| `/v1/debug/memstats` | GET, HEAD | Go memory statistics. |

Four operator-facing behaviours the route table does not show:

- **A private key is never accepted in a request body.** Escrow creation, registration and import all take `private_key_env`, the *name* of the environment variable holding the key; there is no `private_key` field on any of the three request types. A body that omits the variable name is rejected with 400, because a key sent instead would otherwise reach the commitment row, the logs, and every operator in between.
- **Deactivate and settle stop routing before the record changes**, so nothing new is admitted while the write runs, and a settle that fails leaves the escrow retired rather than re-added — its record is already inactive and settlement-pending, which is where the rotation tick picks it up again.
- **Deleting an escrow deletes its session storage**, on a path derived from a client-supplied escrow id. Sessions and the delete route must derive that path the same way or a delete removes a directory nothing was using, and the delete refuses any path that escapes the gateway's own storage directory or is the base directory itself. The `escrow-` filename prefix already neutralises a simple `../`, but an id containing a separator does escape without the guard.
- **Import copies session storage rather than referencing it**, so the gateway owns the only handle to what it serves.

### The kill switch

`GATEWAY_DISABLED` (or the `disabled` override) turns off client traffic. With a redirect URL configured the response is a 308 carrying `Location`; otherwise a 503 with the configured message. `/metrics` and every operator route stay reachable — monitoring and remediation are what a turned-off gateway still needs.

When no admin key is configured, the admin routes answer **404, not 401**: the surface is invisible rather than merely locked. A configured key must be at least sixteen characters, checked at start-up, because a key short enough to guess is worse than none — none disables admin entirely, while a weak one authenticates.

## Configuration

Three layers, later wins: **defaults ← environment ← admin overrides**. The result is validated fail-fast; a bad configuration refuses to start, and validation reports every problem at once using the snake_case names the admin API uses.

A configuration snapshot is immutable. Reconfiguration builds and validates a *new* snapshot, persists the overrides, then swaps an atomic pointer. Readers load the pointer on each use, so they pick up a change on their next call.

### Environment variables

Deployment identity only. `Load` returns what is *set*; defaults live in `config.Defaults()`, never in the environment layer, and every parse failure is accumulated so an operator sees all of them at once. An unset variable takes the default; an empty one is treated as unset.

A name the gateway does not read is ignored in silence, so a typo costs a debugging session — the table below is the whole list it reads.

| variable | what it controls |
|---|---|
| `GATEWAY_PORT` | the port the HTTP server listens on |
| `GATEWAY_STORAGE_DIR` | where the SQLite state, ledger and per-escrow session files live |
| `GATEWAY_API_KEYS` | keys that may call the chat routes |
| `GATEWAY_ADMIN_API_KEY` | key that may call `/v1/admin/*`; absent disables the admin surface |
| `GATEWAY_ESCROWS_JSON` | escrows to serve at startup; empty starts with none and they are added through the admin API. |
| `GATEWAY_CHAIN_GRPC` | chain gRPC endpoint — every chain read and every broadcast |
| `GATEWAY_CHAIN_RPC` | CometBFT RPC endpoint |
| `GATEWAY_CHAIN_ID` | chain id signatures are bound to; a mismatch invalidates every transaction |
| `GATEWAY_PUBLIC_API` | the network's public API, used to resolve host addresses |
| `GATEWAY_TX_FEE_AMOUNT` | fee attached to a chain transaction |
| `GATEWAY_TX_GAS_LIMIT` | gas limit for a chain transaction |
| `GATEWAY_DEFAULT_MAX_TOKENS` | output budget given to a request that names none |
| `GATEWAY_MAX_TOKENS_CAP` | ceiling a client's own budget is clamped to |
| `GATEWAY_FORCE_UPSTREAM_STREAMING` | whether the gateway asks the host to stream whatever the client asked for. On by default: streaming is what lets it time the first token, spot a stall mid-answer and receipt the nonce early. Off sends the client's own request, which is the rollback lever if forcing turns out to break a host |
| `GATEWAY_MAX_CONCURRENT_REQUESTS` | gateway-wide concurrency cap before capacity scaling |
| `GATEWAY_ADMISSION_QUEUE_WAIT_MS` | how long a request may wait for capacity before it is refused |
| `GATEWAY_MATCH_WAIT_MS` | how long a bound nonce waits for a request that has not excluded its participant, before it is burned |
| `GATEWAY_WARM_NEW_ESCROWS` | whether a newly published escrow spends one nonce to teach itself to its whole group |
| `GATEWAY_LOG_FORMAT` | `json` makes every stage line a JSON object a collector reads as labels; anything else keeps the text form |
| `GATEWAY_CHARGE_REFUSED_NONCES` | whether a nonce burned on a host that refused it earns that host a refusal vote. Off by default: it changes what the network bills for, so it is turned on deliberately |
| `GATEWAY_ADMISSION_QUEUE_PER_SLOT` | how deep the queue may grow per slot before arrivals are refused on sight |
| `GATEWAY_POC_MODE` | whether proof-of-compute blocking is honoured or bypassed |
| `GATEWAY_DISABLED` | takes the shard out of service without stopping it |
| `GATEWAY_DISABLED_MESSAGE` | what a client is told while disabled |
| `GATEWAY_DISABLED_REDIRECT_URL` | where a client is pointed while disabled |
| `GATEWAY_ROTATION_ENABLED` | whether spent escrows are replaced automatically |
| `GATEWAY_ROTATION_PRE_POC_BLOCKS` | how many blocks before an epoch switch the bridge escrows are prepared |
| `GATEWAY_ROTATION_SETTLEMENT_ENABLED` | whether a replaced escrow is also settled |
| `GATEWAY_ROTATION_MODELS_JSON` | per-model rotation targets |
| `GATEWAY_CHAT_CACHE_MAX_BYTES` | memory the response cache may hold |
| `GATEWAY_ACCOUNTING_RETENTION_HOURS` | how long request records are kept |
| `GATEWAY_ACCOUNTING_RETENTION_MAX_ROWS` | how many request records are kept |
| `GATEWAY_NONCE_ACCOUNTING_ENABLED` | whether the per-nonce ledger runs and exports its metrics |
| `GATEWAY_NONCE_ACCOUNTING_LISTEN_ADDR` | port for the ledger's private JSON API; empty serves none |
| `GATEWAY_NONCE_ACCOUNTING_RETENTION_EPOCHS` | how far back retired escrows are kept, counted from the current epoch: 2 keeps the current one and the two before it; 0 keeps every one |
| `GATEWAY_NONCE_ACCOUNTING_SNAPSHOT_SECONDS` | how often the ledger is written to disk |
| `GATEWAY_CAPTURE_ENABLED` | whether request/response capture is on |
| `GATEWAY_CAPTURE_DIR` | where captures are written |
| `GATEWAY_CAPTURE_SAMPLE_RATE` | fraction of requests captured |
| `GATEWAY_CAPTURE_MAX_BYTES` | disk the capture may use |
| `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` | how fast a host's measured history decays |

Plus the per-escrow signing keys, read from arbitrarily named variables referenced by each escrow record. Errors from those name the variable and never the value, so a failure can be logged without leaking key material. A record created before the variables were renamed keeps the old name; the lookup falls back to the  spelling of it and logs which variable it actually read, so the drift is visible rather than silent. A record created before the variables were renamed keeps the old name; the lookup falls back to the `GATEWAY_` spelling of it and logs which variable it actually read, so the drift is visible rather than silent.

### Nonce accounting

Two ledgers with similar names answer different questions, and confusing them wastes an investigation. The **request ledger** (`GATEWAY_ACCOUNTING_*`, served at `/v1/requests/{id}`) records what became of one client request. The **nonce ledger** (`GATEWAY_NONCE_ACCOUNTING_*`) records where every committed nonce went — a request burns several, and some nonces belong to no request at all. Settlement counts nonces, not requests, which is why the second ledger exists.

`GATEWAY_NONCE_ACCOUNTING_ENABLED` builds the ledger and exports it as `devshard_gateway_nonces_*` on the gateway's ordinary metrics endpoint. There is no second port and nothing else to configure: a Prometheus that already scrapes the gateway picks the series up on its next scrape, and `deploy/join/observability/grafana/dashboards/gonka-gateway-escrows.json` reads them.

`GATEWAY_NONCE_ACCOUNTING_LISTEN_ADDR` additionally serves the ledger as JSON on its own port, for a reader that needs what a metric cannot carry: escrow ids and slots are unbounded labels and stay out of Prometheus deliberately.

| Route | Answers |
| --- | --- |
| `GET /api/v1/epochs` | every epoch the ledger holds, summed across participants |
| `GET /api/v1/epochs/{epoch}/participants` | every participant of one epoch |
| `GET /api/v1/epochs/{epoch}/participants/{address}` | one participant, addressed by its own chain address |
| `GET /api/v1/epochs/{epoch}/events` | the applied timeouts and invalid verdicts of one epoch, newest first |
| `GET /api/v1/epochs/{epoch}/events/{address}` | the same feed for one participant |
| `GET /api/v1/escrows` | the escrow ids the ledger holds |

`current` stands in for an epoch index and resolves against the chain's phase snapshot. Epoch `0` is refused rather than served: a zero epoch means "unconstrained" inside the ledger, so answering it would report every epoch as though it were one. `?model=` and `?escrow_id=` narrow every route but `/escrows`, and `escrow_id` accepts both repetition and commas; the event feed also takes `?participant=` where the address is not in the path.

The event routes are the drill-down under the counters: a participant's `timeouts_applied` says how many verdicts it took, and the feed says which nonce and which client request took them. Each escrow keeps its newest 256, which caps a pathological run rather than trimming normal traffic — a verdict is rare and an escrow dies with its epoch. A host with nothing against it is answered with an empty feed rather than a 404: here, unlike the participant route, nothing to report is the healthy case.

`POST /v1/admin/accounting/reset/{epoch}` clears one epoch and answers how many escrows went with it. It lives on the admin surface rather than this one because it erases records, and the epoch is named rather than defaulted to the current one for the same reason. Neighbouring epochs are untouched: a rotation leaves escrows of two epochs live at once.

The participant route is the one the surface exists for: a host operator can ask what this gateway saw of its own participant. An address the epoch holds no record of is a 404 rather than an empty list, so "nothing went wrong" stays distinguishable from "this gateway never routed to you". Every route is read-only and serves one gateway's view of public network behaviour, so the listener carries no authentication; it is still a separate port, and whether it is reachable beyond the deployment is the operator's choice.

**Findings.** Every participant record carries a `findings` array beside its counters: this gateway's reading of what the counters mean, for an operator who needs to know what to look at rather than what was counted. A finding names a stable `code`, a `severity` of `warning` or `critical`, the `observed` numbers behind the reading, and a `detail` that says what the behaviour costs and where to look. Findings are derived on every read and never stored — the counters are the fact, the finding is only an interpretation of them.

| Code | Reads |
| --- | --- |
| `execution_timeouts` | acknowledged and never finished — the expensive failure, held to the execution deadline |
| `refusals` | never acknowledged — the cheap failure, freed at the refusal deadline |
| `answers_unused` | finished after another host had already answered the client |
| `throttled_by_gateway` | burned without being sent because this gateway's own window was shut |
| `ledger_disagrees_with_chain` | more nonces classified than the chain assigned; no host behaviour produces this |

Two rules keep a finding honest. Burned nonces are excluded from every host-behaviour rate, because a burn is this gateway's decision and charging it to the host would report our own throttling as its failure. And a rate is only read once its sample passes a volume floor, mirroring the perf ejector: over a handful of nonces a rate describes the sample rather than the host. The thresholds are constants rather than configuration, so two gateways cannot report the same host differently and a host comparing two reports can tell that the host moved, not the ruler.

What the ledger cannot say: it holds nonce dispositions, not timings, so no finding speaks to prefill or decode rate. The nearest thing it offers is `answers_unused`, where losing races consistently points at throughput.

The metrics are gauges, not counters. A nonce's disposition moves when it is reclassified, so a series goes down as well as up and `rate()` over one reports nonsense. Every `devshard_gateway_nonces_*` series carries an `epoch` label: the ledger holds several epochs at once and a participant keeps its slot across a rotation, so the epoch is what separates two otherwise identical series — and it, not the dashboard's time range, is what a panel must filter on. A cumulative gauge does not respond to a time picker.

**Storage.** The ledger lives in memory and is written whole to `accounting.db` under the storage directory every `GATEWAY_NONCE_ACCOUNTING_SNAPSHOT_SECONDS` and once more at shutdown. Nothing queries that database except the ledger's own load at start-up, so its tables mirror the in-memory shape one for one and a write is a single transaction that empties and refills them. The transaction is what makes a half-written ledger impossible: a crash or a failed insert rolls back to the previous contents rather than leaving the tables empty.

A snapshot that cannot be read is reported and the gateway starts with an empty ledger: refusing to start over an unreadable observability file would trade a gateway for a graph.

Only the nonces whose disposition can still move are written down — those awaiting a timeout, and those an unfinished disposition might yet be lifted from. A burned or finished nonce is already counted and nothing lifts it, so the file stays close to the size of the trouble rather than the size of the history. Two things follow. A nonce whose race died with the process is named `abandoned_by_restart` rather than left pending for ever: no timeout was ever voted on it, and it will still settle as a completed inference nobody checked. And an unfinished nonce is re-asked on every sweep — if the protocol finished it after the race gave up, it leaves the unfinished bucket, because that bucket is what settlement reads as work the participant failed to do.

See gateway-capacity-and-health.md, "Nonce dispositions", for which gateway decision feeds which fact and for what the ledger cannot see.

**Two settings are environment-only and take effect at start-up.** `GATEWAY_CHAT_CACHE_MAX_BYTES` and the `GATEWAY_CAPTURE_*` group are read once when their component is built and are not rebuilt on a settings change. Both are deliberately absent from the override list below, and an override document naming one is rejected rather than accepted and ignored — the decoder refuses unknown fields.

### Admin overrides (24)

Run-time tuning, changeable without a redeploy: `default_max_tokens`, `max_tokens_cap`, `force_upstream_streaming`, `max_concurrent_requests`, `max_concurrent_requests_per_10000_weight`, `poc_max_concurrent_requests_per_10000_weight`, `max_input_tokens_in_flight`, `admission_queue_wait_ms`, `admission_queue_per_slot`, `match_wait_ms`, `warm_new_escrows`, `participant_allowlist`, `host_initial_inflight`, `host_max_inflight`, `host_cutoff_after_failures`, `host_cutoff_ms`, `host_cutoff_max_ms`, `model_limits`, `model_access`, `disabled`, `disabled_message`, `disabled_redirect_url`, `rotation_enabled`, `rotation_settlement_enabled`, `rotation_pre_poc_blocks`, `rotation_models_json`.

An unknown field in an override document is an **error**, not a silently ignored key: a typo in an admin PUT must be reported.

Only one component subscribes to configuration changes — the gateway limiter, which needs to sweep its queue when a cap widens. Subscribers run **synchronously inside the swap**, in no particular order, so a subscriber must be fast, must not depend on ordering, and must never trigger another swap. Everything else reads the snapshot per use. Three groups are effectively restart-only: values read once at boot (port, chain endpoints, transaction parameters, ledger retention, boot concurrency), the engine's reassembly budgets, and anything with neither an environment variable nor an override (stream and engine timers, scheduler hold grace, most performance thresholds, transaction poll cadence).

### Governance-driven, not local

The maximum active nonce count is a chain governance parameter, polled by the observer. The gateway never invents its own value; when the parameter has not been read it falls back to the historical ceiling of 19 800 rather than treating the cap as absent. Block height, epoch index, epoch phase and the requests-blocked verdict come from the same source and drive admission and rotation.

### Cross-field rules worth knowing

- `host_cutoff_max_ms` must not exceed the performance ejection maximum, so ejection stays the dominant authority over the breaker.
- `engine_loser_grace_ms` must be at least `engine_inter_chunk_stall_ms`, or losers merely between chunks are cancelled before the gateway would even call them stalled.
- `scheduler_match_wait_ms` is capped at 5 000 ms — a long grace parks a committed-cost nonce on the chance of a co-arrival, so the ceiling is a budget guard.
- `api_keys` must be non-empty if any model is on the `api_key` tier, otherwise that model is unreachable.

## Metrics

The gateway's own families are all `devshard_*` and the names are frozen: dashboards and alerts depend on them. The scrape also carries the standard `go_*` and `process_*` families, because the registry is built with the Go and process collectors registered alongside them. `/metrics` is deliberately *not* instrumented, so the scrape does not count itself.

Most of the gateway's families share the prefix `devshard_gateway_`; the exceptions are `devshard_http_*`, `devshard_runtime_*`, `devshard_host_transport_*` and `devshard_inference_timeouts_total`. Where a list below writes a short name, the full wire name carries the `devshard_gateway_` prefix.

**Race and inference** (from the engine's single recording point, so two call sites cannot disagree about what a race did):

| Family | Type | Labels |
|---|---|---|
| `devshard_gateway_attempts_started_total` | counter | participant_key, model, role, reason |
| `devshard_gateway_attempts_terminal_total` | counter | participant_key, model, role, outcome, visibility |
| `devshard_gateway_attempt_failures_total` | counter | participant_key, model, role, reason, visibility |
| `devshard_gateway_no_winner_attempts_total` | counter | participant_key, model, reason |
| `devshard_gateway_user_visible_wins_total` | counter | participant_key, model |
| `devshard_gateway_participant_transport_errors_total` | counter | participant_key, model, path_kind, status |
| `devshard_gateway_requests_total` | counter | model, outcome, reason |
| `devshard_gateway_critical_user_failures_total` | counter | model, reason |
| `devshard_gateway_user_requests_with_hidden_failure_total` | counter | model, severity, reason |
| `devshard_gateway_escalation_decisions_total` | counter | reason |
| `devshard_gateway_timeout_actions_total` | counter | participant_key, model, kind, action, reason |
| `devshard_inference_timeouts_total` | counter | reason |
| `devshard_gateway_stream_carry_overflow_total` | counter | participant_key, model |
| `devshard_gateway_participant_receipt_seconds` | histogram | participant_key, model |
| `devshard_gateway_participant_first_content_seconds` | histogram | participant_key, model |
| `devshard_gateway_participant_prefill_seconds_per_input_token` | histogram | participant_key, model |
| `devshard_gateway_participant_total_attempt_seconds` | histogram | participant_key, model |

**Limits and capacity** — gauges: `inflight_requests`, `inflight_input_tokens`, `effective_max_concurrent_requests`, `effective_max_input_tokens_in_flight`, `participants_tracked`, `participants_exhausted` (unlabelled); `inflight_requests_by_model`, `inflight_input_tokens_by_model`, `limiter_queue_depth`, `capacity_scale_by_model`, `capacity_total_weight_by_model`, `capacity_baseline_weight_by_model`, `capacity_weights_unobserved_by_model` (by model); `participant_window_size`, `participant_window_inflight` (by participant and model); `participant_breaker_state` (by participant, model and state).

**Registry** — `devshard_runtime_active`, `devshard_runtime_active_requests`, `devshard_gateway_escrow_weight`, `devshard_gateway_escrow_participant_limited`, `devshard_gateway_escrow_blocked_participants`, plus `devshard_gateway_escrow_drain_close_failures_total`. The last counts drained escrows whose snapshot flush or session close failed: retire and close hand that error to their caller, but a drain that ends after the last request has been answered has nobody left to return it to, so it is counted instead of lost.

**Chain** — `chain_block_height`, `chain_epoch_index`, `chain_epoch_switch_block_height`, `chain_max_nonce` (0 means not yet fetched), `chain_requests_blocked`, `chain_snapshot_age_seconds`, `chain_snapshot_healthy`, plus `chain_epoch_phase{phase}` and `chain_block_reason{reason}` as one series per enum member.

**Scheduler** — `devshard_gateway_ghost_nonces_burned_total{devshard_id,reason}`, `devshard_gateway_nonce_holds_total{devshard_id}`, `devshard_gateway_burn_budget_exhausted_total{devshard_id}`. A nonce is money whichever way it is spent, so a burn, a hold and an exhausted budget each get their own family.

**Host and process** — `devshard_gateway_host_ejected{participant_key,model}`, `devshard_gateway_host_inflight_requests{participant_key}`, `devshard_host_transport_open_connections{address}`, `devshard_host_transport_connections{address,state}`, `devshard_gateway_accounting_rows_written_total`, `devshard_gateway_accounting_rows_lost_total{cause}`, `devshard_gateway_accounting_retention_sweeps_failed_total`, `devshard_http_requests_total{path,method,status}`, `devshard_http_request_duration_seconds{path,method}`. A non-zero retention-sweep failure count means a retention delete did not run, so the ledger is past the age or row bound it was configured with. That sweep is the only thing enforcing the bound, so a failure there means the bound is now merely declared.

**Request capture** — `devshard_gateway_captured_requests_total`, `devshard_gateway_captured_requests_refused_total`, `devshard_gateway_capture_bytes_held`. A rising refusal count is not a sampling artefact: nothing evicts capture files, so once the directory reaches its byte cap the sink turns itself off and stays off until an operator empties the directory. `capture_bytes_held` against the configured cap is the gauge that says how close that is.

### Cardinality rules

- A route label is always a route *pattern*, never a raw path; every unmatched path folds into the single label `other`. A raw path would let unauthenticated traffic mint one series per probe.
- Escrow ids appear freely on gauges, where stale series expire, and on counters only for the three scheduler nonce families above, because a spent nonce that cannot be attributed to the escrow that paid for it is money the operator cannot account for. That cost is paid there and nowhere else, and it is paid only while the escrow exists: escrow ids are monotonic chain identifiers and are never reused, so nothing would ever retire those series on their own and every rotation would leave one behind permanently. The dispatch recorder therefore drops all three families for an escrow id when the scheduler reaps that escrow's dispatcher. The registry's per-escrow gauges need no such step, because they are rebuilt from the live set on every scrape.
- A `reason` label is a constant chosen at the branch, never derived from an error string. Terminals with no recovered upstream status report `status="0"` rather than minting one label per error message.
- An empty label value is never emitted: it reads on a dashboard as "does not apply", which is indistinguishable from a broken emit site.
- Enumerations are emitted as one series per member with 1 on the active one (chain phase, block reason, breaker state), which keeps the label set closed.

### The in-repo dashboard needs an edit

`deploy/join/observability/grafana/dashboards/gonka-gateway-observability.json` queries six families this gateway does not emit: `devshard_gateway_limit_rejections_total`, `devshard_gateway_participant_limit_rejections_total`, `devshard_gateway_participant_quarantine_state`, `devshard_gateway_picker_choice_total`, `devshard_gateway_slot_decisions_total`, and the unsuffixed `devshard_gateway_capacity_scale`. All six are defined only by the legacy binary. Three more of its families have no panel and no successor family, only a different answer to the same question: `devshard_speculative_decisions_total` and `devshard_speculative_attempt_starts_total` are the escalation counters, now `devshard_gateway_escalation_decisions_total` and `devshard_gateway_attempts_started_total`; `devshard_gateway_startup_skipped_escrow` has no counter here at all -- an escrow whose state cannot be recovered at boot is written down (`devshard cannot be served, marking inactive`) and nothing counts it, so an operator watching only dashboards would not see one. The quarantine panels have no successor at all — the mechanism was deliberately deleted. The others need repointing at the new family names, or replacing with the scheduler's ghost-burn and hold counters, which cover the same question. Conversely, most of the new chain, transport, accounting, dispatch and breaker families have no panel yet.

There are no alerting rules in the repository, so nothing pages on any of this today.

## Logs

The gateway logs through `devshard/logging`, the same package the rest of devshard uses, so its lines land in the operator's existing format. It writes far less than the legacy binary's roughly three hundred call sites, and the difference is one of kind: the legacy names a `stage=` on every step of a failing operation, while here the error carries that itself, wrapped as `resolving signer for escrow X` or `building settlement for escrow X`. The escrow tick logs the joined result once. Adding stage labels beside errors that already name their step would restate the same fact twice.

What it does write is every event that moves money, changes what the gateway will serve, or is an operator's own doing.

| Event | When | Why it is worth a line |
|---|---|---|
| `request finished` | every completed race | the delivery record, below |
| `escrow created` | a create transaction landed | carries the escrow id, model, role, epoch and tx hash — the moment funds were committed |
| `escrow settled` | a settle transaction landed | carries the tx hash and the settling address; this is the audit line for money leaving |
| `escrow parked for settlement` | routing stopped, row marked pending | the escrow stopped taking traffic and is now waiting to settle |
| `settled escrow record dropped` | the row was deleted | that row named the only key able to settle the escrow, so its removal is irreversible |
| `escrow depleted with no replacement configured` | nonces exhausted, rotation has no model for it | capacity left the fleet and nothing replaces it |
| `escrow tick failed` | the lifecycle tick returned | one line carrying every joined failure, each naming its own step and escrow |
| `escrow serving` / `escrow retired` / `draining escrow closed` | an escrow entered the published set, left it, and finally let go of its storage | the close runs after the last in-flight request, on nobody's request path, and is invisible otherwise |
| `escrow warmed` | a new escrow taught itself to its group: the nonce it spent, whether the executor answered, and any catch-up failure | see [gateway-escrow-lifecycle.md](./gateway-escrow-lifecycle.md), "Warming" — `catch_up_error` names hosts that stayed cold |
| `escrow gone from chain, taken out of service` | a lookup confirmed the escrow no longer exists on chain | `escrow retired` fires for settlement parking too, so this is the only line carrying the cause |
| `escrow recovered from commitment` | reconciliation resolved a durable creation intent to an escrow id | the create landed while the gateway was down, so `escrow created` never ran and the escrow exists in no other line |
| `commitment cleared` | a durable creation intent was abandoned | it names the reason, and one of the two — `transaction created no escrow` — means the transaction did commit |
| `bridge prepared` / `bridge finished` / `regular escrows promoted to temp` | one phase of the epoch rotation completed | which phase asked for the escrows that just appeared and left, and whether the bridge is made of fresh temps or relabelled regulars |
| `settle tx broadcast` | a settlement transaction reached the node, before the wait for its commit | for the confirm window the transaction hash lives nowhere else; if the process dies there, money moved under a name nobody recorded |
| `chain snapshot stale` / `chain snapshot recovered` | a poll started or stopped failing — on the edge only | the health gauge says the view went stale, not which of four reads went dark, and a failed refresh keeps routing on the previous participants |
| `attempt crowned` | one attempt became the client's answer | which host actually served a request is in no metric, and the crown was once decided by a random `select` |
| `attempt finished` | an attempt reached a terminal | carries the terminal **the attempt itself reported**. A goroutine sees only its own cancellation, so the coordinator reclassifies at the end of the race: an attempt that outlived the backstop becomes `hard_timeout`, and a host that went silent mid-stream becomes `stalled`. The line itself still reads `client_cancelled`, because that is what the attempt saw |
| `host blocked for state divergence` | a host returned a post-state-root the escrow does not share | the block never lifts while the process runs, and no metric exposes it, so "why is this host never picked" is answerable only here |
| `escalation unfilled` | an escalation pick was given no host | no nonce is committed, and the error reaches a caller only when no attempt ever started |
| `nonce burned for nobody` | a ghost nonce was committed | the nonce cost money on chain and will serve nobody; the line names the escrow and the reason |
| `escrow stopped burning nonces at its budget` | a drain reached its burn budget | the escrow now queues callers rather than spending on requests it cannot serve |
| `chain epoch` / `chain blocked requests` / `chain unblocked requests` | the epoch or the blocked verdict changed | the observer republishes every five seconds whether or not anything moved, so only the change is written |
| `admin: …` | nine operator mutations | settings replaced; escrow registered, imported, deleted, activated, deactivated; participant added to the never-trust list, removed from it; breaker cleared |
| `admin request failed` / `admin request refused` | an operator route answered 5xx, or refused with a 4xx | the mutation lines above are written on the successful path only, so a failed operator action was invisible: the route, method, status and the message the caller was given |

The admin lines carry the action and its subject, never the request body: an override payload can hold the admin key. The failure line carries the response message, which the gateway wrote itself, truncated at the same bound as every other logged error.

An unkeyed call on an operator route is refused with a 401 and written down: that is the shape an intrusion attempt takes, and it is worth a line even though nothing failed.

A participant's breaker is deliberately not logged. Its state is already a per-participant metric with history, so a dashboard shows both that traffic stopped and when; a line would add only the cause, at the price of a push contract in a package that is otherwise pure algorithm. If the cause turns out to be the thing that is actually needed, that is the moment to pay for it.

Failures on the money path are not logged separately, because every one of them is returned as a wrapped error that names its own step, and the tick logs the joined result. A success has no such carrier, which is why the successful transitions are the ones written down.

### The trace

The trace is always on. There is no level knob, deliberately: a trace that ships off by default is not there for the incident that already happened, and turning it on afterwards cannot recover what was not written.

| Line | Carries |
|---|---|
| `nonce committed` | request, escrow, nonce, participant, slot, role, and why this attempt started |
| `attempt finished` | the same identity plus the terminal verdict, whether the nonce was finished, and whether the host diverged on state |
| `nonce stranded` | at Warn: a committed nonce nobody will answer for is the shape every recurring settlement defect here has taken |

Following one request means grepping its request id; following one nonce through commit, dispatch and verdict means grepping the nonce.

Volume is roughly five lines per request — one per attempt commit, one per attempt verdict, one on completion. A level knob belongs here only once a measured volume demands it; adding one now would mean shipping the trace off by default.

### The request record

`request finished` is one line per completed race, at Info when it went out clean and Warn when it did not. Its fields answer the one question a finished request can no longer be asked:

| Field | What it settles |
|---|---|
| `bytes` | how much actually reached the client, counted at the socket rather than at the caller, because the strip rewrites events on the way out |
| `terminated` | whether the SSE terminator went with them; without it a client waits out its own timeout on a reply it already has |
| `outcome` | `served`, `failed_mid_stream`, or `failed_before_first_byte` — the last distinguishes a reply the client can retry from one it cannot |
| `deliver_error` | the failure that reached the client instead of the last bytes, which is the case no status code can express: a stream commits 200 on its first byte |

The record deliberately carries no request or response body. Capture files exist for that, are sampled, and are bounded.

Its `error` field is truncated at 256 bytes, and that is not tidiness. A `HostApplicationError` renders its raw upstream payload as the error text when the host sent no message, so an untruncated field would write a whole SSE event — generated tokens included — once per failed request. The other lines carrying an error text are bounded by cadence instead: the chain's staleness line fires twice per outage rather than per request, and the escrow tick logs at most once every fifteen seconds.

### What is still unanswerable

Nothing records per-stage timing inside a request, so "where did the latency go" needs the metrics, which are per-gateway rather than per-request. The record cannot say whether a client read what it was sent — only that the gateway wrote it. And nothing logs the moment a participant's breaker opens: the state is a metric, so a dashboard shows that traffic stopped, but not when it stopped or after which fault.

## Start-up

1. Read the environment, resolve the storage directory (default `$HOME/.cache/gonka-gateway`), open the store.
2. Load admin overrides, build and validate the configuration.
3. Construct everything without starting anything.
4. Start the chain observer, seed the escrow registry from `GATEWAY_ESCROWS_JSON`, publish active escrows, start the escrow tick, start the listener.

Seeding leaves an escrow it already knows alone, so a restart cannot resurrect one an operator deactivated.

**The chain is reached over gRPC alone** (`chain_grpc`). One dial serves the escrow bridge, the phase observer's two chain reads, and every transaction; its fallback needs no setting, because `common/chain` derives the CometBFT RPC endpoint from the gRPC host at the standard port, which is how every deployment is laid out. `public_api` is a different thing and still HTTP: it is the decentralized-api, not the chain, and it serves epochs and participants.

`chain_id` is configuration rather than discovery. A mismatch invalidates every signature the gateway produces, so the operator's value decides; with none set the gateway asks the node and signs against whatever chain the connection actually reaches.

**Environment variables accept both spellings.** The gateway reads its own `GATEWAY_*` names first and falls back to the `devshardctl` name for the nineteen settings that have one, so a deployment still running the shipped template starts without an edit. The suffixes are not a mechanical swap — rotation, the disabled switch, the PoC mode and the tx-query URLs were all renamed — so the pairs are listed explicitly in `env/env.go`. Where both are set, the `GATEWAY_*` value wins; an empty value counts as unset on both sides. This interacts with the storage rule below: falling back to `DEVSHARD_STORAGE_DIR` points this gateway at the directory `devshardctl` uses, which is exactly the case the database guard refuses — loudly, at start-up, rather than silently.

**Point this gateway at a storage directory `devshardctl` has never used.** Both binaries name their database `<storageDir>/gateway.db`, and two table names are common to both with different columns. Because every migration here is a `CREATE TABLE IF NOT EXISTS`, opening a `devshardctl` database would silently adopt the legacy shape for those two and abandon the legacy devshard and suspicious-host registries for fresh empty ones — a start that looks clean and then fails one query at a time. The store therefore refuses to open such a file, recognising it by the absence of `schema_version` alongside a table only `devshardctl` creates. There is no migration path between the two; the escrows are re-seeded from `GATEWAY_ESCROWS_JSON`.

Publishing builds escrow runtimes concurrently under a semaphore whose depth is also the HTTP idle-connection pool size, so bounded concurrency does not turn into connection churn. Two failures are survivable and mark the escrow inactive with a warning — the escrow is gone from chain, or its key variable is unset. **Every other failure is fatal**, so the gateway never quietly comes up with a smaller pool than intended.

## Shutdown and restart

Shutdown is eight ordered steps under a ten-second grace period; see [gateway-invariants.md](./gateway-invariants.md) for the contract and the reasoning. What matters operationally:

- The listener stops accepting immediately; in-flight races continue to the vote that settles their nonces.
- If the grace period expires, the overrunning drain is **left running** and reported rather than cancelled. The store and the public API connections still close; the escrow sessions deliberately do **not**, because closing a session takes no lock and closing one under a drain still committing nonces corrupts on-disk state to save a vote this shutdown was already going to drop. If the orchestrator then sends SIGKILL, votes can be lost — a longer grace period buys real safety here, because the protocol wait a vote may block on is measured in minutes.
- Queued accounting rows are drained before the database handle closes.

What survives a restart: escrow records, creation commitments, rotation status, admin overrides, suspicious-host pins, the accounting ledger.

What does not: participant windows, breaker state, host ejections, capability flags, the response cache, and the in-flight escrow counters. A restart therefore gives every host one clean window, and the first requests after a restart route on membership share until the first successful chain poll lands.

## Reading the gateway's state

- `GET /v1/status` and `GET /v1/admin/state` for a live overview. The capacity block carries every weight under two spellings, old and new; the older names are what existing dashboards select on, so they are kept deliberately.
- `GET /v1/debug/rotation` for what rotation did last, per model and role, including the last creation error.
- `GET /v1/requests/{id}` for one request: which escrow and participant served it, which nonce, the winner's timings, and output tokens summed over every attempt. No monetary cost — that lives in chain settlement.
- `devshard_gateway_capacity_weights_unobserved_by_model` for the one silent degradation: escrow scoring running on membership share because the chain reported no weights.

## Host clock offset

Every `request finished` line for a stamped reply carries `host_clock_offset_s`: the `created` the winning host put on its answer, minus the second the gateway dispatched to it. Upstream stamps a reply once, when it accepts the request, and repeats that stamp on every chunk — so the value is bounded from both sides by facts the gateway already knows.

A small positive number is ordinary: the host accepted the request shortly after we sent it. **A negative number is not reachable by any slow host** — it says the reply was accepted before we dispatched it, which only a clock running behind ours can produce. A value larger than the request's own time to first token says the opposite: a clock running ahead.

The stamp has one-second resolution, so this reads drift, not latency. It is not a substitute for the first-token measurement, which the gateway takes on its own clock and which is what the escalation ladder acts on.
