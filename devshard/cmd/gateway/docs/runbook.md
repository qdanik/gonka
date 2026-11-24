# Runbook

Step-by-step procedures for running the gateway: releasing, rolling back, backing up, settling and deactivating escrows, and finding the cause of a symptom. Every command is run against a live gateway; the keys they set are described in [configuration.md](./configuration.md) and the signals they read in [observability.md](./observability.md).

## Before you start

The commands below use two shell variables: `GATEWAY`, the base URL of the gateway (`http://localhost:18080` on the [`deploy/`](../deploy/README.md) stack), and `ADMIN_KEY`, the value of `DEVSHARD_ADMIN_API_KEY`. The `docker compose` commands run from the `deploy` directory.

```bash
export GATEWAY=http://localhost:18080
export ADMIN_KEY=<the value of DEVSHARD_ADMIN_API_KEY>
```

## When something is wrong

| Symptom | Look at |
| --- | --- |
| every request 429 | `limiter_queue_depth` and `effective_max_concurrent_requests` — the weight model may have scaled capacity to almost nothing after an epoch switch |
| every request 503 with no model listed | the chain snapshot is stale, or the escrow set is empty — check `chain_snapshot_healthy` and `/v1/admin/devshards` |
| 503 on a healthy-looking gateway | `buffered_response_bytes` at the ceiling: non-streaming replies are holding the whole budget |
| a model returns 401 to every caller but the admin | neither the model nor the network names an `access` for it — set `access` under `models.<id>`; on an `api_key` model, a caller whose key is not an active client key is refused too, so list them with `GET /v1/admin/api-keys` |
| the admin surface 404s | `DEVSHARD_ADMIN_API_KEY` is unset |
| burns climbing | `ghost_nonces_burned_total` by reason — see [accounting.md](./accounting.md) |
| prompts past a model's context reach hosts | the log line `prompt tokenizer could not be loaded`, `prompt tokenizer pin could not be read`, `model has no tokenizer the gateway reads` or `prompt tokenizer parse panicked` for the model, or no context length for it — see [operations.md](./operations.md), "The context check" |
| shutdown reports "abandoned with work still running" | a host stopped answering and the drain hit the grace period; the votes it owed were not paid |

To reproduce an escrow lifecycle problem (create, drain, timeout votes, settle, prune) without a stand, use the scenario tests in [`scenarios/`](../scenarios/README.md); the `loadsim` test described in [CONTRIBUTING.md](../CONTRIBUTING.md#load-simulation) stays the profiling tool.

## Release and rollback

A release is an image. `make release` builds it for `linux/amd64` (`PLATFORM`) and pushes it as `$(GATEWAY_IMAGE):<tag>` for every tag in `GATEWAY_TAGS`. It refuses to run without `GATEWAY_VERSION`, which is stamped into every settlement ([`CONTRIBUTING.md`](../CONTRIBUTING.md), "Before you push"), and without `GATEWAY_TAGS`.

To release and deploy:

1. Build and push the image from `devshard/cmd/gateway`:

   ```bash
   make release GATEWAY_VERSION=<version> GATEWAY_TAGS='<tag> <tag>-latest'
   ```

2. Point the stack at the new tag and recreate the container from the `deploy` directory. `GATEWAY_IMAGE` is a full image reference with its tag:

   ```bash
   export GATEWAY_IMAGE=<registry>/<image>:<tag>
   docker compose -f docker-compose.devshard-gateway.yml pull
   docker compose -f docker-compose.devshard-gateway.yml up -d
   ```

   `up -d` stops the running container with SIGTERM, so the outgoing gateway runs its whole shutdown ([operations.md](./operations.md), "Shutdown") before the new one opens the same storage directory.

3. Check the new process: `curl -s "$GATEWAY/v1/status"`.

To roll back, set `GATEWAY_IMAGE` to the previous tag and repeat step 2. Before you do, read what the newer release wrote into the storage directory, because that decides what the older one reads:

- **`gateway.db`** is versioned by `schema_version`, one row per applied entry of `migrations` (`store/store.go`, `migrate`). The list is append-only, so a release reads every store a release with a list no longer than its own wrote, and migrates it up at `Open`. An older release opening a store a newer one migrated applies nothing and does not refuse it: the tables and columns it knows are all still there, and every column a later entry adds carries a default, so the rows it inserts stay valid. A rollback across a migration that is not a plain added table or column is not covered by this; read the entries the newer release added before rolling back over them.
- **`accounting.db`** carries the ledger's `accounting.SchemaVersion` and is read only by a release with the same number (`accounting/sqlload.go`, `Store.Load`). Across a change of that number the ledger starts without its stored counters, logs `nonce accounting started without its stored counters`, and replaces the stored tables on its next write. That loses the ledger's history, never money: the ledger is observation, and settlement reads the escrow sessions ([accounting.md](./accounting.md), "Storage").
- **The escrow sessions** (`escrow-<id>/epoch_<N>.db`; [operations.md](./operations.md), "Session storage") are written by the devshard libraries the release is built with, and a gateway release runs only with the devshard release of the same version.

## Upgrade

The stack in [`deploy/docker-compose.devshard-gateway.bluegreen.yml`](../deploy/docker-compose.devshard-gateway.bluegreen.yml) runs two gateways, blue and green, behind nginx, and upgrades one to the other without a gap in service. From `devshard/cmd/gateway`:

```
make upgrade UPGRADE_IMAGE=<registry>/<image>:<tag>
```

The command is `deploy/upgrade.sh`; its flags and the first start are in [`deploy/README.md`](../deploy/README.md), "Upgrading without downtime". The two colours never share a store ([operations.md](./operations.md), "One process per escrow set"): each keeps its own storage directory and mounts the other's read-only, so the incoming one can read the outgoing one's escrow sessions through `POST /v1/admin/devshards/import`.

The order, with the old colour A and the new one B:

1. B starts on the new image with no seeded escrows and rotation off (`DEVSHARDS_JSON` and `DEVSHARD_ESCROW_ROTATION_ENABLED` are fixed in the compose file), and the script waits for `/v1/models`. B booting with A's escrows would sequence the same nonces as A, and B with rotation on would fund a set of its own.
2. A's admin settings are copied to B, minus `rotation_enabled`: they live in the store, so a fresh B would otherwise serve `model_access` as admin-only. `rotation_enabled` is turned off on A, so no escrow is created while they move; a gateway applies it live, on its next escrow tick (`escrow/manager.go`, `Manager.tickWith`), with no restart.
3. Wave 1 hands over one active escrow per model, a regular one before a reserve: deactivate on A with `POST /v1/admin/devshards/{id}/deactivate?handover=true`, wait until `GET /v1/admin/devshards` shows the row without `runtime` (the session is released once the requests already running on it end and the refusal votes they owe are posted, `registry/README.md`, "Publishing, retiring and draining"), then import on B. The `handover=true` query stops A's session waiting for execution votes not yet due ([race.md](./race.md), "Handing an escrow over"), so those do not set the length of the wait; a deactivation without it keeps holding the session until they are posted, so a settlement started right after it waits for them. A deactivated escrow takes no new request, so A keeps routing to its other escrows while B takes its first.
4. B is checked for being alive (below), nginx switches to B with a graceful reload, the script waits until nginx reports B, and B is checked again. Requests already open stay on A's connections until they finish.
5. Wave 2 hands over the rest, which A no longer receives traffic for.
6. `rotation_enabled` is turned on again on B if it was on, A is given `--drain-timeout` seconds to bring both `devshard_gateway_inflight_requests` and `devshard_gateway_owed_timeout_votes` to 0, B is checked once more, and only then is A stopped and the script says B serves. A's storage stays where it is until the next upgrade moves it aside to `data/<colour>.retired-<time>`; nothing deletes it.

A step before the switch that fails stops B, reactivates what was deactivated on A, restores A's settings and puts the previous nginx config back, so A keeps serving. A reactivation A refuses with `409` because the earlier session is still held is repeated every two seconds for up to `--drain-timeout`, and A writes a row it refused to activate back inactive, so a row never reads active for an escrow nothing serves; one still refused after that is logged, and the script exits non-zero with that escrow inactive on both colours. After the switch nothing is rolled back: A keeps running with what is left, and the script prints the escrows it had imported and those still on A. A B that fails the check before the switch is handled as any step before it. When B passes the check after the switch, it serves, and the rest is finished by hand; when it does not, see "A dead new colour" below. Finish by hand with the same calls for each escrow still on A: `POST /v1/admin/devshards/{id}/deactivate?handover=true`, wait until its row has no `runtime`, `POST /v1/admin/devshards/import` on B with `source_path` `/handover/<A>/escrow-<id>` and the row's `rotation_role`, `rotation_epoch` and `route_prefix` from `GET /v1/admin/devshards` on A; then turn `rotation_enabled` on again on B and stop A. A second upgrade in the other direction would leave them behind, since it refuses while A runs and moves A's storage aside.

B is alive when it answers `/v1/models` and `GET /v1/admin/devshards` on B shows a session (`runtime`) for every escrow imported active, except one B retired itself (inactive, gone from the chain, parked); the script retries that check for up to 30 s before it gives up. A dead new colour: if B fails the check after the switch or before A is stopped, or a later step fails while B fails it, the script says that B does not run every escrow imported to it, exits non-zero, does not stop A and does not reverse the switch. The end state is that nginx sends every request to B and the models of the escrows B does not run are not answered. The script lists four groups: the escrows imported to B; the escrows with a copy in `data/<B>/escrow-<id>` and no confirmed import, which count as imported; the escrows deactivated on A and not copied, which are inactive on both colours and served by none; and the escrows not yet handed over, which are unchanged on A, a colour nginx does not reach. Recovery:

1. Read `docker compose ps` and the logs of B (the compose file restarts it unless stopped), then restart it with `docker compose -f docker-compose.devshard-gateway.bluegreen.yml restart gateway-<B>`. B reloads its imported escrows from its own storage and continues each session from the nonce it holds there: the import copied the session files into B's storage and wrote the row into B's `gateway.db` before it answered (`app/operations.go`, `ImportDevshard`), and boot publishes every active row from that storage (`app/devshards_publish.go`, `publishEscrows`; `user/httpsession.go`, the recovery of an existing session).
2. `GET /v1/admin/devshards` on B shows a `runtime` for each escrow it serves. An active row with no `runtime` after the boot is unserved, and B's log says why.
3. Do not activate an imported escrow, or one with a copy in `data/<B>`, on A. `POST /v1/admin/devshards/{id}/activate` on A reopens the session from A's own storage, at the nonce A deactivated it at, and nothing marks the escrow as handed over (`app/operations.go`, `Activate`; the refusals are a parked escrow and `409` while the earlier session is draining), so a nonce B spent since would be sent again. For an escrow with an unanswered import, look at B's list first: a row there is B's.
4. Do not run `upgrade.sh` again (it refuses while A runs) and do not move `data/<B>` aside; B's storage holds the only current session of its imported escrows.
5. When B serves, hand over the escrows left on A with the calls above; a row already inactive on A needs no deactivation, only the import. Then turn `rotation_enabled` on again on B if it was on, and stop A.

What clients notice:

- **A model with two or more escrows** runs on one escrow fewer, from each escrow's deactivation until its import. Wave 1 moves one escrow per model, so the model never has fewer than one, and wave 2 happens after the switch, when B already serves the first. It can still answer `409 pinned escrow gone` to a request that was queued on the escrow when it was deactivated ([routing.md](./routing.md), "One re-pick when an escrow gives up"), and `503 no escrow capacity` when the escrows left cannot take the load, as on a fleet with failing or paused hosts.
- **A model with a single escrow** answers 503 when it is named in `model_access` or `model_limits`, and 400 otherwise, from its deactivation until wave 1 has imported it and nginx has switched. That span is the wave's wait for the slowest escrow in it: the requests running on the escrow end and the refusal votes they owe are posted, then come the copy and the switch. On a healthy fleet it is seconds, and it does not include `refusal_timeout` (60 s by chain default) because no refusal vote is owed. Failing or paused hosts stretch it two ways: a request a host refused owes a refusal vote, and a request a paused host accepted keeps running, since the handover does not cancel running attempts, until the host or the gateway's own deadline ends it. Execution votes are not waited for, since they travel with the escrow. A request that streams for minutes lengthens it by as much. `--drain-timeout` (default 900 s) bounds it: an escrow still held when it runs out fails the upgrade before the switch, and the rollback leaves the old colour serving.
- **A cold B**: its response cache and host performance estimates start empty until traffic warms them.
- **404 for `/v1/requests/{id}`** of a request served before the upgrade: the request records are in A's store, and B starts with none (`accounting.db` is not carried over either).
- **Timeout votes**: execution votes not yet due travel with the escrow, since the nonce and its confirmation time are in the session files B copies; B's timeout sweep posts them once due (`registry/timeout_sweep.go`, `GATEWAY_TIMEOUT_SWEEP_BUDGET_PER_TICK`), as it does for a nonce no race is left to vote. Refusal votes and error-miss claims are posted by A or not at all: a refusal vote needs the prompt A saved in A's `gateway.db`, which B's boot replays only from its own store ([operations.md](./operations.md), "Boot", step 7) and the import does not copy, and a claim holds a proof only A's memory has. Finalization charges the nonces of a vote that is never posted in full ([race.md](./race.md), "The timeout-vote queue"). A race holds its escrow's session on A until those votes are posted (`engine/engine.go`, `holdEscrow`), so the wait for the row to lose its `runtime` in steps 3 and 5 waits for the refusal votes of every escrow being handed over, and only for them. The wait in step 6 for `devshard_gateway_owed_timeout_votes` is a second check, before A stops, for votes held on escrows that were not handed over; a handed-over execution vote leaves the count when A stops waiting for it.

The escrow cost: none when every model has two or more active escrows, since the session files are copied and the chain sees no transaction; a model with a single escrow has the 503 span above, which one extra escrow created beforehand closes, at one create fee per model. An inactive escrow with no session directory on A is registered inactive on B instead of imported. Escrows that are parked for settlement are not handed over, and the script refuses unless `--skip-parked` is given; the outgoing colour then keeps running to settle them and is not stopped by the script, and the next upgrade refuses until it is stopped. Rotation stays off while the handover runs; an escrow that appears on A after the list is read, or is parked on it, stops the script before it stops A. An escrow create that was in flight when rotation was turned off finishes on A, so check `GET /v1/debug/rotation` on A before the upgrade.

The import carries the model, the key variable, the session files and, when given, the row's `rotation_role`, `rotation_epoch` and `route_prefix` (`ImportDevshardRequest`, `app/operations.go`, `ImportDevshard`), which the script reads from A's `GET /v1/admin/devshards`. They are stored as a rotation-created escrow's are, so the planner on B counts an imported escrow against its targets and turning rotation back on funds nothing new for it. Left out, they are stored empty: a row with no rotation role and epoch 0 on the build's own route prefix, which the planner counts against `max_unsettled` but toward no epoch's target and no reserve count. The route prefix is written when the row is new; an import onto a row the colour already holds keeps that row's prefix.

The shutdown grace (`shutdownGracePeriod`, 10 s) is not raised for this: A is drained by the script before it gets SIGTERM, so a stream is cut only when `--drain-timeout` runs out.

## Backup and restore

The storage directory (`DEVSHARD_STORAGE_DIR`, mounted from `DEVSHARD_STORAGE_HOST_DIR`) holds everything the gateway remembers: `gateway.db` (escrow rows, create commitments, admin overrides, client API keys), `accounting.db` (the nonce ledger) and one `escrow-<id>/` directory per escrow session. The three describe the same escrows at the same moment, so they are archived together and restored together: a `gateway.db` older than the session directories beside it lacks the rows of escrows those sessions serve, and session directories older than the rows hold nonces the hosts have moved past. No command takes a backup for you; this is the manual procedure.

1. **Stop routing.** Turn on the kill switch ([operations.md](./operations.md), "The kill switch") through the admin settings: `curl -X POST -H "Authorization: Bearer $ADMIN_KEY" -d '{"disabled": true}' "$GATEWAY/v1/admin/settings"`. Clients are refused from then on; the admin and recovery surfaces stay up.
2. **Wait for the drain.** `devshard_gateway_inflight_requests` reaches 0 once the last client has its answer, and `devshard_gateway_owed_timeout_votes` reaches 0 once every vote the races owe is posted, which can take as long as the protocol's own deadline, half an hour after a host confirmed the work ([race.md](./race.md), "The timeout-vote queue").
3. **Stop the container.** `docker compose -f docker-compose.devshard-gateway.yml stop` runs the shutdown ([operations.md](./operations.md), "Shutdown"): the ledger's final write, the sessions' close and the store's close. The escrow tick and the ledger's sweep write to the storage directory while the process runs, so it is copied only once the process is down.
4. **Checkpoint the WAL.** Both databases run in WAL mode (`connectionPragmas` in `store/store.go` and in `accounting/sqlstore.go`). A clean close folds the WAL back into the database; where a `gateway.db-wal` or `accounting.db-wal` file is still there, fold it in before copying: `sqlite3 gateway.db 'PRAGMA wal_checkpoint(TRUNCATE);'`, and the same for `accounting.db`.
5. **Archive the whole directory in one piece**, for example `tar -C "$DEVSHARD_STORAGE_HOST_DIR" -czf gateway-storage-<date>.tar.gz .`, so `gateway.db`, `accounting.db` and every `escrow-<id>/` come from the same moment.
6. **Start again** with `docker compose -f docker-compose.devshard-gateway.yml up -d`; the admin overrides are in the store, so the kill switch is still on. Turn it off with `{"disabled": false}` through the same endpoint.

To restore, stop the container, replace the contents of the storage directory with one archive's contents (never files from two archives), and start it with the kill switch still on from the archived overrides; check `/v1/status` and the escrow list before turning it off. Two rules decide what a restored store does at boot ([operations.md](./operations.md), "Boot", steps 4 and 5):

- **`DEVSHARDS_JSON` writes rows back.** An escrow settled after the archive was taken has its row restored as active, and the seed writes back every seeded escrow whose row settlement deleted. Neither is served: publication asks the chain, refuses an escrow it reports settled (`bridge.ErrEscrowSettled`) and deletes its row again, so a restore cannot resurrect a settled escrow.
- **What happened after the archive is not in it.** An escrow created after the archive was taken has no row in it, so the restored gateway does not know it; an escrow that served after the archive comes back with its archived session, older than the copy its hosts hold. Take the archive right before the change it protects against, and settle or let drain what you do not need restored.

## The participants setting

`participants` is one admin setting holding three lists of participant addresses. It is set through `/v1/admin/settings`, by `PUT` or `POST`: there is no env value and no default behind the network lists. A model's own lists (below) can also come from the `participants` of a model in `GATEWAY_MODELS_JSON`; the admin layer wins per model and per list.

```json
{"participants": {"trust": ["gonka1..."], "shadow": ["gonka1..."], "unthrottled": ["gonka1..."]}}
```

```bash
curl -X POST "$GATEWAY/v1/admin/settings" -H "Authorization: Bearer $ADMIN_KEY" -H "Content-Type: application/json" -d '{"participants": {"shadow": ["gonka1..."]}}'
```

- **`trust`** narrows **which participants this gateway routes client traffic to**. When it is non-empty only its members, and the `unthrottled` ones, are routed to; when it is empty everybody is, except the `shadow` members.
- **`unthrottled`** names participants whose congestion window and ejection the gateway declines to apply, and admits them whether or not `trust` names them. See [routing.md](./routing.md), "Unthrottled participants".
- **`shadow`** names participants that receive inference probes and never client traffic. A probe repeats a client request the gateway has queued, with `max_tokens` and `min_tokens` both set to one value drawn per probe from 256 to the least of 1024, the model's output cap and the context room the prompt leaves, so the shadow host receives that client's prompt; it is sent to the host in place of the ghost burn its nonce would otherwise be, so the probe rate follows the shadow host's share of an escrow's slots, and a probe is the operator's cost: billed like the burn it replaces plus the tokens it reserves, never charged to the client it repeats. A grammar-constrained request, or one that carries tools, is repeated with no `min_tokens`; a request with under 256 tokens of room, or one whose body does not decode, is passed over for the next queued request, and when none can be repeated the shadow host's nonce is held uncommitted and asked again after a second or when a request is queued, never burned. Nothing throttles or limits a shadow host: its congestion window, cut-off and ejection do not apply to a probe. A shadow host absent from the escrow's session, owing proof-of-compute or with diverged escrow state is burned, as every host in that state is, because a probe cannot be served there. See [routing.md](./routing.md), "Shadow participants".

**An empty `trust` with a non-empty `shadow` routes clients to everybody except the shadow members.** That is also how an operator keeps one participant out of routing without naming everybody else.

A `POST` merges inside `participants`, one sub-key at a time: a sub-key it names replaces the stored one, a sub-key it sets to `null` is cleared, and an omitted sub-key stays. `{"participants": null}` clears all three. A `PUT` replaces the stored overrides document whole, so a `PUT` that does not resend `participants` drops it.

**One model can run its own lists.** `models.<id>.participants` takes the same three sub-keys, and a list a model names is the model's list: the network list does not apply to that model and is never joined to it, and a model that names none runs the network's. `{"participants": {"shadow": ["gonka1a"]}, "models": {"qwen": {"participants": {"shadow": []}}}}` shadows `gonka1a` on every model but `qwen`, and `{"models": {"qwen": {"participants": {"shadow": ["gonka1b"]}}}}` shadows `gonka1b` on `qwen` alone and nobody else there. A `POST` merges `models.<id>.participants` one sub-key at a time, like the network lists. The checks below run on each model's effective lists, so a model list that shadows a participant the model's effective `trust` names is refused as `models[<id>].participants.shadow`. `GET /v1/admin/state` shows under `participants` the network lists and, for each model with a block, the lists that model runs under.

**That makes the lists one `PUT` away from being cleared.** A dropped `trust` fails open: the gateway routes to everybody again without refusing anything, and a dropped `shadow` makes the shadow hosts routable. The same is true of a fresh or lost store. Read the lists back either way: the `settings written` audit line carries `participants_trust`, `participants_shadow` and `participants_unthrottled` as they stand after the call, and `participants_by_model` with each model's effective lists.

An entry must look like an address. The shape is checked when the settings document is built, so a short host label pasted from a log or a Grafana `host=` tag is refused at the settings call rather than narrowing dispatch to nobody. A participant named in both `shadow` and `trust`, or in both `shadow` and `unthrottled`, is refused with an error naming it; `trust` and `unthrottled` may name the same participant. A non-empty `shadow` is also refused, for the network and for each `models.<id>` (a `model_limits` entry included), while the effective `max_tokens_cap` is below 256: no probe can be built there.

The lists restrain nothing but routing. Host pings, catch-up, finalize, the retirement flush, the timeout sweep and the warmup prober all address the group the chain gave the escrow. See [routing.md](./routing.md), "Paths that ignore the routing lists, by design".

**Deciding whether to promote a shadow host.** Each probe ends in one `shadow probe served` or `shadow probe failed` line and in `devshard_gateway_shadow_probes_total{participant,model,outcome}`. A served probe also records `devshard_gateway_shadow_probe_first_token_seconds{participant,model}` (only when a byte streamed), `devshard_gateway_shadow_probe_duration_seconds{participant,model}` and `devshard_gateway_shadow_probe_output_tokens_total{participant,model}`. Compare them with the same participant's `devshard_gateway_participant_*` families before moving the participant from `shadow` to `trust`. A failed probe is voted as a timeout like any nonce no reply finished. The nonce is recorded in the ledger under the terminal `shadow_probe`, so it lands on neither side of a serving ratio ([accounting.md](./accounting.md), "Four rules that keep a finding honest").

## Settling an escrow that is still busy

`POST /v1/admin/devshards/{id}/settle` answers 409 `devshard busy` while requests are still spending the escrow's nonces, and for up to a minute after its timeout sweep failed a vote, so the vote is retried before finalize pays that record in full ([escrows.md](./escrows.md), "Settlement and retirement"). By then the escrow has already been parked and taken out of routing, so it is draining: calling again once it has drained settles it.

`POST /v1/admin/devshards/{id}/settle?force=true` does not wait:

```bash
curl -sS -X POST -H "Authorization: Bearer $ADMIN_KEY" "$GATEWAY/v1/admin/devshards/<id>/settle?force=true"
```

This route reads `force` from the query alone and takes no body; only the batch route below also reads it from its body. It crosses the busy check and nothing else — a second admin settle of the same escrow waits for the first to return and answers 404 `unknown devshard` if the first settled it, one the rotation lifecycle has in flight is still refused, and an escrow already settled is still not rebroadcast. **The requests still in flight are failed for their callers, and their full reserved cost is paid to the hosts that held them**, because finalize closes the door on new nonces and the records still live take the settlement default. The forced call is written to the audit log as `escrow settled under force`, with `in_flight` naming how many requests it overrode.

A forced settlement runs on an escrow that still has requests, so the one that finishes last would close the escrow's store while finalize is still collecting signatures. Finalizing, building the settlement, inspecting and the admin finalize and signature-collection routes therefore each take a settlement hold, and the draining escrow closes only after they return. The hold is counted apart from requests, so it neither makes the escrow busy nor moves routing (`registry/views.go`, `Registry.HoldSettlement`).

A settle that has started runs to its end even if the client leaves, because a settle cut short between its timeout sweep and the end of finalize would pay full reserve on the records the sweep did not reach: it runs on a context detached from the request, bounded by `adminSettleTimeout` (5 minutes; `api/admin_settle.go`). That timeout starts counting before the settle waits for the escrow's operator lock, and it does not interrupt that wait: a settle queued behind another write of the same escrow waits until that write returns, then has only what is left of the 5 minutes. A settle that does time out stops before finalize or partway through it, and the next settle resumes the finalize where it stopped.

## Settling an escrow the chain has pruned

The chain deletes an escrow a couple of epochs after the one that funded it. A settle of one it no longer holds answers 410 `escrow pruned on chain` and drops the escrow's row, so it is gone from the list afterwards; nothing is finalized or broadcast. See [escrows.md](./escrows.md), "Settlement and retirement".

## Settling a list of escrows

`POST /v1/admin/devshards/settle` with the body `{"escrow_ids": ["93033", "93040"], "batch_size": 8, "force": true}` settles each escrow the way the single-escrow route does. `force` crosses the busy check for every escrow in the list; `?force=true`, as on the single-escrow route, does the same. A repeated id is settled once.

The answer is a stream of NDJSON (`application/x-ndjson`): `200` goes out before the first settle starts, then one line per escrow the moment it finishes, in finishing order (its transaction, or the status and message the single route would have answered with), and a last line `{"settled": N, "failed": M}`. A line for a client that has left is dropped, and once it has left no further escrow of the list is started; one already settling finishes. A list the route refuses outright (no ids, too many, a bad `batch_size`) still answers a plain `400`. Read it with `curl -N`:

```bash
curl -sS -N -X POST "$GATEWAY/v1/admin/devshards/settle" \
  -H "Authorization: Bearer $ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"escrow_ids": ["93033", "93040"], "batch_size": 8, "force": true}' | jq -c
```

- **`batch_size` at a time, four by default, sixteen at most.** Building a settlement asks every host in the escrow's group for its signature, so an unbounded batch is an unbounded fan-out onto the same hosts (`api/admin_batch.go`, `defaultBatchSize`, `maxBatchSize`).
- **At most fifty per call.** Each settle runs to its end on its own context, but a caller that leaves stops the list from going on, so a list long enough to outlive the operator's patience would stop halfway, with transactions already broadcast (`batchEscrowLimit`).

## Deactivating a list of escrows

`POST /v1/admin/devshards/deactivate` with the body `{"escrow_ids": ["93033", "93040"], "batch_size": 8}` deactivates each escrow the way `/v1/admin/devshards/{id}/deactivate` does: routing stops first, then the row goes inactive. A repeated id is deactivated once, and an id the gateway has no row for answers `404` on its own line.

The answer has the shape of the settle batch: `200` NDJSON with one line per escrow in finishing order (`{"escrow_id": "93033"}`, or its `status` and `error`), then `{"deactivated": N, "failed": M}`. The same limits apply — `batch_size` four by default and sixteen at most, fifty ids per call — and a list it refuses outright answers a plain `400` (`api/admin_batch.go`, `admitBatch`).

```bash
curl -sS -N -X POST "$GATEWAY/v1/admin/devshards/deactivate" \
  -H "Authorization: Bearer $ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"escrow_ids": ["93033", "93040"]}' | jq -c
```

A registration (`POST /v1/admin/devshards`) with `activate` false deactivates an escrow already routed the same way, so its storage can be imported once its session is released.

A deactivation does not wait on the nonce ledger: the escrow's last reading is taken in memory and the ledger's own goroutine writes it out (`accounting.Service.RequestFlush`). What it can still wait on is the escrow's last diff to its hosts, up to five seconds when the escrow holds gossiped transactions no request composed (`registry/retirement_flush.go`).

## Creating an escrow the wallet cannot pay for

`POST /v1/admin/escrows` reads the wallet's spendable balance before it builds the transaction, and answers 402 naming the wallet, what it holds and what the amount plus the fee needs, without broadcasting anything.
