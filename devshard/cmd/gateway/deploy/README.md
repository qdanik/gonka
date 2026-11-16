# Running the devshard gateway

This is the self-hosted stack for the gateway in `devshard/cmd/gateway`.

| File | What it is |
| --- | --- |
| `docker-compose.devshard-gateway.yml` | one service, joined to the node's network |
| `docker-compose.devshard-gateway.bluegreen.yml` | two gateway colours (blue, green) behind nginx, for upgrades without downtime |
| `nginx/` | the proxy config, and one upstream file per colour |
| `upgrade.sh` | the upgrade command for the blue-green stack |
| `config.devshard-gateway.env.template` | the variables this stack sets, with the defaults it ships; the full list the gateway reads is [`env/env.go`](../env/env.go) |

## Before you start

The compose file joins the external network `join_default`, which the node's own stack creates. Bring the node up first, or the gateway will refuse to start with `network join_default declared as external, but could not be found`.

## Run it

```
cp config.devshard-gateway.env.template config.devshard-gateway.env
$EDITOR config.devshard-gateway.env
docker compose -f docker-compose.devshard-gateway.yml up -d
```

The gateway answers on `127.0.0.1:18080` (`GATEWAY_HOST_PORT`), bound to loopback by design: the chat API is reachable from the host but not from the network. Check it with

```
curl -s localhost:18080/v1/status
```

The container's healthcheck deliberately probes a *different* route, `/healthz`, which the kill switch leaves up and which answers 200 without checking anything else. `/v1/status` stays up through the kill switch too, so a gateway an operator had disabled on purpose is not restarted either way, but every probe of it would build the status of every escrow. Pointed at `/metrics`, every probe would be a full scrape. So `docker compose ps` reporting `healthy` means the process is up and answering, not that it is serving chat. `/v1/status` does not answer that alone either: its `requests_blocked` reports the chain's admission gate, while a disabled gateway answers 503 (or 308, with a redirect set) on the chat routes.

## What you must fill in

Nothing has a working default for these:

- `DEVSHARD_CHAIN_RPC`, `DEVSHARD_CHAIN_GRPC`, `DEVSHARD_CHAIN_ID`, `DEVSHARD_PUBLIC_API` — the chain the gateway reads and broadcasts to. A wrong `DEVSHARD_CHAIN_ID` invalidates every transaction it signs.
- `DEVSHARD_API_KEYS` — the keys clients present, each at least 16 characters, for the models the `model_access` admin override marks `api_key`. A model that override does not name answers only the admin key, so a fresh gateway serves no client until the operator sets it ([`docs/operations.md`](../docs/operations.md), "Who may call what").
- `DEVSHARD_ADMIN_API_KEY` — empty disables the admin routes entirely, including creating escrows and setting `model_access`, so every model stays admin-only and the gateway serves no client. Set it.

## Signing keys are named, never pasted

`DEVSHARDS_JSON` seeds the escrows to serve at startup, and each entry names the **variable** that holds the key rather than the key:

```json
[{"id": "63362", "private_key_env": "DEVSHARD_PRIVATE_KEY", "model": "deepseek-ai/DeepSeek-V4-Flash-0731"}]
```

The gateway then reads `DEVSHARD_PRIVATE_KEY` from its environment. This is why the admin API takes a variable name too: a key pasted into a request body would reach the logs, the shell history and the audit trail. Leave `DEVSHARDS_JSON` empty to start with no escrows and add them through the admin API.

## Storage

`DEVSHARD_STORAGE_HOST_DIR` (default `.devshard-gateway`, next to the compose file) is mounted at `DEVSHARD_STORAGE_DIR` in the container. It holds the escrow database, the per-escrow session state and the nonce ledger. Losing it loses the gateway's memory of the escrows it serves, so back it up or point it somewhere durable before running anything that matters. How to take a backup and restore one is in [`docs/operations.md`](../docs/operations.md), "Backup and restore"; how to roll back a release, in "Release and rollback".

## Tuning without a redeploy

Most limits are also runtime overrides through the admin API, so the values here are the boot defaults rather than the last word. Which keys are live and what each one means is in [operations.md](../docs/operations.md), under "Configuration".

Two settings decide behaviour before the first run:

- `DEVSHARD_STATS_ENABLED` ships as `false` and the built-in default is also off: a config that never sets it gets no counters, no findings and no `accounting.db`, without an error.
- `DEVSHARD_POC_REQUEST_MODE` ships as `relaxed` and the built-in default is also relaxed: the gateway serves through the chain phase that otherwise blocks new inferences. It is the right setting for a gateway that must keep answering across an epoch boundary; set `off` where the chain's own admission must hold.

## Upgrading without downtime

`docker-compose.devshard-gateway.bluegreen.yml` replaces the single gateway with two, blue and green, behind nginx on `127.0.0.1:18080` (`GATEWAY_HOST_PORT`). The idle one is stopped. An upgrade starts it on the new image, moves the escrows over, switches nginx and stops the old one; the full order and what each step guards against is in [`docs/operations.md`](../docs/operations.md), "Upgrade". The single-gateway file above is unchanged and keeps working on its own.

The stack needs Docker Compose 2.24 or later, `curl` and `jq` on the host, and the same `config.devshard-gateway.env` as the single gateway. Two settings are fixed in the compose file over the env file: `DEVSHARDS_JSON` is empty and `DEVSHARD_ESCROW_ROTATION_ENABLED` is false. The first would let a restarted colour publish escrows the other still holds, and rotation is switched by the admin setting `rotation_enabled`, which the store keeps.

First start, with blue (the colours have separate storage under `data/`; to adopt a single gateway's storage, move its directory to `data/blue`):

```
mkdir -p data/blue nginx-active
cp nginx/colour-blue.conf nginx-active/active.conf
docker compose -f docker-compose.devshard-gateway.bluegreen.yml up -d gateway-blue nginx
curl -X POST -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" -d '{"rotation_enabled": true}' 127.0.0.1:18081/v1/admin/settings
```

Escrows are added through the admin API as for a single gateway, against blue's own port (`127.0.0.1:18081`, `GATEWAY_BLUE_HOST_PORT`; green is `18082`, `GATEWAY_GREEN_HOST_PORT`) or through nginx. The stats port (`DEVSHARD_STATS_PORT`, published by the single stack as `DEVSHARD_STATS_HOST_PORT`) is not published in this stack; add a `ports` entry to the colour that needs it.

The upgrade, from `devshard/cmd/gateway`:

```
make upgrade UPGRADE_IMAGE=<registry>/<image>:<tag>
```

or from this directory `./upgrade.sh --image <registry>/<image>:<tag>`. The flags:

- `--drain-timeout SECONDS`, default 900: how long an escrow may take to release its session on the old colour, how long the old colour has to finish its open requests and post the refusal votes it owes before it is stopped, and how long a rollback keeps retrying an activation the old colour refuses while it still holds the session. The wait is the longest running request plus, when a refusal vote is owed, the chain's `refusal_timeout` (60 s by default), not the execution timeout: execution votes not yet due travel with the escrow and the new colour posts them. A stream still open when the time runs out is cut, and a vote still owed is not posted, so finalization charges its nonces in full.
- `--ready-timeout SECONDS`, default 300: how long the new colour has to answer `/v1/models`, and nginx to report it after the reload.
- `--skip-parked`: leave escrows parked for settlement on the old colour, which then keeps running to settle them and is not stopped; without the flag the upgrade refuses while any exists.

The script reads `DEVSHARD_ADMIN_API_KEY` from `DEVSHARD_ENV_FILE` (default `./config.devshard-gateway.env`) and hands it to curl through a descriptor, so it is in no argument list and no output. If a step before the switch fails, the new colour is stopped, the old one is given its escrows, settings and nginx config back and keeps serving; an activation the old colour refuses with 409 because the earlier session is still held is retried until it is accepted or `--drain-timeout` ends, and an escrow it never accepts is logged and left inactive. After the switch nothing is rolled back; the script checks that the new colour is alive before it switches nginx, after it, and before it stops the old one or says the new colour serves, prints the escrows already imported and those still on the old colour, and exits non-zero if the new colour is not alive. The rest are moved by hand, and a dead new colour is recovered, as described in [`docs/operations.md`](../docs/operations.md), "Upgrade".

What clients notice during an upgrade:

- a model with two or more escrows runs on one escrow fewer while each of its escrows moves, and can still answer `409 pinned escrow gone` or `503 no escrow capacity`;
- a model with one escrow answers 503 when it is named in `model_access` or `model_limits`, and 400 otherwise, from its deactivation until wave 1 has imported it and nginx has switched: the time its running requests take to end, seconds on a healthy fleet and longer when a paused host holds one, plus `refusal_timeout` (60 s by chain default) only when a refusal vote is owed, as with a failing host, and the copy;
- a cold cache and cold host performance estimates on the new colour;
- 404 from `/v1/requests/{id}` for a request served before the upgrade.

The handover itself costs nothing when every model has two or more active escrows: the session files are copied and no chain transaction is sent. The import carries the rotation role, rotation epoch and route prefix of an escrow, so turning rotation on again funds nothing new for it; see "Upgrade" in the operations guide. A model with one escrow has a gap, which one extra escrow created before the upgrade closes, at the price of one create fee per model. Rotation is off for the handover, so it creates nothing in between.

Upgrade only through the script: a plain `docker compose up -d` would recreate a colour on `GATEWAY_IMAGE` or the default tag rather than the one it runs. The old colour's storage stays in `data/<colour>` until the next upgrade moves it to `data/<colour>.retired-<time>`; delete those directories by hand once you no longer need them. nginx is started with its defaults for open connections after a reload (`worker_shutdown_timeout` is unset), so a stream open at the switch is served to its end. nginx resolves a colour's address when it loads its config: if the active colour's container is recreated with a new address, reload nginx (`docker compose -f docker-compose.devshard-gateway.bluegreen.yml exec nginx nginx -s reload`).
