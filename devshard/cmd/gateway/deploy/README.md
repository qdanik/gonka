# Running the devshard gateway

This directory holds the self-hosted Docker Compose stack for the gateway in `devshard/cmd/gateway`. Follow the steps below to bring up one gateway next to a node; the settings behind them are described in [`docs/configuration.md`](../docs/configuration.md).

| File | What it is |
| --- | --- |
| `docker-compose.devshard-gateway.yml` | one service, joined to the node's network |
| `docker-compose.devshard-gateway.bluegreen.yml` | two gateway colours (blue, green) behind nginx, for upgrades without downtime |
| `nginx/` | the proxy config, and one upstream file per colour |
| `upgrade.sh` | the upgrade command for the blue-green stack |
| `config.devshard-gateway.env.template` | the variables this stack sets, with the defaults it ships; the full list the gateway reads is [`env/env.go`](../env/env.go) |

## Run it

1. Bring the node's own stack up first. The compose file joins the external network `join_default`, which the node's stack creates; without it the gateway refuses to start with `network join_default declared as external, but could not be found`.

2. Copy the template and fill it in ("What you must fill in"):

   ```bash
   cp config.devshard-gateway.env.template config.devshard-gateway.env
   $EDITOR config.devshard-gateway.env
   ```

3. Choose the image and start the container. `GATEWAY_IMAGE` is a full image reference with its tag; building one is in [`docs/runbook.md`](../docs/runbook.md), "Release and rollback":

   ```bash
   export GATEWAY_IMAGE=<registry>/<image>:<tag>
   docker compose -f docker-compose.devshard-gateway.yml up -d
   ```

4. Check that it answers. The gateway listens on `127.0.0.1:18080` (`GATEWAY_HOST_PORT`), bound to loopback so the chat API is reachable from the host but not from the network:

   ```bash
   curl -s localhost:18080/v1/status
   ```

## What you must fill in

These variables have no working default:

| Variable | What it is |
| --- | --- |
| `DEVSHARD_CHAIN_RPC`, `DEVSHARD_CHAIN_GRPC`, `DEVSHARD_CHAIN_ID`, `DEVSHARD_PUBLIC_API` | the chain the gateway reads and broadcasts to. A wrong `DEVSHARD_CHAIN_ID` invalidates every transaction it signs. The gateway runs with `DEVSHARD_CHAIN_GRPC` unreachable as long as `DEVSHARD_CHAIN_RPC` points at a live CometBFT RPC |
| `DEVSHARD_ADMIN_API_KEY` | the admin key. Empty disables the admin routes entirely, including creating escrows, setting `access` and creating client keys |

Client keys are not an environment setting, bar the legacy variable below: after the first start, create each with `POST /v1/admin/api-keys` for the models whose `access` under `models` (or the network `access`) is `api_key`. A fresh gateway serves no client until the operator sets `access` for a model under `models` and, for an `api_key` model, creates a key: a model with no `access` of its own answers only the admin key ([`docs/operations.md`](../docs/operations.md), "Who may call what", "Client API keys").

## Legacy inputs

- Legacy `DEVSHARD_API_KEYS` (deprecated, removal planned for v7 or v8): the variable is still read, comma-separated and each key at least 16 characters, and its keys are active beside the database keys; create them through `POST /v1/admin/api-keys` instead. The list and a read by id show them with `source` `env`, no value and no `created_at`, and they cannot be revoked or created again through the API (409): to remove one, move its clients to a new key, then unset the variable.
- Legacy `model_limits` (deprecated, removal planned for v7 or v8): the settings key is still accepted and read as `models.<id>` with the same field names; set `models.<id>` instead, which wins field by field, and a boot or a write holding the key logs a warning.
- Legacy `model_access` (deprecated, removal planned for v7 or v8): the settings key is still read as `models.<id>.access`; set `models.<id>.access` instead, which wins when both name the model, and a boot or a write holding the key logs a warning.

## Signing keys are named, never pasted

`DEVSHARDS_JSON` seeds the escrows to serve at startup, and each entry names the variable that holds the key rather than the key:

```json
[{"id": "63362", "private_key_env": "DEVSHARD_PRIVATE_KEY", "model": "deepseek-ai/DeepSeek-V4-Flash-0731"}]
```

The gateway then reads `DEVSHARD_PRIVATE_KEY` from its environment. The admin API takes a variable name for the same reason: a key pasted into a request body would reach the logs, the shell history and the audit trail. Leave `DEVSHARDS_JSON` empty to start with no escrows and add them through the admin API.

## Storage

`DEVSHARD_STORAGE_HOST_DIR` (default `.devshard-gateway`, next to the compose file) is mounted at `DEVSHARD_STORAGE_DIR` in the container. It holds the escrow database, the per-escrow session state and the nonce ledger. Losing it loses the gateway's memory of the escrows it serves, so point it somewhere durable and back it up ([`docs/runbook.md`](../docs/runbook.md), "Backup and restore").

## What the healthcheck means

The container's healthcheck probes `/healthz`, which answers 200 without checking anything else and stays up through the kill switch. `docker compose ps` reporting `healthy` therefore means the process is up and answering, not that it is serving chat.

| Route | Why the healthcheck does not use it |
| --- | --- |
| `/v1/status` | every probe would build the status of every escrow. It also does not say whether chat is served: its `requests_blocked` reports the chain's admission gate, while a disabled gateway answers 503, or 308 with a redirect set, on the chat routes |
| `/metrics` | every probe would be a full scrape |

## Settings to decide before the first run

Most limits in the template are boot defaults that the admin API can override at runtime without a redeploy ([`docs/configuration.md`](../docs/configuration.md), "The three layers"). Two settings decide behaviour from the first request:

| Variable | Shipped value | Effect |
| --- | --- | --- |
| `DEVSHARD_STATS_ENABLED` | `false`, which is also the built-in default | with it off there are no counters, no findings and no `accounting.db`, and no error says so |
| `DEVSHARD_POC_REQUEST_MODE` | `relaxed`, which is also the built-in default | the gateway serves through the chain phase that otherwise blocks new inferences. Set `off` where the chain's own admission must hold |

## Upgrading without downtime

`docker-compose.devshard-gateway.bluegreen.yml` replaces the single gateway with two, blue and green, behind nginx on `127.0.0.1:18080` (`GATEWAY_HOST_PORT`). The idle one is stopped. An upgrade starts it on the new image, moves the escrows over, switches nginx and stops the old one; the full order and what each step guards against is in [`docs/runbook.md`](../docs/runbook.md), "Upgrade". The single-gateway file above is unchanged and keeps working on its own.

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

The script reads `DEVSHARD_ADMIN_API_KEY` from `DEVSHARD_ENV_FILE` (default `./config.devshard-gateway.env`) and hands it to curl through a descriptor, so it is in no argument list and no output. If a step before the switch fails, the new colour is stopped, the old one is given its escrows, settings and nginx config back and keeps serving; an activation the old colour refuses with 409 because the earlier session is still held is retried until it is accepted or `--drain-timeout` ends, and an escrow it never accepts is logged and left inactive. After the switch nothing is rolled back; the script checks that the new colour is alive before it switches nginx, after it, and before it stops the old one or says the new colour serves, prints the escrows already imported and those still on the old colour, and exits non-zero if the new colour is not alive. The rest are moved by hand, and a dead new colour is recovered, as described in [`docs/runbook.md`](../docs/runbook.md), "Upgrade".

What clients notice during an upgrade:

- a model with two or more escrows runs on one escrow fewer while each of its escrows moves, and can still answer `409 pinned escrow gone` or `503 no escrow capacity`;
- a model with one escrow answers 503 when it is named in `model_access` or `model_limits`, and 400 otherwise, from its deactivation until wave 1 has imported it and nginx has switched: the time its running requests take to end, seconds on a healthy fleet and longer when a paused host holds one, plus `refusal_timeout` (60 s by chain default) only when a refusal vote is owed, as with a failing host, and the copy;
- a cold cache and cold host performance estimates on the new colour;
- 404 from `/v1/requests/{id}` for a request served before the upgrade.

The handover itself costs nothing when every model has two or more active escrows: the session files are copied and no chain transaction is sent. The import carries the rotation role, rotation epoch and route prefix of an escrow, so turning rotation on again funds nothing new for it; see "Upgrade" in the [runbook](../docs/runbook.md). A model with one escrow has a gap, which one extra escrow created before the upgrade closes, at the price of one create fee per model. Rotation is off for the handover, so it creates nothing in between.

Upgrade only through the script: a plain `docker compose up -d` would recreate a colour on `GATEWAY_IMAGE` or the default tag rather than the one it runs. The old colour's storage stays in `data/<colour>` until the next upgrade moves it to `data/<colour>.retired-<time>`; delete those directories by hand once you no longer need them. nginx is started with its defaults for open connections after a reload (`worker_shutdown_timeout` is unset), so a stream open at the switch is served to its end. nginx resolves a colour's address when it loads its config: if the active colour's container is recreated with a new address, reload nginx (`docker compose -f docker-compose.devshard-gateway.bluegreen.yml exec nginx nginx -s reload`).
