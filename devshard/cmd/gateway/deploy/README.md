# Running the devshard gateway

This is the self-hosted stack for the gateway in `devshard/cmd/gateway`. The release stack that ships with a node lives in [`deploy/join/docker-compose.devshard-gateway.yml`](../../../../deploy/join/docker-compose.devshard-gateway.yml) and pins a published image; this one defaults to a development image and is the pair to edit while working on the gateway itself.

| File | What it is |
| --- | --- |
| `docker-compose.devshard-gateway.yml` | one service, joined to the node's network |
| `config.devshard-gateway.env.template` | every variable the gateway reads, with the defaults it ships |

## Before you start

The compose file joins the external network `join_default`, which the node's own stack creates. Bring the node up first, or the gateway will refuse to start with `network join_default declared as external, but could not be found`.

## Run it

```
cp config.devshard-gateway.env.template config.devshard-gateway.env
$EDITOR config.devshard-gateway.env
docker compose -f docker-compose.devshard-gateway.yml up -d
```

The gateway answers on `127.0.0.1:18080` (`GATEWAY_HOST_PORT`), bound to loopback deliberately: the chat API is reachable from the host but not from the network. Check it with

```
curl -s localhost:18080/v1/status
```

which is the same route the container's healthcheck uses, so `docker compose ps` reporting `healthy` means the same thing.

## What you must fill in

Nothing has a working default for these:

- `GATEWAY_CHAIN_RPC`, `GATEWAY_CHAIN_GRPC`, `GATEWAY_CHAIN_ID`, `GATEWAY_PUBLIC_API` — the chain the gateway reads and broadcasts to. A wrong `GATEWAY_CHAIN_ID` invalidates every transaction it signs.
- `GATEWAY_API_KEYS` — the keys clients present. Empty leaves the chat API open to anyone who can reach the port.
- `GATEWAY_ADMIN_API_KEY` — empty disables the admin routes entirely, including creating escrows. Set it, or serve only what `GATEWAY_ESCROWS_JSON` seeds.

## Signing keys are named, never pasted

`GATEWAY_ESCROWS_JSON` seeds the escrows to serve at startup, and each entry names the **variable** that holds the key rather than the key:

```json
[{"escrow_id": "63362", "private_key_env": "GATEWAY_PRIVATE_KEY", "model": "deepseek-ai/DeepSeek-V4-Flash-0731"}]
```

The gateway then reads `GATEWAY_PRIVATE_KEY` from its environment. This is why the admin API takes a variable name too: a key pasted into a request body would reach the logs, the shell history and the audit trail. Leave `GATEWAY_ESCROWS_JSON` empty to start with no escrows and add them through the admin API.

## Storage

`GATEWAY_STORAGE_HOST_DIR` (default `.devshard-gateway`, next to the compose file) is mounted at `GATEWAY_STORAGE_DIR` in the container. It holds the escrow database, the per-escrow session state and the nonce ledger. Losing it loses the gateway's memory of the escrows it serves, so back it up or point it somewhere durable before running anything that matters.

## Tuning without a redeploy

Most limits are also runtime overrides through the admin API, so the values here are the boot defaults rather than the last word. Which keys are live and what each one means is in [gateway-operations.md](../docs/gateway-operations.md), under "Operator".

Two worth knowing before the first run:

- `GATEWAY_NONCE_ACCOUNTING_ENABLED` ships as `false` and the built-in default is also off. The old gateway had its ledger **on**, so an operator porting a config gets no counters, no findings and no `accounting.db` — without an error. Turn it on unless you mean to run blind.
- `GATEWAY_POC_MODE=relaxed` serves through the chain phase that otherwise blocks new inferences. It is the right setting for a gateway that must keep answering across an epoch boundary, and the wrong one if you want the chain's own admission to hold.
