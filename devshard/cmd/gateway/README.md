# devshard gateway

An OpenAI-compatible chat-completions endpoint in front of a decentralised inference network. A client sends an ordinary chat request; the gateway spends a blockchain-committed **nonce** on it, races it across several hosts, streams back the first real answer, and settles every nonce it spent — including the ones that served nobody.

The hard part is not proxying. It is that **every nonce costs the escrow money whether or not anyone was served**, so a request that fails still has protocol obligations, and a gateway that forgets one pays for silence.

## Start here

| If you want to | Read |
| --- | --- |
| run it | [`deploy/README.md`](./deploy/README.md) |
| understand how it fits together | [`Architecture.md`](./Architecture.md) |
| follow one request end to end | [`docs/request.md`](./docs/request.md) |
| change one layer | that layer's own `README.md` — see the table below |
| operate it | [`docs/gateway-operations.md`](./docs/gateway-operations.md) |

## The layers

| Package | What it owns |
| --- | --- |
| [`filters/`](./filters/) | the request and response boundary: parameter rules, model profiles, response folding and stripping |
| [`api/`](./api/) | HTTP: routes, auth, admission, cache, the client writer, error mapping, the admin surface |
| [`scheduler/`](./scheduler/) | which escrow, which host, which nonce — and burning the nonces nobody can serve |
| [`engine/`](./engine/) | the speculative race: attempts, escalation, crowning, streaming, and the votes the losers owe |
| [`registry/`](./registry/) | the live escrow set and the sessions that dispatch on it |
| [`escrow/`](./escrow/) | escrow creation, rotation, depletion, settlement, retirement, crash recovery |
| [`chain/`](./chain/) | all blockchain input and output, and the epoch phase snapshot |
| [`limits/`](./limits/) | admission, the per-host AIMD window, and the chain-weight capacity model |
| [`perf/`](./perf/) | per-host history, outlier ejection, capability refusal counts |
| [`accounting/`](./accounting/) | the per-nonce ledger and the findings derived from it |
| [`nonces/`](./nonces/) | what feeds that ledger: live events, chain diffs, the sweep |
| [`burns/`](./burns/) | charging a host for a nonce it refused, after probing it with a real request |
| [`warmup/`](./warmup/) | teaching a newly published escrow to its own group |
| [`store/`](./store/) | control-plane state in SQLite |
| [`config/`](./config/) | the immutable configuration snapshot and its atomic holder |
| [`env/`](./env/) | the only place environment variables are read |
| [`metrics/`](./metrics/) | the Prometheus registry and every collector |

The package root itself is the composition root: `main.go` wires the above, and its neighbours hold the escrow records this process owns and the admin operations over them.

## Working on it

```
go build ./...
go test ./... -count=1
golangci-lint run ./...
```

The tests are the specification. Where a rule exists because something went wrong once, the test that pins it says so in its name — `TestAWinnerCrownedAfterTheClientLeftIsNotLabelledUserVisible` is a bug report that cannot rot.

## Cross-cutting documents

These describe behaviour that no single package owns:

- [`docs/request.md`](./docs/request.md) — one request from socket to settled nonce, and every rule applied on the way
- [`docs/gateway-speculative-race.md`](./docs/gateway-speculative-race.md) — the race, the crown, the drain
- [`docs/gateway-routing-and-nonces.md`](./docs/gateway-routing-and-nonces.md) — the drain loop, the host gates, every burn reason
- [`docs/escrows.md`](./docs/escrows.md) — escrow states, rotation, depletion, settlement, draining
- [`docs/gateway-capacity-and-health.md`](./docs/gateway-capacity-and-health.md) — admission, ejection, buffered replies
- [`docs/gateway-request-filtering.md`](./docs/gateway-request-filtering.md) — every parameter rule and bound
- [`docs/accounting-findings.md`](./docs/accounting-findings.md) — every finding and what to do about it
- [`docs/gateway-invariants.md`](./docs/gateway-invariants.md) — what must never stop being true
- [`docs/gateway-non-goals.md`](./docs/gateway-non-goals.md) — what this gateway deliberately does not do
