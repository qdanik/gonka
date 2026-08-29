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
| operate it | [`docs/operations.md`](./docs/operations.md) |

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

## The composition root

Five files, and what each is for: `main.go` wires everything, `lifecycle.go` starts and stops it, `devshards.go` turns stored rows into live escrow sessions, `observers.go` adapts one event into the several readers that want it, and `operations.go` is the admin surface behind [`api`](./api/).

### Wiring order, and the knots in it

Most of `compose` is a straight line. Four places are not:

- **Logging is configured before anything else can log.** A collector reads JSON fields as labels; the default text line carries `log`'s own date prefix and would have to be re-parsed.
- **`serve` owns the signal context**, so releasing it survives a panic. `os.Exit` skips a `defer` left in `main`.
- **Routing joins the escrow set to the picker through the capacity model.** An escrow whose membership never reaches that model scores as weightless, is skipped by every pick, and serves nothing — so the join is not optional wiring. The warmup is handed the registry *after* the registry exists, because the registry publishes to it; and a nil warmup must never be assigned into the interface field, since a typed nil there is non-nil to a nil check.
- **The capture and cache collectors are registered after the server**, which owns both sinks. Nothing evicts capture files, so the refusal count is the only signal that capture has turned itself off at its byte cap.

### Two readers of one fact

Three small adapters exist because two subsystems want the same event for different reasons, and neither belongs inside the other:

- **`nonceAccountedRaces`** hands one race outcome to both readers of it. The metrics recorder asks how the fleet performed; the ledger asks where each nonce went. The engine should know about neither. The warmup and the burn charge vote through the same poster and observer the race already uses.
- **`tracedDispatches`** narrates the dispatch events an operator would otherwise have to infer from a counter's slope, and forwards every one to the recorder. It wraps rather than living inside [`metrics`](./metrics/) because counting and narrating are different jobs, and it keeps the scheduler free of a logger. A burned nonce is logged with its nonce and *never labelled with it* — a counter keyed by nonce would grow without end.
- **`phaseNarrator`** turns the observer's five-second poll into a line only when something an operator cares about actually changed. Subscribing without it would write the same snapshot twelve times a minute.

### Relaxed mode, in one place

`modelCapacity` is one wrapper for both capacity readers, because the gauge must report the scale that admission actually applies, and only this type knows the operator's relaxed-mode override of the chain's raw blocking state.

Reading the chain's raw state instead would zero the scale during PoC, and a zero scale clamps every weight-derived cap to nothing — so relaxed mode would go dead in exactly the deployments that set an input-token cap or a per-model override, which are the ones that need it. `api.admission` folds the same override in for the pre-queue gate.

The per-weight *allowance* is the exception: it follows the raw chain phase rather than the override, because it bounds what the hosts can actually do.

### Shutdown

Nine steps, in a fixed order, described in [`docs/operations.md`](./docs/operations.md), "Shutdown". Every step runs even after an earlier one fails, except a step marked as needing a quiesced system — that one is skipped and the skip reported, because it destroys state the steps above it may still be using. A drain step is bounded by the shutdown budget without cancelling the work inside it.

Two positions in that order are load-bearing:

- **Nonce accounting closes after every emitter above it has stopped**, so the final snapshot holds the counters the run ended with rather than one taken while races were still classifying nonces.
- **Public-API connections close last.** Every step above can still reach the public API, and an idle socket closed under one of them is a socket the next poll has to re-dial. The chain's own gRPC connection is *not* closed here and cannot be: `common/chain` owns it and exposes no `Close`, so it lives until the process exits. That is why the tests ignore its goroutines rather than waiting for them.

Boot has a matching budget: the concurrent-build limit and the idle connection pool those builds reuse are sized together, so the pool is neither starved nor larger than the builders can use.

### Escrow sessions and the chain connection

`chainBackedSessions` owns the chain connection because it is the only provider that needs one — an in-process provider dials nothing, which is what keeps a test from carrying a live gRPC client. `sessionSources` is a parameter rather than a constant so the transport an escrow is served over is chosen once, here; the chain access travels with the sessions because it rides the same connection, and `Reader` and `Transport` are interfaces so a provider that dials nothing can answer them in process.

- **The bridge is one object for the process.** It holds the chain client every session reads escrow state through, so building one per session would open a connection per escrow and lose the client's cache.
- **Production does not use upstream's `NewGRPCBridgeFromURL`**, which is its test constructor. The bridge is built over a client carrying the CometBFT RPC query fallback, so an escrow read survives the gRPC query path failing. An empty RPC endpoint lets `common/chain` derive one from the gRPC host at the standard port, which is how a default deployment is laid out — a deployment that moved it has to say so, or the fallback resolves to a host nobody is listening on and dies silently.
- **Seeding leaves a devshard it already knows alone**, so a restart cannot resurrect one an operator deactivated.
- **Publication follows the store's `active` flag**, builders at a time; an escrow whose key or record is missing is marked inactive rather than failing the boot. A write to a devshard row wakes a republish, because the rotation lifecycle owns those rows and knows nothing about the registry. `depletionNotice` breaks the reverse cycle: the manager settles through the registry, so it cannot also be constructed before it.
- **Importing an escrow copies its storage rather than referencing it**, so the gateway owns the only handle to what it serves. Only regular files are copied — session storage is a flat set of SQLite files, so a directory below it is not part of the escrow.

### Admin operations

`suspiciousHosts` is the operator's manual pin list, cached in memory because every race reads it and backed by the store because a pin outlives the incident that prompted it. **The store is written first**: a pin the gateway acts on but forgets on restart is the failure an operator cannot see.

- **`Deactivate` and `Settle` stop routing before the row changes**, so nothing new is admitted while the write runs.
- **`Activate` refuses an escrow parked for settlement.** Its balance is already committed to a settlement that is either on chain or on its way, and serving from it again spends nonces that settlement does not account for.
- **Every key is named, never carried.** `CreateEscrow` takes the name of the variable holding the signing key. See [`docs/operations.md`](./docs/operations.md), "What is exposed".

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
- [`docs/race.md`](./docs/race.md) — the race, the crown, the drain
- [`docs/routing.md`](./docs/routing.md) — the drain loop, the host gates, every burn reason
- [`docs/escrows.md`](./docs/escrows.md) — escrow states, rotation, depletion, settlement, draining
- [`docs/capacity.md`](./docs/capacity.md) — admission, ejection, buffered replies
- [`docs/accounting.md`](./docs/accounting.md) — the nonce ledger: its vocabulary, its surface, every finding and what to do about it
- [`docs/rules.md`](./docs/rules.md) — the invariants, the deliberate non-goals, and what the tests do not verify
