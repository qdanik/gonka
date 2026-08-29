# `api` — the HTTP boundary

The only package that speaks HTTP to a client, and the one that decides what a failure looks like from outside.

## What it owns

| File | What it holds |
| --- | --- |
| `routes.go`, `server.go` | the route table and the server; every route names its own label and whether the operator kill switch exempts it |
| `middleware.go` | authentication, body limits, request ids |
| `admission.go` | the limiter slot and the token budget a request takes before it is raced |
| `stream.go` | `clientStream`: the writer the race streams through, and the fold for a non-streaming client |
| `buffer_budget.go` | the ceiling on the bytes every non-streaming reply holds at once |
| `cache.go` | the response cache, keyed on everything that varies the answer |
| `errors.go` | which error becomes which status |
| `admin*.go` | the operator surface: escrows, overrides, quarantine, accounting resets |
| `capture.go`, `inspect.go` | sampled request capture for diagnosis |
| `dispatch.go`, `timeouts.go` | the adapters that join a registry session to the race and to the vote path |

## Boundaries worth knowing

- **Our own limiter is 429; the shard being full is 503.** A quota the caller exceeded is theirs to slow down; no room on the shard is not.
- **A reply is folded as it arrives**, so a non-streaming request holds the answer being assembled rather than the stream it came from. Past the process-wide ceiling it is refused with 503 rather than held until the kernel intervenes.
- **The status is chosen from the assembled body, not before it.** A body the assembler could not fold is answered as a failure, not as a 200 carrying an error object.
- **`clientStream` is written by attempt goroutines after the handler has returned**, so every path through it takes the lock and stops once closed.
- **The admin surface is disabled entirely when no admin key is set** — including escrow creation.

## What the server is given

`Deps` is everything the boundary reads or calls. `New` requires every field except `Version` and `RequestIDs`, which default to empty and to a random id generator.

`Operations` holds the lifecycle actions the operator routes trigger. They span the chain client, the escrow manager and the registry, so the composition root implements them and this package only calls them.

`admission` is the pre-queue chain check. Relaxed proof-of-compute mode is read at three further sites: the shutdown gate in `main`, and the race's own bypass and generating checks in `engine`.

## Authentication and the kill switch

`credentials` is one request's resolved identity, computed only where an answer is used: `resolveCredentials` returns an empty value when no `Authorization` header was sent, so an unauthenticated request never reaches a key comparison at all, and a route that needs neither answer never compares a key.

`keyGate` hashes both the presented bearer and every configured key before comparing them, and compares against all of them, so the time taken does not depend on which one matched.

`requireAdmin` is where the admin key comparison lives, on the operator routes themselves rather than in a blanket middleware. With no admin key configured the whole surface answers 404.

`disabled` is the operator kill switch. Routes marked `alwaysOn` — `/metrics` and the operator surface — stay reachable through it.

## Reading a body

- `chatIngestLimit` stops a body at the socket that the filters would reject after buffering it.
- `adminIngestLimit` bounds every operator body; the largest of them is a settings patch.
- `bodyReadTimeout` is armed per request rather than as `http.Server.ReadTimeout`, which stays armed for the whole exchange: there it would expire mid-response and cancel the request a long stream is still writing for.
- `readBody` bounds ingest with `http.MaxBytesReader` rather than `io.LimitReader`: it returns a typed error the status mapper recognises, and it marks the connection so the server stops reading a hostile body instead of draining it. The read deadline is cleared again before the response begins, so only the read is bounded. A writer that neither carries a deadline nor unwraps to one reports `ErrNotSupported`, and the size bound stands alone.
- `baseWriter` walks the `Unwrap` chain down to the writer the server owns, because `MaxBytesReader` marks a connection by type-asserting its writer: handed a wrapper it cannot see through, it leaves the server draining a hostile body it has already refused.
- `HTTPServer` sets neither `ReadTimeout` nor `WriteTimeout`. Both are absolute deadlines over an exchange that outlives the request, and either would cut an SSE stream mid-answer; the body read carries its own deadline instead, armed and cleared per request in `readBody`.

## The route table

Each `route` is one registered pattern. An empty label means the route is not instrumented, which is how `/metrics` stays out of its own counters; `alwaysOn` exempts a route from the operator kill switch; `otherRouteLabel` is the single label every unmatched path folds into.

`/v1/admin/devshards/import` deliberately carries the templated `/v1/admin/devshards/{id}` label: the established series covers this path under the same name, and a label of its own would split the panel that reads it.

`handleDevshardChat` pins the race to one escrow, and refuses a pin the gateway no longer routes to.

`routableModel` refuses an unroutable model before it can take a limiter slot or an input-token budget, and fails closed on an empty registry.

## What the boundary hands the engine

`Sessions` adapts the live escrows to the engine's two outbound boundaries: the target a race dispatches through, and the poster that settles the nonces the race left unfinished. The `escrows` interface it reads has two lookups that differ on a retired escrow on purpose — see `rules.md`, "4. Routing and settlement read the escrow set asymmetrically, on purpose".

- `Target` resolves one escrow per race, because an escrow can rotate out between the pick and the send. Its release keeps the escrow draining rather than closed until the race's vote is posted.
- `Send` hands the stream on unwrapped: the transport flushes each SSE line through an `http.Flusher` assertion on it, and a wrapper would leave a crowned winner's bytes in the server's buffer. `ProcessResponse` is applied for a reply that arrived beside an error too.
- `divergedState` reads a post state root the escrow cannot accept from either side of the wire: the host reports it as a diff it could not apply, the session as a hash that differs from the local root.
- `Poster` resolves the vote poster for one escrow and the params its race committed. A retired escrow still resolves: its committed nonces have no other settlement path.
- `timeoutPayload` rebuilds what the host was asked for. A verifier re-checks the payload field by field against the record the nonce committed, so every field but the prompt is read back from that record rather than carried alongside it.

## Streaming the reply

`maxBufferedResponseBytes` tracks the stream carry cap on purpose. Without a cap at all, a host that wins the race on its first token can then send unlimited valid SSE and take the process out; and because this buffer holds the same `prompt_token_ids` frames the carry cap was derived from, lowering one without the other would refuse the large-context replies that arithmetic was meant to admit.

`writeChatHeaders` is the one place every chat reply header is set, so a cache replay and a live race cannot answer the same request differently.

`statusForAssembled` answers 502 for the bodies the assembler substitutes for an answer it could not fold: the host is upstream, and neither body carries a status of its own.

In `race`, the second `X-Devshard-ID` write is the authoritative one for a reply still holding its headers. The error from `client.Close()` is returned rather than only logged: a tail the rewriter could not finish is a failure the status can no longer carry — the reply is already 200 — and returning it is what makes the cache refuse a body that stops mid-answer. `race` reports that failure alongside the outcome for the same reason: a stream commits 200 on its first byte, so the caller needs the error itself to tell a response worth replaying from one that is not.

## Errors and statuses

- Our own limiter is a quota the caller exceeded, which is 429; capacity and a busy escrow are the shard having no room, which is 503. The old gateway drew the line in the same place.
- `writeError` renders the JSON envelope; `http.Error` would send `text/plain` instead.
- `writeControlFailure` is deliberately not `writeErrorFor`: that one answers 502 for an unrecognised error, which is wrong for a store this process owns.
- `rateLimited` singles out the gateway limiter's own rejection, because the status, the `Retry-After` header and the counter that names which cap was hit all ask for it.
- `Retry-After` is rounded up: a zero would tell a client to retry immediately, the opposite of what a queue timeout means.
- `maxLoggedErrorBytes` bounds host-controlled text in a log line, because a `HostApplicationError` with no message renders its whole upstream payload. A body truncated at that cap no longer parses, so `failureRecorder.reason` falls back to the raw text rather than an empty field.
- `adminFailure` exists because `auditAdmin` records only the successful path. Its `failureRecorder` forwards `Flush` so a wrapped handler keeps streaming.

## What a finished request records

`logRequestFinished` answers the one question a finished request can no longer be asked: how much reached the client, and whether the terminator went with it. A delivery error is the difference between a reply the client read and one it is still waiting out its own timeout for. Input tokens ride along because every escalation deadline is computed from them.

`winnerOutputTokens` is what the client actually received, which with the line's own timestamp is what a measured output rate is computed from.

`estimatePromptTokens` is the limiter's and the perf buckets' input size, not a tokenizer call.

`RaceLedger` is how the engine's single recording point reaches the ledger; nothing else writes a row.

## The views a response is built from

`filterOptions` is where the operator's configuration becomes the request-shaping rules. Forcing is expressed as its opposite there so that an `Options` built without it keeps forcing, which is what every caller that predates the switch expects.

`modelTokenLimits` closes over the per-model override map only when an operator configured one, so a deployment without overrides allocates nothing on the request path.

`devshardView` and `rotationView` keep the storage rows out of the response: without them a column rename in the store silently changes the API, and the record's own field names reach the client as written.

`devshardStatus.SessionVersion` is the protocol tag the session was created under and the one every settlement payload carries. An operator reading this endpoint has no other way to see which version an escrow is bound to, and a mismatch with the host it dispatches to is what makes a settlement unacceptable.

`capacityStatus` carries each weight under both spellings, old and new — see `operations.md`, "Reading the gateway's state".

## The operator and recovery surface

`auditAdmin` records an operator action that changed state the gateway serves or settles from. It carries the action and its subject, never the body: an override payload can hold the admin key.

`badRequestUnlessOversized` keeps an ingest-cap rejection at 413 while every other decode failure is treated as the client's malformed body.

`DevshardStoragePath` is where one escrow's session keeps its SQLite files, and sessions and the delete route must derive it the same way. `removeDevshardStorage` guards `os.RemoveAll` on a path derived from a client-supplied escrow id: it refuses any path outside the base storage dir, and the base dir itself.

`escrowInspection` is one escrow's state as an operator reads it while recovering a stuck devshard. Live and sealed inferences are counted apart because sealing drains records out of the live map, so the live tail alone reads as a whole history and is off by the escrow's entire lifetime. `unresolved` marks the inferences that have not produced a result yet — the ones holding a stuck escrow short of settlement.

`inspectable` resolves a handle for reading: a resident escrow answers from its own session, and an already-settled one is rehydrated from local storage, because "what did this escrow do" is a question asked after it stopped serving, not while.

## Request capture

`admit` passes a deterministic stride of what it sees rather than a random sample.

The sink's two failure counters mean different things. `refused` only ever grows: nothing deletes capture files, so once the directory reaches its cap the sink is off until an operator empties it, and this count is the only signal that has happened. `failed` is the other way capture goes dark — an unwritable or full directory — and is counted apart because it needs an operator, not a cleanup.

## The response cache

An entry is keyed on **everything that varies the answer**, and two of those are not obvious:

- **The logprob intent**, because the normalised body is not part of what varies: the force rules make one client's request byte-identical to another's, and the two are answered with differently stripped bodies.
- **The client's streaming and usage intents, separately from the body**, because the body now asks every host to stream and to report usage. Two callers wanting different reply shapes would otherwise share one entry, and whichever arrived first would decide what the second one got.

The caller and the escrow scope are in the key as well, so a cached reply never crosses a caller or an escrow boundary.

**One entry is bounded by what a single reply may hold**, not by the cache's own ceiling: the recorder buffers into memory per in-flight request, so bounding it by the cache would let every concurrent request hold the whole cache on its own, outside every budget that reports held bytes. A reply past that bound is dropped as it streams rather than held to the end and refused by `put`.

**The recorder keeps the first status the handler chose**, so a write after the response has begun cannot turn a recorded failure into a 200. What the bytes cannot state themselves — a client that left, and a failure a committed 200 hid — is passed to `entry` as the unstorable reason.

Eviction drops entries in map order: with a per-caller key and an hour's TTL there is no access pattern a smarter policy would reward, and map order costs nothing.

## Read next

- [`docs/request.md`](../docs/request.md) — one request from the socket to the settled nonce.
- [`docs/operations.md`](../docs/operations.md) — every route and every knob.
- [`docs/request.md`](../docs/request.md) — one request from the socket to the settled nonce.
