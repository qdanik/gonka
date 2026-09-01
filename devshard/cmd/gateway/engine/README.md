# `engine` — the speculative race

One client request, several attempts on different hosts, one winner. This package owns that race from the first pick to the vote that settles the last losing nonce.

## What it owns

| File | What it holds |
| --- | --- |
| `race.go` | the coordinator: the event loop, the exits, and the outcome it reports once |
| `pick.go` | asking the scheduler for a host and launching an attempt on it |
| `attempt.go` | one attempt's life: dispatch, receipt, chunks, terminal |
| `escalation.go` | when to start another attempt — the deadline ladder measured from the host's own history |
| `crown.go`, `stream.go` | crowning the first attempt to produce content, and forwarding only its bytes |
| `deadline.go`, `drain.go` | the timers, and the barrier that outlives the client |
| `classify.go`, `outcome.go` | what the attempt ended as, in the vocabulary the ledger admits |
| `settle.go`, `session.go` | the timeout vote every unfinished nonce owes |
| `reassembly.go`, `carry.go` | rebuilding events split across chunk boundaries |
| `vocabulary.go` | the wire strings — metric labels, log fields, ledger reasons — declared once |

## What it does not own

It does not choose the escrow or commit the nonce — that is [`scheduler`](../scheduler/). It does not shape the request or the reply — that is [`filters`](../filters/). It does not write the ledger — it reports one outcome, and [`nonces`](../nonces/) records it.

## Boundaries

- **A losing attempt is not a free attempt.** Its nonce is committed and costs the escrow; the race is not over until every one of them has been settled or reported.
- **The race outlives the client.** A client that hangs up does not cancel the attempts, because their nonces still owe votes. The drain barrier is what makes shutdown wait for them.
- **The outcome is reported exactly once**, from whichever goroutine ends the race.
- **An SSE error event counts as a chunk but never crowns.** A host that answers with an error has answered something, but not content.

## From pick to report

`begin` picks under `schedulerPickTimeout`, because the race context never cancels and a scheduler waiting for capacity would otherwise hang. It resolves the escrow's `DispatchTarget`, reads the phase snapshot for the PoC bypass and the attempt budget, asks the policy for a `StartPlan`, launches the primary, and starts however many more immediate attempts the plan asked for.

`await` then loops: settle held crown claims, compute the next deadline, and block on attempt events, a finished pick, a crown claim, the timer, or the client leaving. It exits when nothing is pending, no pick is running, and no escalation is armed — an armed escalation outlives the last pending attempt, because a race whose every attempt failed is owed another.

There are three exits. `exitComplete` ends the race. `exitWinnerServed` and `exitClientGone` hand the still-pending losers to a second `await` on another goroutine, which reports; the outcome the caller gets back is the client's view, and the reported one may be finished later.

A nonce the race can no longer spend is **stranded**, not dropped: it is committed, paid for and answered by nobody, so it enters the attempt list with `TerminalNoReceipt` and is carried into the timeout plan.

## Who owns which state

- `liveAttempt` is written only by the coordinator, from `AttemptEvent`s and from its own deadline firings.
- `attemptState` is owned by the attempt's own goroutine and never read by another; every fact the race needs travels as an `AttemptEvent` instead. `AttemptSpec` is fixed at construction and never written afterwards.
- The event channel is buffered (`eventBuffer`) so chunk progress does not park a host's goroutine. `emit` blocks — terminal events must arrive — while `offer` drops progress the coordinator is too busy to take. `expire` drains the queue before reading `lastChunk`, so a dropped progress event only ever ages it between reads. Every select arm that reads race state rather than adding to it starts with `catchUp` for the same reason.
- Events must be drained until every started attempt has delivered its `AttemptDone`. See rules.md, "1. A committed nonce is always settled".

## Escalation and the deadline ladder

`EscalationPolicy` is pure over its arguments: no host performance, no phase snapshot, no clock of its own. `nextDeadline` takes the earliest armed deadline, with the declaration order of `deadlineTrigger` breaking exact ties.

An `ArmedEscalation` is a deadline to arm, not a permission to escalate; only `Confirm` converts it, by re-deriving the same stage at the same deadline. The attempt's `escalated` flag is consumed before the pick starts, so a pick that finds no host cannot retry the same trigger.

Escalation is disarmed entirely when a pick is already running (that pick *is* the escalation), when the client has left (another attempt is another nonce to settle for a response nobody will read), when an attempt has been crowned, or when the budget is spent.

The rungs, in `triggerFor` order: an already-escalated attempt never arms; a suspicious host arms immediately; a done attempt whose nonce is finished never arms, and any other done attempt arms immediately; an attempt with no receipt arms at `SendTime + receiptTimeout`; an attempt with a first token never arms; everything else arms on the first-token curve.

- **Receipt timeout doubles above `receiptTimeoutDoubleAboveTokens`** — admitting a very large prompt is itself work.
- **The first-token curve** is the measured fit over prompt size: a fixed cost to start answering (`firstTokenBaseSeconds`), a per-token cost to read the prompt, and a quadratic term that only matters on very large prompts. It is floored at `FirstTokenFloor` and capped at `FirstTokenCeiling`, because uncapped the quadratic outgrows the backstop that cancels the attempt; a zero ceiling means the operator removed the cap.
- **A host's own history only ever extends the budget.** `firstTokenBudget` uses the observed first-content p75 (× `firstTokenObservedSlack / 2`) only while it is within `firstTokenObservedLimit` × the curve and longer than the curve; a host slower than that keeps the curve, because waiting out its own history would delay the attempt that rescues the request. The quantile is read once per attempt, since `plan()` runs on every event and it cannot move mid-attempt.
- **The curve is measured from dispatch**, but a receipt that used more than the curve allows would leave the rung already due, so the deadline is never earlier than `ReceiptTime + FirstTokenFloor`: the host owes a first token, not the time its receipt took.

### Backstops are not tunable

`streamingHardTimeout` and `schedulerPickTimeout` bound a request every tunable already failed to bound. The streaming backstop is 20 minutes, but gives way if the chain's own execution deadline ever moves below it — a stream held past that deadline is work nobody can be paid for.

Not tunable still means not tunable **by an operator**. `Deps.E2E` can shorten the streaming backstop, and nothing but a declared end-to-end stand can fill it: see [`docs/rules.md`](../docs/rules.md), "What a test stand may reach".

## Crowning and the client's bytes

`winnerWriter` is one attempt's claim on the client stream. `client` is the only path to the client anywhere in the writer, and `withheld` is the only source it is ever assigned from, so a losing attempt has no reachable sink at all rather than a sink guarded by a branch somebody must remember to write.

- `Write` buffers every pre-content chunk and claims the crown on the first content chunk; a suppressed attempt reports success without writing. Past `filters.MaxStreamCarryBytes` the buffered prefix is dropped, never the attempt.
- `claim` blocks on the coordinator's single answer, and abandons if the race ends first. `Abandon` clears the verdict, the prefix and both writer fields: an attempt that ends without earning the crown must never have what it buffered forwarded.
- `Flush` needs no gate of its own — an uncrowned attempt has no client to reach. `attemptWriter.Flush` is how the transport's per-line flush reaches the client at all: without it the assertion the transport makes on its writer fails and a crowned winner's bytes sit in the server's buffer.
- `contentGate` hands the sink beside it the one fact an `io.Writer` signature cannot carry. `Classify` is called immediately before the `Write` of the same chunk, on the same goroutine.

The coordinator answers claims in `answer`: an unknown nonce or an already-crowned race is suppressed, a **suspicious host's claim is held rather than refused**, because a refusal is permanent, and anything else is crowned on the spot. `settleClaims` answers the held claims once the race can tell whether a rival will serve — a rival being a pending attempt beyond the claimants, a running pick, or an immediate attempt still owed. Alone, a suspicious host is crowned. `crownWinner` is the single place one attempt becomes the client's answer, so the reason travels with it into the log.

### Crown denial

`crownStrikes` withholds the crown from a host that answers without content, while leaving it in the scheduler's rotation. `crownDenialStrikes` content-free answers deny it; a content-bearing answer removes the entry, and entries are otherwise never evicted (see rules.md, "9. Bounded by construction"). `suspicionGate` folds the operator's manual never-trust-this-host pins into the same gate.

Only attempts that say something about the host are observed: it answered with content, or it claimed to serve and produced none. A dial failure, a stranded nonce or a cancelled client says neither, and clearing the host's strikes on one would hand it a clean record it did not earn.

## Classification and reassembly

`carryBudget` bounds the bytes held for SSE reassembly at three levels — attempt, participant and global. `reserve` charges the participant first and then the global pool, undoing the participant charge when the global pool trips. A participant's counter is created on first use and never removed (rules.md, "9. Bounded by construction").

`carryBuffer` reassembles events across chunk boundaries for one attempt and belongs to that attempt's goroutine alone; only the byte charge it holds is shared. `Take` returns every newline-terminated event in the held fragment plus the chunk and retains the rest; a cap trip releases the fragment and yields the raw chunk instead, so classification degrades to whatever parses on its own rather than stopping, and the first such trip is reported once as a classify overflow. `Tail` is the unterminated final fragment: a stream whose last event arrives without a newline is classified from it by `Flush`, or it reads as having produced nothing.

What the classifier reads out of an event:

- **Content** names the field carrying the first client-renderable output. `choices[].text` is excluded: the gateway serves only `/v1/chat/completions`, where a host emitting it renders nothing. A `stop` with host-reported completion tokens and no output counts as content only on a thinking-budget route, which is also the only route whose `completion_tokens` can separate a model that produced nothing from a host that carried nothing.
- **Errors** are extracted in both the nested `{"error":{...}}` shape and the flat `{"object":"error",...}` one vLLM emits, without a byte-wise pre-filter: the host writes these bytes, and a key spelled with a `\u` escape would pass the scan while the decoder reads it as the error it is, leaving the attempt classified as something it is not. The parse a pre-filter saves does not offset that misclassification.
- **Usage** is pre-filtered on the `"usage"` key, which is sound because vLLM emits the usage object once per stream, so the check skips almost every chunk.
- **A capability refusal is kept out of `Error`** and travels as `CapabilityRefused` instead, so a different host can still serve it; its message still reaches the perf recorder.

## Outcome, samples and verdicts

`Terminal` is an attempt's classified end state, and every downstream vocabulary — limiter verdict, perf sample, metric label — is a total function of it. `terminalStatuses` is the one table linking an upstream status to the terminal it produces: `StatusFor` reads it forward for metrics and dispatch-error classification reads the derived inverse, so the two cannot disagree about which status a label describes. `Terminal.String` falls back to `reason()` rather than keeping a second list, so the failure names cannot drift apart; `reason()` is empty for the two outcomes that are not failures, which `failureReason` relies on to fall through.

`racedTerminal` is applied by the coordinator because an attempt goroutine knows only its own cancellation, not the crown, the silence or the backstop: a cancelled attempt becomes a hard timeout when the backstop fired on it, stalled when it went silent mid-content or when the hosts abandoned the race, and won when it is the crowned attempt. An attempt the race stopped listening to still gets an `unreportedOutcome`, otherwise its spent nonce leaves nothing downstream can see. `phaseAborted` is asked with the phase the coordinator sees, so the log line and the ladders cannot disagree; a no-receipt attempt never reached the host's queue, so a phase transition cannot be what ended it.

### The exemption ladder

`sampleExemption` is the ordered set of reasons an attempt contributes no perf sample and therefore never counts toward its host's ejection: never dispatched, never reported (the race stopped listening, so judging it would charge a host for the gateway's own cancellation), phase aborted, error stream or capability refusal, state divergent, long response, empty stream under a PoC bypass, empty stream with no winner, and finally client-cancelled.

`Verdict` is a second ladder and **disagrees with the sample ladder at client cancellation** — the race cancels its own losers. It also turns an empty stream held past `emptyStreamHeldTooLong` into an overload verdict: below that an empty stream is the model's output, at or above it the host held the request past the refusal point and returned nothing.

`responsive` decides whether a host earns a positive sample: confirmed, nonce finished, and not an empty stream. A long non-stream reply is exempt from being judged slow, but an empty one earns nothing — crediting a host that held the request for the whole window and returned no content teaches the router to prefer it. The long-response exemption gates on `ContentSource`, not `ContentChunks`, which counts error events too (rules.md, "1. A committed nonce is always settled").

### Measurements

- `IsWinner` is the one test for "this attempt won the race". Whether a client was still there to receive it is `Lifecycle.ClientGone`, which visibility reads: the race outlives the client.
- `ClockOffset` compares the executor's stamp against the midpoint of the send-to-receipt window rather than the dispatch, which would charge the host for the outbound leg; half a second is added back because the executor stamps whole seconds downward.
- `TimePerOutputToken` starts at the first content chunk, so prefill is not charged to decode speed.
- `MaxChunkGap` keeps the longest silence a host left mid-stream, which a chunk count cannot show. The silence before `[DONE]` is the end of the stream, not a host that went quiet. `MeanChunkGap` is the inverse of the delivered rate.
- A served attempt's label reason is blanked, except under `VisibilityNoWinner`: blanking it there renders the panel's headline case as "unknown".

## Timeout votes

Every nonce the race did not leave settled owes a chain vote. `TimeoutStep.StartedAt` is the race's start, not the attempt's dispatch: verifiers recompute a refusal deadline from the committed record, so every nonce a request commits must carry the one stamp, dispatched or stranded.

`timeoutSkipReason` names every skip — phase aborted, empty stream with a finished nonce, finished nonce, long response. A host whose escrow state diverged is not one of them. `SettleTimeouts` emits a started event and then a completed one; a step nobody will attempt is emitted only as skipped, because a started event with no completion following reads as a hung settle.

`Deps.Timeouts` is resolved per race rather than held, because escrows rotate, and it is handed the request params because the vote must carry the prompt the committed record keeps only as a hash.

`SettleTimeout` reads the handler's own record of whether the vote reached the escrow state: the handler returns a non-nil error on its success path too — that error carries "the inference timed out" to the request — so the error alone cannot tell a settled vote from an unsettled one. `TimeoutOutcome` prefers the handler's own detail over the generic collection error, because that is the only place the refusing verifier is named; `escrowMissing` is the caller's reading, since vote collection reports a count and never the verifier's error.

## The Stop barrier and the escrow hold

`Engine.Stop` refuses new races and then waits: it returns once every race it admitted has posted the vote that settles its nonces. `admit` registers a race under the lock before it starts, and returns nil once stopped. A panicking race still owes the barrier its release, and the panic itself is re-raised unchanged.

`raceRegistration` is that place in the barrier and the holder of the escrow's in-flight count. Releasing is idempotent because the settling path and a panicking request path both reach for it and only one may be counted. It keeps the *first* hold for as long as the race's vote is owed — every hold a race is offered counts the same escrow, so a later one goes straight back rather than displacing the one being kept — and `launch` gives the commit's own hold back immediately for that reason.

Facts the race observes about the escrow but must not act on travel in `Lifecycle` and are handed to the lifecycle observer on the response path, which must mark and return rather than reach the chain. The escrow-missing mark is made before the ledger row is queued, so an observer that has seen the row has also seen the mark.

## Errors the engine returns

- `ErrStopped` — the engine can no longer see a request through to a settled nonce.
- `ErrWinnerIncomplete` — the crowned attempt's bytes are already on the wire, so no other attempt's payload can be put in their place.
- `ErrEmptyStream` — every attempt ended empty.
- `ErrAllAttemptsFailed` — nothing above applied.
- `HostApplicationError` — an upstream refusal the client must see verbatim: the host answered, and its answer is the response. `hostError` prefers the crowned attempt's refusal, because a host chosen to answer is the one whose answer the client asked for.
- `StatusForError` maps a host application error to its own status, and an upstream throttle or unavailability to 429.

## Read next

- [`docs/race.md`](../docs/race.md) — the ladder, the crown, the drain, and why each is shaped that way.
