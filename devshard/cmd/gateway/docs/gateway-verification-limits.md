# What the test suite does not verify

The end-to-end suite (`cmd/gateway/e2e_test.go`) composes the real gateway — its engine, scheduler, registry, limits and store — with real devshard sessions talking to real in-process hosts. Real diffs, real state roots, real signatures, a real `MsgFinishInference`, no network and no chain.

This document exists because reading that suite as green and concluding "verified" would be wrong. What follows are not gaps to be closed by writing more of the same suite; they are outside what any in-process test can reach.

## Outside the reach of any in-process test

**That the real chain accepts our transactions.** Every chain response in the suite is one we wrote. Protobuf field numbers, unordered-transaction TTL semantics, gas, fee denomination, sequence handling under contention and the escrow-created event shape are all checked against *our model of the chain*, not the chain. One wrong field number is a green suite and a rejected transaction. This is the largest irreducible risk in the rewrite, and nothing in the suite touches it.

**That real hosts behave like these.** The hosts are real `host.Host` state machines, but their inference engine is a stub. A vLLM version emitting a new SSE field, a valid receipt followed by a stream truncated at an unscripted byte boundary, and TCP behaviour under packet loss are all unrepresented. The non-streaming reply is SSE-shaped because the in-process client writes SSE either way, so non-streaming *body* bytes are not evidence about a real host.

**Any tuning threshold.** Peak-EWMA, the first-token hedging trigger, outlier ejection and the AIMD window all take a latency distribution as input. "Given these numbers, this decision" is asserted in the owning packages. "These thresholds are right for the fleet" is unanswerable without production traffic.

**Real concurrency at real scale.** The widest race in the suite is single digits. Two scale hazards are known and need load to observe: `ProcessResponse` serialising a wide race on one `Session.mu`, and the registry's read lock on `Candidates`.

**Settlement money end to end.** The suite asserts the payload the gateway builds. Whether the chain then pays the right participants the right amounts is chain-side.

**Byte-for-byte compatibility with real clients.** The frames are pinned in the suite. Whether an OpenAI-SDK or aiohttp client in the field parses them as it parsed the legacy gateway's is an assumption, not a result.

## The boundary in one sentence

The suite verifies every decision the gateway makes given a set of inputs, and the wiring that makes those decisions reachable — not that our model of the chain or of a real host is correct, and not any tuning.
