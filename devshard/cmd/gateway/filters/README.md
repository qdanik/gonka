# `filters` — the request and response boundary

Everything a client sends is normalised here before it reaches a host, and everything a host returns is folded and stripped here before it reaches a client. This is the only package in the gateway with no dependency on any other: it is pure, and its behaviour is pinned byte-for-byte against the goldens in `testdata/`.

## What it owns

**The request side.**
- `table.go` — one rule table, one row per top-level chat-completion parameter, each naming the stage it runs in and what it does. A parameter absent from the table is rejected: the gateway does not forward what it has not been taught.
- `profiles.go`, `profile_*.go` — per-model divergences, kept next to the rule they change rather than scattered through it.
- `rules_*.go` — the rule bodies: types, ranges, arrays, schemas, token budgets, reasoning controls.
- `messages.go` — message hygiene, which is separate from parameter rules because a message is a nested document.

**The response side.**
- `fold.go` — `BodyFolder` folds the host's SSE stream into one JSON body **as chunks arrive**, stripping the fields the client must not see before merging rather than after. A client that did not ask for logprobs never accumulates them.
- `stream.go` — the rewriter for a streaming client, which does the same strip per event on the way out.
- `response.go` — which fields are stripped, and which a client can ask back.
- `assemble.go` — the older whole-body fold, now the reference implementation the incremental one is verified against.

## Boundaries worth knowing

- **The gateway forces `stream: true` upstream** even when the client did not ask for it, so both sides of this package are always in play.
- **Three fields are never returned to anyone**: `token_ids`, `prompt_token_ids`, `prompt_logprobs`. Three more are returned only if asked: `logprob`, `logprobs`, `top_logprobs`.
- **The strip list is derived, not written twice.** A field added to what is stripped cannot be left out of what is always stripped — which is exactly how `top_logprobs` once leaked.
- **A malformed body passes through unchanged rather than being dropped**, except where a host writes `NaN`/`Infinity` as barewords, which are normalised so the body is inspectable at all.

## Read next

- [`docs/gateway-request-filtering.md`](../docs/gateway-request-filtering.md) — every rule, every stage, and the arithmetic behind the bounds.
