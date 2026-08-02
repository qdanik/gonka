# Devshard gateway — request and response filtering

`filters/` owns both boundaries: what a client may send, and what a host's answer may contain when it reaches the client. It is the only package in the gateway with no dependencies of its own, and its behaviour is pinned byte-for-byte against a golden corpus in `testdata/`.

The client-facing catalogue of accepted parameters lives in [`docs/chat-api/`](../../../../docs/chat-api/README.md). This document is the *why*: how the pipeline is shaped, which limits exist because a real host crashes without them, and which orderings are semantics rather than style.

## Why the gateway filters at all

Hosts are third-party participants running unpinned vLLM versions. A parameter the gateway forwards blindly can crash the engine, poison a CUDA context so the host fails *every subsequent request*, or stall a worker. Several of the bounds here are CVE mitigations with the CVE named in the code; a "simplification" that drops one reopens it.

The gateway also forces three fields on every request so that it can judge the answer, and strips those same fields from the response so the client never sees them.

## The pipeline

Stage order is fixed (`filters/pipeline.go`, `runPipeline`):

```
resolve routed model → select model profile → flatten extra_body →
reject unknown parameters → pre-validation rules → message normalisers →
message validation → decode token view → apply output-token limits →
post-limit rules → write the token view back → marshal
```

There are exactly two rule stages: pre-validation, before message hygiene and the token decode, and post-limits, after the output-token budget resolves. Every top-level parameter is declared **once**, in one table, with its stage and its rule chain (`filters/table.go`). The whitelist of known parameters is derived from that table at package initialisation, so a parameter cannot exist without a declaration, and an unknown key is a 400 naming the offending field — sorted, so the reported violation is deterministic when several are unknown.

Rules compose from four verbs: **reject**, **clamp** (rewrite into range), **strip** (delete the key), and **force** (overwrite unconditionally).

`extra_body` is flattened into the top level *before* the whitelist runs, an existing top-level key wins on conflict, and the envelope is dropped either way (`filters/table.go`, `unwrapExtraBody`).

### Registration order is semantics

Rules run in table order, then in chain order. Five orderings are load-bearing and each is a real defect if reversed:

| Order | Why |
|---|---|
| `logit_bias` key check **before** its value/size rules | The value rules *drop* offending entries; a malformed key must **reject**. Reversed, a bad key with an out-of-range value is deleted along with it and slips through as "nothing left to check". |
| `n` **before** `temperature` | Greedy sampling (`temperature == 0`) forces `n = 1`, and it must read the client's raw temperature before temperature's own clamp rewrites it. vLLM rejects `n > 1` under greedy. |
| `stop_token_ids` checks size **before** elements | An oversized array is rejected on size alone, without inspecting a single element. (`stop` is deliberately the other way round.) |
| `tools` **before** `tool_choice` | `tools` coerces and defaults `tool_choice`; validating it first would reject a value that is about to be rewritten. |
| `thinking` **before** `chat_template_kwargs` | Thinking mirrors into `chat_template_kwargs`, and that object's bounds must validate the *merged* result. |

## Structural bounds before anything is decoded

| Bound | Value | Reason |
|---|---|---|
| Body size | 10 MiB | A million-token prompt is 4–6 MiB of text before JSON overhead. This rejects the absurd; it does **not** throttle load — concurrent memory is bounded by the in-flight input-token budget. |
| Nesting depth | 32 | |
| Structural nodes | 250 000 | A body inside the byte cap can still decode into an order of magnitude more heap than its own bytes. |

The strip is not the same for every client. The gateway forces `logprobs` on and `top_logprobs` to a fixed value at `StagePostLimits`, because the network validates an inference against them — so by the time the response comes back, the request no longer records what the client itself asked for. `NormalizeRequest` reads that intent from the document *before* the force rules run and returns it as `LogprobIntent` (`filters/pipeline.go`, `filters/response.go`). A client that asked for logprobs gets them; one that did not loses the whole family, including a `top_logprobs` a host placed outside the logprobs object. A client that asked for logprobs but not alternatives gets `top_logprobs` present and empty, which is the shape its schema expects — removing the key would drop a field, and leaving it would hand back alternatives it never requested. The three internal fields — `token_ids`, `prompt_token_ids`, `prompt_logprobs` — are removed for everyone; no request can ask for them.

The intent is part of the response-cache key (`api/cache.go`, `cacheKeyFor`). The normalized body is not enough to separate these clients: the force rules make one request byte-identical to the other, so without the intent in the key a client that asked for logprobs is answered from an entry stripped of them.

A host can spell an internal field with a `\u` escape, which the raw-byte pre-check does not match while the client's decoder reads it back as the field itself, so any payload carrying that escape goes down the full strip regardless of what the markers find (`filters/response.go`, `hasStrippableField`). Only `\uXXXX` can encode a letter of a key, so the widened gate costs the strip path nothing on ordinary content.

The completion-to-chunks conversion carries every host-controlled field as raw JSON and decodes with the standard library (`filters/stream.go`, `sseCompletion`). A typed field is a way for a host to fail the conversion — a numeric `id`, a `created` past the float range — and a failed conversion forwards a `chat.completion` where the client renders a delta: it displays nothing while the nonce settles all the same. The re-encode turns HTML escaping off for the same reason the strip does.

The depth and node checks are one byte-wise pass outside string literals, which is cheaper than decoding first and measuring after (`filters/document.go`, `ensureStructuralBounds`). The same 10 MiB constant is what the HTTP layer hands to `MaxBytesReader`, so ingest and decode share one number.

## What the gateway forces, and the paired strip

Set on every request, present or not:

- `logprobs = true`
- `top_logprobs = 5`
- `return_token_ids = true`

The gateway needs them to classify an answer — whether the host produced content, whether it burned tokens producing nothing. The client never asked for them, so the response side removes `logprob`, `logprobs`, `top_logprobs`, `token_ids`, `prompt_token_ids` and `prompt_logprobs` at any nesting depth. The two lists move as one, because every field the gateway forces on comes back in the host's answer: a force rule added without its strip counterpart hands a client an internal field it never requested. `return_token_ids` is the one pair whose names differ — the request parameter makes vLLM emit `token_ids` (`filters/response.go`, `clientStrippedFields`).

`max_tokens` is also always written, even when the client sent neither token field. Zero always means "unset", so it resolves to the operator's default **in full**: the cap bounds what a client may ask for, not what an operator grants a client that asks for nothing, so a default above the cap is honoured rather than trimmed. A non-zero value is clamped to the cap unless the caller is an administrator, which is the only cap bypass. When both fields are present the result is the minimum of the two, mirrored back into `max_completion_tokens` only if the client sent it (`filters/rules_tokens.go`, `applyOutputTokenLimits`). Per-model overrides can replace either limit; a zero from an override means "not set for this model", so the global limit stands.

## Bounds that exist because a host dies without them

| Field | Rule | Consequence of removing it |
|---|---|---|
| `logit_bias` keys | Must parse as non-negative 32-bit integers | A key vLLM cannot read as a token id trips a device-side assert that **poisons the CUDA context**, so the host then fails every later request, not just this one (vLLM issue #16529). |
| JSON-schema `type` | Only the seven JSON-Schema primitives | Anything else crashes xgrammar's grammar compiler (CVE-2025-48944). |
| JSON-schema `pattern` | Must compile, bounded length | Same CVE. |
| `chat_template_kwargs.chat_template` | Forbidden | Arbitrary Jinja template execution (CVE-2025-61620). |
| `chat_template_kwargs.tokenize` | Forbidden | Stalls the request handler (CVE-2025-62426). |
| Grammar nesting | Active-depth cap of 200 | Unmatched opening brackets — the CVE-2026-25048 proof-of-concept shape — drive depth up with no close ever bringing it down. |
| `structural_tag` | Object form only | The JSON-encoded string form crashes the engine with an HTTP 500. |
| `$ref`, `$defs`, `definitions` | Banned in schemas | Reference expansion in the grammar compiler. |
| `top_k` | `-1` or a positive integer, capped at 262 144, coerced to an integer | Non-finite and float values reach the sampler. |

The forbidden `chat_template_kwargs` keys are those that override the chat-template helper's *positional arguments* instead of becoming template variables. `add_generation_prompt` is explicitly **not** banned, because it is a genuine template variable.

Schema bound families are kept separate per field even where the numbers currently match, so that tuning one never silently retunes another (`filters/schema_bounds.go`, the per-field bound constants). Walking happens before the size measurement, so a deep or wide attack payload never pays for a full marshal.

Two subtleties in the schema walker: data-carrying keys (`enum`, `const`, `default`, `examples`, `required`, `dependentRequired`) are *not* recursed into, because a `$ref` inside a default value is data rather than a reference; and the wrapper maps of `properties`/`patternProperties`/`dependentSchemas` are not counted as extra nodes.

## Message hygiene

The normaliser chain is fixed and order-sensitive — dropping orphaned and empty turns first keeps their malformed content from ever reaching the shape checks (`filters/messages.go`, `messageNormalizerChain`):

1. Drop `tool` messages whose `tool_call_id` has no matching prior assistant tool call.
2. Drop assistant turns with no content and no call payload — informationless placeholders some clients resend.
3. Replace empty `tool` content with the sentinel `<empty tool result>`, because vLLM chat templates require some text in every tool turn.
4. Strip the legacy `name` field from tool messages, left over from the retired `function` role.
5. Flatten single-text-part content arrays into one newline-joined string.

Validation then enforces role policies (which fields each role may and must carry), unique tool-call ids, and that every `tool` message answers a still-pending call. A role absent from the policy table is rejected rather than passed through.

One accommodation worth knowing: a `null` `tool_calls` or `function_call` is treated as absent and silently deleted, because some SDKs serialise empty slots that way.

## Model profiles

Per-model behaviour is expressed as *hooks on a profile*, and rules consult the hooks rather than comparing model identifiers (`filters/profiles.go`, `Profile`). A nil profile is the default. This replaced three different model-scoping conventions in the legacy gateway.

| Profile | Deltas |
|---|---|
| Kimi | `max_tokens` floor of 16; penalties forced to zero (fixed at zero on the wire); `structured_outputs` rejected — use `response_format`; `safety_identifier` allowed; thinking mirrored into `chat_template_kwargs`; thinking-token budget resolution. |
| MiniMax | `enable_thinking` and `thinking` stripped — there is no chat-template knob, reasoning is interleaved and structural to the template; `reasoning_split` is a native passthrough field. |
| Default (Qwen and others) | No deltas. |

One documented hole: a `thinking_token_budget` sent as a *string* fails the numeric coercion and is left completely untouched, bypassing every clamp below it (`filters/rules_reasoning.go`, `thinkingTokenBudgetResolve`). It is acknowledged in the code rather than being an oversight.

`reasoning_effort` is validated and then stripped: no route serves it today, but rejecting a valid value would break clients that send it harmlessly.

## The response side

**Non-streaming replies** are buffered whole and stripped at any nesting depth. A body that does not parse passes through unchanged rather than being dropped.

**Streaming replies** go through a stateful `StreamRewriter`, which is the part that must not be got wrong. A stateless per-chunk rewriter is what the gateway had first, and TCP does not deliver event-aligned chunks: any frame split across a read boundary was emitted verbatim, leaking exactly the token ids and log-probabilities the strip exists to hide.

The rewriter (`filters/stream.go`, `StreamRewriter`):

- Emits only complete events and holds the trailing partial.
- Terminates on both `\n\n` and `\r\n\r\n`, preferring the earliest.
- Scans incrementally from just before the previous scan's end, so a terminator straddling a chunk boundary is found without rescanning the whole carry — a byte-dripping host cannot force quadratic work.
- **Fails closed**: once the carry exceeds 32 MiB the rewriter fails permanently, every later write returns the overflow error with no output, and close returns it too.
- Joins an event's `data:` lines with a newline before parsing, the way a client does, so one object split across two lines is one object to the strip as well; drops an event whose joined payload opens as an object and does not parse, and re-emits what it kept as a single `data:` line.
- Decides both the strip and the completion-to-chunks conversion on the **parsed** payload, never on the event's raw bytes. Every event is parsed, with no byte-wise fast path: measured at 5 960 -> 7 470 ns per chunk, which is the price of there being one path rather than two that must agree. A cheap pre-check is what let an escaped key through twice, and on a path whose wall-clock is set by the model, 1.5 microseconds a chunk is not worth a second way to be wrong. The host writes those bytes: a key spelled `\u006dessage` defeats a byte-wise gate while the client's decoder reads it as `message`, and the response then reaches a streaming client as something it renders nothing from -- while the nonce settles and the caller pays for it.
- Returns a distinct truncation error when a stream ends mid-event.

The 32 MiB carry cap has arithmetic behind it, and the arithmetic depends on the gateway's own forcing: `return_token_ids` makes hosts emit `prompt_token_ids`, roughly seven bytes per prompt token, so a million-token context already needs about 6.7 MiB in a single frame. 32 MiB covers roughly 4.8 million tokens. An earlier 4 MiB cap, sized from a 131 072-token assumption, would have broken legitimate maximum-context streams from around 600 000 tokens.

### A complete reply on a streaming request is rewritten into chunks

Some hosts answer a streaming request with one whole `chat.completion` instead of a sequence of `chat.completion.chunk` events. The two shapes differ exactly where a client reads them: a completion carries `choices[].message`, a chunk carries `choices[].delta`. Forwarded as-is, an OpenAI streaming client renders nothing — while the receipt lands, the nonce settles and the money is spent. So the rewriter converts it instead of passing it on.

The conversion sits inside the same event rewrite that does the stripping (`filters/stream.go`, `rewriteEvent` and `completionAsChunks`). A substring check for `"message"` decides whether an event is worth trying at all, the strip runs first so a converted chunk can never carry the token ids the strip exists to hide, and an event whose payload does not parse is still dropped rather than converted.

Each choice is emitted as the separate chunks a real stream would have sent, in that order: the role on its own if the message named one, then everything else in the message as one delta, then a terminating chunk with an empty delta carrying `finish_reason` and `stop_reason`. A host-reported `usage` object follows as a final chunk with no choices. A JSON `null` counts as absent throughout, so a field the host spelled out as null is not re-sent as one the client has to interpret.

Conversion is deliberately best-effort. A payload that does not unmarshal into a completion, or one whose choices produce no chunk at all, falls through to the ordinary stripped event rather than costing the client an answer the race has already paid for.

### Two `data:` prefixes that must not be unified

`filters/stream.go` declares both `"data: "` (with the space, used for **emitting**) and `"data:"` (without, used for **reading**). They look like a duplication bug, and unifying them would break classification of every space-less frame — the SSE wire format allows `data:{...}`, and a reader that demanded the space would classify such a frame as carrying no data at all (`filters/stream.go`, `sseDataPrefix` and `sseDataParsePrefix`).

### Cacheability

Whether an upstream response may be cached is response policy, so it lives here (`filters/response.go`, `nonCacheableErrorMarkers`, `IsCacheableResponse` and `isCacheableErrorDetails`). A response is cacheable when it has a body, no non-cacheable error, and either a 2xx status or a cacheable 400. Seventeen error-message markers — transient, environmental and model-availability classes — make a response non-cacheable, matched case-insensitively against message, type and code. Both vLLM error shapes are decoded: the nested `{"error":{…}}` and the flat `{"object":"error",…}` one it still emits. The check is re-run on *read* as well as write, so a poisoned entry drops itself.

Capability refusals are explicitly not cacheable, because a different host may serve the same request fine.

### The vLLM capability-error parser lives here

The phrases vLLM emits for a capability refusal — the tool-choice message and the "maximum context length is " marker — are an external contract, matched in exactly one place in the tree (`filters/capability.go`, `CapabilityLimits`). It sits in `filters` because the engine may import `filters` and `filters` imports nothing from the gateway, so this is the only package both sides reach, and it is already the layer that reads upstream response bodies.

It used to exist twice, and the copies had already drifted into a bug. One took its index from a lowercased string and sliced the original — and lowercasing can *shorten* a string, since U+212A (KELVIN SIGN) lowers to a one-byte `k`. The slice then lands mid-word, so `"maximum context length is 131072 tokens"` parsed as zero, and a retriable capability refusal became a hard failure. Both the search and the slice now run on the lowered copy, and the surviving copy is the only one.

The other half of that bug: the drifted copy also mis-parsed a message whose number ends the string, so the two copies disagreed on `"maximum context length is 8192"` — one read 8192, the other 0 — and the filters copy therefore *cached* a refusal the race would have retried.

## Rejections

A rejection is a `RejectError` carrying an HTTP status, defaulting to 400, and the message is operator-facing prose that names the network, points at the API documentation and the issue tracker, and says why: some non-standard parameters can crash the vLLM engine on Gonka host nodes.

One status subtlety: `ErrorStatus` checks for the oversized-body error *first*, so a body refused for its size stays **413** even when a 400-carrying rejection wraps it (`filters/errors.go`, `ErrorStatus`). The API layer delegates to this function, so the request classifier and the filter package's own oracle are literally the same code.
