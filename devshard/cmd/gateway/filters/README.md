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

## The request pipeline

`NormalizeRequest` parses the body into a `Document`, then `runPipeline` walks it in a fixed order:

1. Resolve the routed model — the body's `model` when it is a non-blank string, otherwise `Options.RoutedModel` — and pick that model's `*Profile`.
2. Unwrap `extra_body`.
3. Reject any parameter absent from the table.
4. Run every `StagePreValidation` rule.
5. Normalise the messages, then validate them.
6. Decode the typed `requestView`: `model`, `stream`, `max_tokens`, `max_completion_tokens`, `n`.
7. Resolve the output-token limits and write them back into the document.
8. Read the client's `LogprobIntent`.
9. Run every `StagePostLimits` rule.
10. Re-decode the view, keeping the token fields step 7 resolved.
11. Read `stream_options.include_usage` as the client's usage intent.
12. Force streaming upstream, unless `Options.KeepClientStream` is set.
13. Marshal.

Step 8 has to sit where it does: `StagePostLimits` forces `logprobs` on for validation, so after it the document says what the gateway wants, not what the client asked for.

### Registration order is semantics

Rules run in `parameterTable` order, so where a row sits is behaviour rather than style:

- `logit_bias`'s key check runs before its value and size rules.
- `tools` runs before `tool_choice`, because `validTools` coerces a `"required"` choice and can delete both fields outright.
- `thinking` runs before `chat_template_kwargs`, so a mirrored value is already in place when the kwargs bounds are checked.

### `extra_body` and the whitelist

`unwrapExtraBody` flattens an `extra_body` envelope into top-level fields before the whitelist runs. An existing top-level key wins on conflict, a nested `extra_body` key is ignored, and the envelope itself is dropped either way.

`rejectUnknownParameters` then rejects any key absent from `parameterTable`. The unknown keys are sorted and only the first is reported, so the rejection is deterministic; a key with an empty name gets its own message. The rejection text names the reason — a non-standard parameter can crash the vLLM engine on a host — and points at the network's chat-API docs and issue tracker.

### Streaming is forced upstream

`forceUpstreamStreaming` sets `stream: true` and `stream_options: {"include_usage": true}` on every host request unless `KeepClientStream` says otherwise. The forced `stream_options` map is shared rather than rebuilt per request: the document is marshalled and dropped straight after, so nothing can mutate it.

`Result.ClientStream` and `Result.ClientUsage` carry what the client actually asked for, which is what the response side needs in order to undo the forcing.

## Structural bounds before the decode

Three bounds run before anything is decoded: `MaxBodyBytes` (10 MiB) on the raw body, `MaxNestingDepth` (32), and `MaxStructuralNodes` (250,000) counting containers and elements.

`ensureStructuralBounds` scans the body byte by byte in a single pass, tracking string literals and escapes so that braces and commas inside a string are not counted. A closing bracket never drives the depth below zero, so an unbalanced body is bounded rather than rejected here — the decoder that follows is what rejects it.

## Which JSON decoder, and why

The package uses two: `encoding/json` from the standard library, and `github.com/goccy/go-json`. Each site picks one deliberately.

| Site | Decoder | Why |
| --- | --- | --- |
| `ParseDocument` | standard library | Its error text goes to the client verbatim, and the golden corpus pins that wording against the gateway this replaces. A faster library with different phrasing would change what every malformed request sees. |
| `stripInternalFields`, `decodeStreamedEvent`, `completionAsChunks` | standard library | goccy parses a number token even into a `json.RawMessage` and errors past the float64 range. That fails the strip open — a body carrying `1e999` would keep every internal field — and fails the chunk conversion on a `created` out of range. |
| `Document.Marshal`, `DecodeUpstreamError`, `jsonMarshaledSize` | goccy | Neither the error text nor the range behaviour is load-bearing here, and `Marshal` sorts map keys, so the normalised body is deterministic. |

Every decode that matters uses `UseNumber`, so a number survives as its literal rather than as a float64.

Encoding goes through `encodeCompact`, which uses an `Encoder` rather than `Marshal` for one reason: only the encoder can call `SetEscapeHTML(false)`, and the default would inflate every `<`, `>` and `&` the model generated to six bytes. The newline `Encode` appends is trimmed off, since it is not part of the value. `growingText.MarshalJSON` goes through it for the same reason. `jsonMarshaledSize` measures with a counting writer instead of allocating the output, subtracting the one byte that newline adds.

## Parameter rules

Type rules differ on what an explicit `null` means. `requireUint` and `requireBool` treat a null like an absent field and pass it through; `requireString` and `validModelName` reject it, because a null is not a string.

Numeric rules run through `sanitizeFloatField`, which coerces the field to float64 and *deletes* it when it is absent, unparseable, or non-finite — so `NaN` and `Inf` never reach a host.

- `clampFloat` clamps into `[min, max]` and writes the result back. It backs `temperature` (0–2), `min_p` (0–1), and both penalties (-2–2).
- `rejectNonPositiveThenClamp` clamps down to the maximum and *then* rejects a non-positive result. The order is the point: an exclusive lower bound cannot be enforced by clamping without producing an illegal value. It backs `top_p` and `repetition_penalty`.
- `validTopK` accepts only `-1` (disabled) or a value at or above 1, then clamps down to 262144 and truncates toward zero.

Collection rules:

- `validListLength` bounds an array's length and, optionally, the length of its string elements. `messages` is capped at 2048 entries, `stop` at 16 entries of 256 bytes, `bad_words` at 64 entries of 128 bytes.
- `requireStringElements` rejects the first non-string element, reported as `<param>[<index>]`.
- `dropBlankStringListElements` removes whitespace-only strings from `bad_words` and drops the field when nothing survives.
- `validFloatMap` rejects `logit_bias` outright past 1024 raw entries, then drops individual entries outside `[-100, 100]` or non-finite, and drops the field once none survive.
- `requireTokenIDKeys` rejects a `logit_bias` key that is not a non-negative 32-bit integer (vLLM #16529). The lexicographically first offender is reported, so the message is stable across map iteration order.
- `validMetadata` enforces the OpenAI-compatible contract: an object, at most 16 keys, keys up to 64 bytes, string values up to 512 bytes.
- `validStreamOptions` strips the field entirely unless `stream` is exactly `true`, then keeps only `include_usage` and drops the field when nothing survives.

What the gateway forces, and what it strips:

- `logprobs` → `true`, `top_logprobs` → `completionapi.ForcedTopLogprobs`, `return_token_ids` → `true`. Each is forced for validation and paired with a response-side strip, a pairing a test enforces.
- `n` is replaced with `1` when present: the reservation budgets one `max_tokens` worth of output, and `n` choices can produce `n` times what it signed for.
- Silently stripped: `service_tier`, `store`, `provider`, `plugins`, `prompt_cache_key`, `cache_key`, `extra_headers`, `thinking_config`, `think`, `stop_token_ids`.
- `model` must match `^[A-Za-z0-9._/-]+$` and stay under 256 bytes; `user` and `safety_identifier` under 512.

## Output token limits

`DefaultRequestMaxTokens` (3072) and `RequestMaxTokensCap` (4096) are the package defaults, and also the single source `config.Defaults` reads. `Options` may override either globally, and `Options.ModelTokenLimits` may override either per routed model — a zero returned there means "not set for this model" and leaves the global one alone.

`capOutputTokens` treats zero as "the client named no budget": it returns the configured default, which the cap deliberately does not clamp. A nonzero value is clamped to the cap unless `Options.Admin` bypasses it.

`applyOutputTokenLimits` then resolves one number from whichever fields the client sent — the minimum of both when both are present — floors it at `completionapi.MinTokensFloor`, and writes it to `max_tokens`. It mirrors the result into `max_completion_tokens` only when the client sent that field, so the gateway does not introduce a field the request never carried. `min_tokens` is set to the requested value raised to the floor and capped at the resolved `max_tokens`.

`rejectNonPositiveOutputTokens` rejects a present, numeric, non-positive `max_tokens` or `max_completion_tokens` on every route: a zero output budget makes no answer, and the redundancy layer then waits out a winner that cannot come.

## Model profiles

A `*Profile` is one routed model's set of deltas from the default pipeline. A nil profile *is* the default — Qwen and anything unrecognised — and `ProfileFor` returns it for any model no registered profile claims.

| Model | Deltas |
| --- | --- |
| `moonshotai/Kimi-K2.6` | Zero penalties forced, `structured_outputs` rejected, `safety_identifier` allowed, thinking mirrored into `chat_template_kwargs`, owns a `thinking_token_budget` resolution. |
| `MiniMaxAI/MiniMax-M2.7` | Thinking fields stripped, `reasoning_split` kept. |
| `deepseek-ai/DeepSeek-V4-Flash-0731` | `reasoning_effort` defaults to `"max"`. |

`ThinkingDisposition` is the closed set of ways a profile handles `thinking`/`enable_thinking`: normalise in place (the default), mirror into `chat_template_kwargs`, or strip entirely.

## Reasoning and thinking

`reasoning` is a wrapper the gateway does not forward: `reasoningWrapper` deletes it, and lifts `.effort` into a not-yet-present `reasoning_effort` — unless the wrapper carries `enabled: false`, which becomes `reasoning_effort: "none"` instead.

`reasoning_effort` is validated against a closed enum (`none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`) and then scoped separately: a profile with no `ReasoningEffortDefault` has the field deleted, and a profile with one gets it filled in when the client sent nothing.

`enable_thinking` and `thinking` both depend on the profile's disposition:

- `ThinkingStrip` deletes the field — the model has no matching chat-template knob.
- `ThinkingMirrorToKwargs` moves the boolean into `chat_template_kwargs`, preserving a value already nested there and always removing the top-level field.
- Otherwise `thinking.type` is normalised in place to `"enabled"` or `"disabled"`, and the `display` hint is dropped. `adaptive` and `auto` both resolve to enabled: they signal opt-in thinking with an SDK-chosen budget.

`thinkingTokenBudgetResolve` runs after the token limits and clamps whatever budget the request carries, for every model, so content keeps room to be written: down to `thinkingBudgetAbsoluteMax` (96,000), and down to `max_tokens - thinkingBudgetContentHeadroom` (64). Only a profile that declares `ThinkingTokenBudget` gets a budget *invented* — `max_tokens / 2` — because a host on the V2 model runner rejects the field outright, and V2 is the default for every non-MoE model.

### Silencing Kimi's reasoning

Below `kimiThinkingBudgetForceZeroBelow` (256) output tokens, a profile that owns the budget gets `thinking_token_budget: 0` and `chat_template_kwargs.thinking: false`. That second write overwrites rather than fills, unlike every other mirror: the budget alone is a logits processor, which speculative decoding discards, and the `thinking` rule has already mirrored the caller's answer into the kwargs. A request that cannot afford thinking cannot afford it in the template either.

### The rest of the profile-scoped rules

- `safetyIdentifier` validates and keeps the field for a profile with `AllowSafetyIdentifier`, and strips it for every other.
- `reasoningSplit` strips the field for profiles that cannot serve it, and fills it with `true` for the one that can: M2.x thinks unconditionally, so without the split its reasoning arrives inline in `content`.
- `forceZeroPenalty` overwrites `frequency_penalty` and `presence_penalty` to 0 for a `ForceZeroPenalties` profile, but only when the field is already present — it never introduces one.

## Schema bounds

Four fields carry a nested payload, and each has its own bound family, kept separate even where the values currently match: `tools`, `response_format`, `structured_outputs`, and `chat_template_kwargs`.

`SchemaBounds.Check` walks a JSON-Schema payload before measuring its serialised size, and enforces:

- depth (16), node count (128 or 256), serialised size (16 KiB), branch arms per `anyOf`/`oneOf`/`allOf` (16), and `enum` size (256).
- `$ref`, `$defs` and `definitions` are forbidden outright.
- `type` must be a JSON-Schema primitive, or an array of them; anything else crashes xgrammar's grammar compiler (CVE-2025-48944).
- `pattern` must be a string, under 512 bytes, and must compile (CVE-2025-48944).

The walk distinguishes two key families. `schemaDataKeys` — `enum`, `const`, `default`, `examples`, `required`, `dependentRequired` — hold literal data, not child schemas, so the walker must not recurse into them. `schemaChildMapKeys` — `properties`, `patternProperties`, `dependentSchemas` — hold name-to-schema maps: each value is walked as its own child schema, and the wrapper map itself is not counted as an extra node.

`ObjectBounds` is the plain-object counterpart used for `chat_template_kwargs`: depth, node count and size only, with no `$ref` ban and no `type`/`pattern`/`enum`/branch semantics, since that payload feeds a Jinja renderer rather than a grammar compiler. Both walkers share `depthExceededError` and `nodesExceededError` so they render identical rejections.

### `tools` and `tool_choice`

`validTools` enforces the OpenAI tool contract — every entry an object of `type: "function"` with a non-empty `function.name` — and does the cross-field cleanup that `tool_choice` depends on: `"required"` collapses to `"auto"`, an empty `tools` array deletes both fields, an absent choice defaults to `"auto"`, and `function.strict` is stripped silently. `validToolChoice` then accepts only `"auto"`, `"none"`, or a function object with a name under 64 bytes; `"required"` is coerced upstream and never reaches it.

### `response_format`

`text` and `json_object` pass as-is. `json_schema` additionally requires a name matching `^[A-Za-z0-9_.-]+$` and under 64 bytes, plus a schema object that clears `SchemaBounds`.

### `structured_outputs`

The six constraint fields — `json`, `regex`, `choice`, `grammar`, `json_object`, `structural_tag` — are mutually exclusive per vLLM's `StructuredOutputsParams.__post_init__`: exactly one must be set, and the rejection names the ones that were. Three auxiliary fields may accompany any of them (`whitespace_pattern`, `disable_any_whitespace`, `disable_additional_properties`), two private backend fields (`_backend`, `_backend_was_auto`) are stripped silently, and any other sub-field is rejected. The field cannot be combined with `response_format`, and a profile with `RejectStructuredOutput` rejects it outright because it has no matching route.

Each constraint pairs with its own validator in a map, so a new constraint field cannot be added without one — a coverage test checks the pairing. Per-constraint caps:

- `json` must be an object, not a string-encoded schema, and clears `SchemaBounds`.
- `regex` and `whitespace_pattern` share one validator: string, under `MaxPatternLen`, and must compile.
- `choice` is a non-empty string array, at most 256 entries, each under 1024 bytes, with a total under the shared size cap.
- `grammar` is capped at 8 KiB and 200 levels of nesting. The check tracks *active* bracket depth: unmatched opens, the CVE-2026-25048 proof-of-concept shape, drive the depth up without a close ever bringing it down.
- `structural_tag` accepts only the object form (vLLM's `StructuralTagResponseFormat`) under 4 KiB; a JSON-encoded string form crashes the engine with an HTTP 500.

### `chat_template_kwargs`

Forbidden keys are rejected before the object bounds run. They are the ones that override `apply_hf_chat_template`'s positional arguments instead of becoming template variables: `chat_template` (CVE-2025-61620, arbitrary Jinja template), `tokenize` (CVE-2025-62426, stalls the request handler), plus `tools`, `documents`, `conversation`, `continue_final_message`, `padding`, `truncation`, `max_length`, `return_tensors`, `return_dict`. `add_generation_prompt` is deliberately not banned.

## Message hygiene

Normalisation runs before validation, as a fixed and order-sensitive chain:

1. `dropOrphanToolMessages` — removes a `role: "tool"` entry whose `tool_call_id` has no matching prior assistant `tool_call`, tracking pending ids exactly the way `validateMessages` does.
2. `dropEmptyAssistantTurns` — removes an assistant message with no content and no `tool_calls`/`function_call`: an informationless placeholder some clients resend.
3. `normalizeEmptyMessageContent` — fills empty `role: "tool"` content with `<empty tool result>`, because vLLM chat templates require some text in every tool turn, and nullifies empty assistant content that carries a call payload.
4. `stripLegacyToolName` — drops the `name` field from `role: "tool"` messages, a leftover from the retired `role: "function"` shape.
5. `flattenMessageTextParts` — joins a content array of `{type: "text", text}` parts into one newline-separated string. Any other shape is left alone for validation to reject.

Validation then requires a non-empty `messages` array, and keys every message on its role. A role missing from `messageRolePolicies` is rejected, which makes the policy map the closed set of accepted roles.

| Role | Disallowed fields | Also required |
| --- | --- | --- |
| `developer`, `system`, `user` | `tool_calls`, `tool_call_id`, `function_call` | non-empty content |
| `assistant` | `tool_call_id` | content, unless `tool_calls` or `function_call` is present |
| `tool` | `tool_calls`, `function_call` | a `tool_call_id` matching a previous assistant call, and content |
| `function` | `tool_calls`, `tool_call_id`, `function_call` | a `name`, and content |

An explicit `null` counts as present for the disallowed-field check. Inside `tool_calls` and `function_call`, though, a null value is treated as absent and silently removed — some SDKs serialise empty slots that way. `tool_calls` ids must be unique, and every entry must be `type: "function"` with a `name`.

Content is accepted as a non-blank string, or as a non-empty array of `{type: "text", text}` parts. `isEmptyContent` reports a blank string or a zero-length parts array; anything else, `nil` included, is not empty — a missing field is a different failure from an empty one.

## Response stripping

`LogprobIntent` is what the client's own request asked for, read before the force rules overwrite it. Without it the strip cannot tell a client who asked for logprobs from one who did not, and would answer both by removing them.

- A client that asked for nothing loses `clientStrippedFields`: the whole logprob family plus `token_ids`, `prompt_token_ids`, `prompt_logprobs`.
- A client that asked for logprobs loses only `alwaysStrippedFields`, which is *derived* by subtracting `requestableFields` from the full list. A hand-written second list is exactly how `top_logprobs` once leaked.
- A client that asked for logprobs but not alternatives keeps the key with an empty array: that is the shape OpenAI returns. Leaving the forced alternatives in would hand the client a request it never made, and removing the key would drop a field its schema expects to be present.

`decodeLogprobIntent` is deliberately lenient: only an explicit `true` counts as a request, so a value of the wrong shape reads as "not asked" rather than rejecting a request the gateway would otherwise have accepted — the force rules overwrite both fields regardless of type.

### Non-finite barewords

A backend writes `NaN`, `Infinity` and `-Infinity` as barewords for a probability of zero or an overflow. None is valid JSON, so a body carrying one parses nowhere: the buffered path would forward it with every internal field intact, and the streaming path would drop the event and the client's answer with it.

`replaceNonFiniteNumbers` rewrites them to `null` outside string literals. It returns `ok = false` when the body carries none, so the ordinary path allocates nothing, and it matches the longest literal first so `-Infinity` is not read as a minus sign followed by `Infinity`.

When that rescue fires, the caller must receive the re-encoded bytes even if no field was deleted. Handed the original, everything downstream meets the barewords again: the completion-to-chunks conversion fails on them and forwards a response a streaming client renders nothing from, while the attempt is crowned on its content and the nonce is paid for.

### Cacheability

`IsCacheableResponse` stores a success whose payload carries no failure, or a deterministic client-input error (an HTTP 400 with a parseable error body). `HasNonCacheableError` is the same check run on read, so a poisoned entry drops itself.

An error is *not* cacheable when its message, type, or code contains one of `nonCacheableErrorMarkers` — cancellation, timeouts, rate limits, overload, unavailability, or model-availability failures — or when it is a host-capability failure, since a different host may serve it fine.

`parseUpstreamErrorDetails` reads the error from plain JSON or from inside an SSE data event, and `DecodeUpstreamError` accepts both the nested `{"error":{...}}` shape and the flat `{"object":"error",...}` one vLLM still emits. The SSE scan does not stop at the first decodable event: an empty `{"error":{}}` decodes while carrying nothing, and stopping there would leave a real error in a later event unseen and read the stream as cacheable. A null `code` renders as absent rather than the literal text `<nil>`.

### Capability errors

`CapabilityLimits` parses vLLM's context-window refusal for the model's limit and the tokens the request needed, and `ToolChoiceUnsupportedMessage` is the tool-choice refusal verbatim. These phrases are matched here and nowhere else.

`uintAfterPhrase` runs both the search and the slice on the lowered copy of the message. Lowercasing can shorten a string — U+212A KELVIN SIGN lowers to a one-byte `k` — so an index taken from one string and applied to the other lands mid-word.

## SSE framing

### Two `data:` prefixes that must not be unified

`sseDataPrefix` (`"data: "`, with the space) is what the gateway *emits*. `sseDataParsePrefix` (`"data:"`, without) is what it *parses*, because a host may send either. They look like a duplicate and are not.

### Reading events

`indexEventEnd` returns the offset just past the first `\n\n` or `\r\n\r\n` terminator. It walks line by line rather than searching for both separators: searching for a CRLF terminator that an LF-framed stream never carries would scan to the end of the buffer for every event.

`eventPayload` joins an event's `data:` lines with a newline, the way a client does per the SSE spec, and reports where those lines were. One object split across two data lines reaches the client as one object, so it must reach the strip as one too.

`rebuildEvent` writes the rewritten payload back, keeping every non-data line where it was — a client reads `event`, `id` and `retry` from the lines around the data. The payload goes out as one `data:` line per segment, the inverse of the join: written after a single prefix, its embedded newlines would start lines carrying no `data:` prefix, and a client drops those and rejoins a truncated object.

`MaxStreamCarryBytes` (32 MiB) bounds the unterminated tail held per stream, so a host that never sends a terminator cannot grow it without limit. Both `StreamRewriter` and `BodyFolder` fail permanently once it is exceeded.

### Rewriting an event

`rewriteEvent` takes every decision on the decoded payload, never on the event's raw bytes. A host controls those bytes: it can spell a key with a `\u` escape, or split one object across two data lines, and either defeats a byte-wise check while the client's own decoder reads the object whole.

A payload that opens as an object and does not parse is dropped rather than forwarded — a host sending something no client can read would otherwise carry along whatever it hides. A payload that is not object-shaped passes through untouched.

When the client did not ask for usage, `stripUsage` removes it. An event left with nothing but housekeeping (`id`, `object`, `created`, `model`, `system_fingerprint`, `service_tier`, and an empty `choices`) is dropped entirely. The test is for housekeeping rather than for empty choices, because a host's error event carries no choices either and must survive.

### A complete reply on a streaming request is rewritten into chunks

Some hosts answer a forced stream with a whole `chat.completion`. That hands the client a `message` where it reads a `delta`, so the client renders nothing while the nonce is settled and the money is spent. `completionAsChunks` converts it into the `chat.completion.chunk` events an OpenAI client actually renders, sending the role, the payload and the finish reason as separate chunks the way a real stream does, with usage last. Since the whole answer arrives as one delta, its logprobs ride that same chunk.

Every host-controlled field is carried as a raw message. Decoding one into a typed field would let a host fail the conversion with a value of the wrong type — a numeric `id`, a `created` past the float range — and the client would be back to reading a message where it renders a delta. `rawOr` supplies a fallback for an absent field, since an empty raw message is not JSON and would fail the very encode this conversion exists to produce; the fallbacks are the zero values the typed fields used to encode, so an ordinary response converts byte for byte. `presentValue` counts JSON `null` as absent, so a field the host spelled out as null is not re-sent as one the client must interpret.

`SSEDoneEvent` is the terminator an SSE client reads until; without it the client waits out its own timeout instead of finishing. `HasSSEDone` checks whether the stream already carries one, line-anchored so a `[DONE]` inside a content delta is not mistaken for the terminator.

## Folding a stream into one body

`BodyFolder` is the incremental fold: it strips per event as chunks arrive, then merges, so a client that did not ask for logprobs never accumulates them. `assembleSSEBody` in `assemble.go` is the older whole-body fold, kept as the reference implementation the incremental one is verified against. Both share the merge.

`Held()` reports what the folder is holding so a shared memory budget can watch it. Merging collapses what the events repeat, so the accumulated size is *measured* rather than summed. The trigger for a re-measure is bytes (`foldMeasureBytes`, 256 KiB) rather than events: one event can carry a megabyte, and a count-based interval would leave that much unaccounted while the cap and the shared budget read low. `measure` re-encodes the accumulator without finalising it, because finalising rewrites the deltas the fold is still appending to.

### What the fold returns

- A failed carry-overflow, or a stream that framed events but carried no payload, returns `NoResponseDataBody`.
- A fold that ran past `maxAssembledEvents` (65,536) returns `TruncatedResponseBody`. Returning the prefix would be a complete-looking answer missing its tail.
- A complete `chat.completion` seen before any chunk was merged is returned stripped, as-is.
- Otherwise the merged accumulator is finalised and encoded.
- A stream that never framed a `data:` line at all is returned as the raw body, stripped — an unframed error page still reaches the caller.

### How the merge works

Everything outside `choices` is a restated header, so it replaces rather than accumulates, bounded by `maxTopLevelFields` (64) for keys not already present. Within a choice, only `delta`, `logprobs` and `token_ids` accumulate; `finish_reason` and the rest replace.

`mergeStreamedValue` accumulates by *field name*, never by Go type. A host restates its identity fields, and growing those hands the client a tool call it cannot answer. Only `content`, `reasoning`, `reasoning_content`, `refusal` and `arguments` grow as text.

Text grows through `growingText`, which keeps the join off the per-chunk path where it would be quadratic in the answer's length. `growText` appends, but replaces instead when a host re-sends `arguments` whole — detected by the incoming string having the accumulated one as its prefix.

Arrays merge by `index` when the accumulated array leads with an indexed element and the incoming one is fully indexed; otherwise they append. `leadsWithAnIndex` decides on the first element alone: scanning the whole array per chunk was what made the merge quadratic. Indexed merging is bounded by `maxIndexedElements` (256).

`finalizeCompletion` rewrites the accumulator in place — so it runs once, at the end, never as part of measuring — turning each choice's `delta` into its `message`, ordered by index. A message with no `content` key gets an explicit `null`, because upstream answers a tool call with a null content rather than with the field absent.

## Errors and rejection status

`RejectError` carries an HTTP status and wraps a cause. `Reject` and `WrapReject` both produce a 400. `ErrorStatus` checks for an ingest-cap overrun first: a body refused for its size stays a 413 even when a `RejectError` carrying 400 wraps it.

## Read next

- [`docs/request.md`](../docs/request.md) — every rule, every stage, and the arithmetic behind the bounds.
