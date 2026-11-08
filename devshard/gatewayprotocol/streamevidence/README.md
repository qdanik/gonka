# `streamevidence` — the evidence of an error stream

## What it owns

- `DataLines`: which lines of a read of SSE events are evidence. A line is kept when it starts with `data: `, after a trailing carriage return is trimmed, and is not a devshard envelope: a data line whose JSON names `"devshard_receipt"` or `"devshard_meta"` is the host's own receipt or Finish tail, not part of the executor's response, so it is dropped. Comment, event and blank lines are dropped too.
- `IsDoneLine`: whether a data line is the `[DONE]` terminator, after the `data: ` prefix and surrounding spaces are stripped. A retained stream that holds it is complete.
- `Payload`: the response payload of an error-miss claim, the JSON of `completionapi.SerializedStreamedResponse` with the evidence lines as its events. A verifier hashes these bytes and compares them with the response hash or the served hash of the Finish transaction, so both gateways must keep the same lines and serialize them the same way.
- The envelope keys and the terminator marker, in `vocabulary.go`.

## How the gateways use it

`devshard/cmd/devshardctl` takes `DataLines` of every streamed read of a non-probe attempt while the attempt is still speculative (`retainErrorStreamLines`) and drops them once the attempt shows no error, marks the attempt complete on `IsDoneLine`, and builds the claim's payload with `Payload` (`errorMissArtifacts`). `devshard/cmd/gateway` does the same in its error-stream retainer (`engine/miss_proof.go`): `DataLines` on each read, `IsDoneLine` for completeness, `Payload` for the proof.

## What it does not own

The retainer: how many bytes of evidence a gateway keeps and what it does when the bound is hit, which differ between the gateways (devshardctl reads its bound from `GATEWAY_ERROR_STREAM_MAX_ATTEMPT_BYTES` and charges its participant classify budget per line; the new gateway keeps a fixed bound). Release and ownership of the retained lines. When a miss claim is attempted and where its Finish comes from.

## The golden

`testdata/newlineless_final_error.sse` is a copy of the terminal-error fixture in `devshard/cmd/gateway/filters/testdata/sse/`: one error event with no final newline. `testdata/newlineless_final_error.json` is the payload devshardctl's and the new gateway's own evidence code each produced from it, byte for byte the same, before both moved onto this package. The golden changes only together with a change to the evidence rule, which both gateways then pick up.

## Imports

`common/completionapi` and the standard library.
