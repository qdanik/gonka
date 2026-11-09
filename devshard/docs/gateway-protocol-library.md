# Gateway protocol library — design

**Status:** Implemented in `devshard/gatewayprotocol` for settlement (`settlement`, `settlementwire`), the timeout claim (`timeoutclaim`), the probe budget (`probe`) and the evidence of an error stream (`streamevidence`).

**Audience:** Engineers changing what either gateway, `devshard/cmd/devshardctl` or `devshard/cmd/gateway`, sends to hosts, verifiers or the chain.

This document says what belongs in the shared tree, where it lives and how its boundary is enforced. The package READMEs under `devshard/gatewayprotocol` say what each package owns.

## 1. What is protocol

Code is protocol when a host, a verifier or the chain would reject, pay differently or judge differently if it received the other gateway's result. Everything else is gateway-local: locking, memory ownership, panic recovery, HTTP, logging, metrics, and also choices no other party can tell apart, such as a byte layout the chain decodes to the same message, a prompt literal, or how a gateway reacts to an error.

Two consequences follow.

- The settle wire encoding is not protocol by the test. devshardctl encodes its settle message through `common/chain/tx`, which writes zero-valued fields; the new gateway marshals the chain's generated type, which omits them. The chain decodes both to the same message. `settlementwire` is still shared because it owns the mapping from the settle message onto `MsgSettleDevshardEscrow`; devshardctl builds its settle transaction through `common/chain/tx`, which `devshardd` links too, and a devshardctl test pins the message that `settleParamsFromJSON` and `common/chain/tx` build from a settlement JSON to the same decoded chain message `settlementwire` marshals.
- Content that is already decided in a shared package gets no package here. The timeout reason a host sees is chosen in `devshard/user`, and the state-root mismatch errors are defined in `devshard/types` and `devshard/state`. The probe's token floor is `state.MinReservationTokens`, enforced by the host's state machine; what lives here is only how a probe applies it, in `probe`.

## 2. Module location

One package tree inside the `devshard` module. A separate module does not pay: the tree needs `devshard/state`, `devshard/types`, `devshard/host` and `devshard/user`, and both gateways need the tree, so a second `go.mod` would form a `replace` cycle; [`spool-shared-library.md`](./spool-shared-library.md) §2 reached the same answer for `spool`. `common` is a separate module that does not require `devshard`, so nothing in `common` can import the tree. Inside `devshard`, the shared packages the tree imports import nothing under `devshard/cmd`, and the guard keeps them from importing the tree.

```
devshard/gatewayprotocol/       README.md, guard_test.go
  probe/                        README.md, budget.go, budget_test.go
  settlement/                   README.md, message.go, order.go, root.go, tests
  settlementwire/               README.md, marshal.go, marshal_test.go, testdata/ (settle goldens)
  streamevidence/               README.md, lines.go, payload.go, vocabulary.go, tests, testdata/ (error stream golden and shapes, long error stream fixture)
  timeoutclaim/                 README.md, claim.go, payload.go, tests
```

## 3. Packages

### 3.1 `settlement`

Owns the settle message (`Message`, `HostStat`, `SlotSignature`), the slot order of its lists (`HostStatsBySlot`, `SignaturesBySlot`) and the settlement root (`Root`, with `PresentStats` to drop nil entries first). `Root` hashes the host stats exactly as given and never filters, so devshardctl's root is the same expression it always computed and the new gateway takes the root of its already-filtered copy.

It does not own escrow-id parsing, signature collection or what to post when the checked signatures fall short of quorum, narration of a settlement the chain would refuse, or the devshardctl admin JSON. Each gateway keeps those.

### 3.2 `settlementwire`

Owns `Marshal(settler, message)`: the mapping from `settlement.Message` onto `MsgSettleDevshardEscrow` and its marshalling through the chain's generated type. It does not own the transaction envelope, signing or broadcasting, which stay in `devshard/cmd/gateway/chain` and `common/chain/tx`, nor the create-escrow message. It is the only package in the tree allowed to import chain types.

### 3.3 `timeoutclaim`

Owns the choice between an error-miss claim and a timeout vote for a nonce left unfinished (`Resolve` over a `Handler`, which `*user.Session` satisfies): a non-empty Finish transaction claims the miss, otherwise the timeout is voted. It owns the Finish the claim carries (`FinishTx`) and the timeout vote's payload (`Payload`), the one rule each that both gateways follow; `devshard/gatewayprotocol/timeoutclaim/README.md` gives the rules and their reasons.

It does not own whether the claim's payload proves the error, which is `streamevidence.ProvesErrorMiss` (§3.5). Deadlines and waiting stay in the gateways too.

### 3.4 `probe`

Owns the token budget of a probe: `MaxTokens`, the reservation floor `state.MinReservationTokens`, and `Params`, which builds a probe's params with that reservation and the prompt's byte length as its input length. devshardctl's probes, the new gateway's warm-up probe and its ghost burn all use it. The probe prompt is not owned: a host accepts any prompt that parses and asks for no more than `MaxTokens`, so each gateway keeps its own literal and formats `MaxTokens` into it.

### 3.5 `streamevidence`

Owns which lines of an error stream are evidence (`DataLines`: `data: ` lines with a trailing carriage return trimmed, without the devshard receipt and meta envelopes), the `[DONE]` test that marks a retained stream complete (`IsDoneLine`), and the error-miss payload those lines serialize into (`Payload`, the JSON of `completionapi.SerializedStreamedResponse`). It also owns the most evidence a gateway retains of one attempt (`MaxRetainedBytes`). A verifier hashes the payload and compares it with the Finish transaction's hashes, so a line kept or dropped differently by one gateway turns an accepted claim into a rejected one, and a stream cut by the bound is never accepted. The bound's value, how lines count against it and why are in the package README, "The retention bound". The envelope keys and the terminator marker sit in its `vocabulary.go`.

It also owns whether the claim's payload proves the error (`ProvesErrorMiss`): an attempt a gateway read as ending in a terminal error is claimed only when its payload passes, and is otherwise voted or skipped like any other unfinished nonce; the package README gives the reason. Which error events count as terminal stays in each gateway.

It does not own the retainer: what a gateway does when the bound is hit, the memory budget it charges retained lines to, and how they are released. Each gateway keeps its own retainer and may stop below the bound when that budget refuses (devshardctl's participant or global classify budget, the new gateway's reassembly budget).

## 4. Allowed imports and the guard

| Package | May import |
| --- | --- |
| `probe` | `devshard/state`, `devshard/user`, standard library |
| `settlement` | `devshard/state`, `devshard/types`, standard library |
| `settlementwire` | `settlement`, `github.com/productscience/inference/x/inference/types`, standard library |
| `streamevidence` | `common/completionapi`, standard library |
| `timeoutclaim` | `devshard/host`, `devshard/types`, `devshard/user`, standard library |

Forbidden everywhere in the tree: `devshard/cmd/...`, `sync`, `sync/atomic`, `net/http`, `os`, `log`, `log/slog`, `devshard/logging`, `devshard/observability`, `github.com/prometheus/...`.

`devshard/gatewayprotocol/guard_test.go` checks both directions:

1. Every directory under the tree with production Go files has a row in the layer table and a `README.md`, every row has such a directory, and each import is in its row or the standard library and not forbidden.
2. Every production `.go` file of the `devshard` module that imports `devshard/gatewayprotocol/...` is under `devshard/cmd/gateway/...`, in `devshard/cmd/devshardctl`, or in the tree itself.

The reverse check keeps host packages from importing the tree directly. It does not catch a host binary that imports a `devshard/cmd/gateway` package; `go list -deps ./cmd/devshardd ./cmd/devshard-host | grep gatewayprotocol`, run from `devshard`, prints nothing while hosts do not link the tree.

## 5. Changing a rule

A protocol change is made in the tree, with a test that pins the bytes or the decision. A gateway picks it up for the parts it calls: today devshardctl calls `settlement.Root`, `settlement.HostStatsBySlot`, `settlement.SignaturesBySlot`, `timeoutclaim`, `probe` and `streamevidence`, and `devshard/cmd/gateway` calls every function of the tree. Where the two gateways still apply a rule differently, each keeps its own version until a change of its own chooses one, with the test that decides it.
