# Gateway protocol library — design

**Status:** Implemented in `devshard/gatewayprotocol` for settlement (`settlement`, `settlementwire`).

**Audience:** Engineers changing what either gateway, `devshard/cmd/devshardctl` or `devshard/cmd/gateway`, sends to hosts, verifiers or the chain.

This document says what belongs in the shared tree, where it lives and how its boundary is enforced. The package READMEs under `devshard/gatewayprotocol` say what each package owns.

## 1. What is protocol

Code is protocol when a host, a verifier or the chain would reject, pay differently or judge differently if it received the other gateway's result. Everything else is gateway-local: locking, memory ownership, panic recovery, HTTP, logging, metrics, and also choices no other party can tell apart, such as a byte layout the chain decodes to the same message, a prompt literal, or how a gateway reacts to an error.

Two consequences follow.

- The settle wire encoding is not protocol by the test. devshardctl encodes its settle message through `common/chain/tx`, which writes zero-valued fields; the new gateway marshals the chain's generated type, which omits them. The chain decodes both to the same message. `settlementwire` is still shared because it owns the mapping from the settle message onto `MsgSettleDevshardEscrow`; devshardctl keeps its encoder until it moves onto `settlementwire` in a change of its own.
- Content that is already decided in a shared package gets no package here. The timeout reason a host sees is chosen in `devshard/user`, the state-root mismatch errors are defined in `devshard/types` and `devshard/state`, and the probe's token floor is `state.MinReservationTokens`, enforced by the host's state machine.

## 2. Module location

One package tree inside the `devshard` module. A separate module does not pay: the tree needs `devshard/state` and `devshard/types`, and both gateways need the tree, so a second `go.mod` would form a `replace` cycle; [`spool-shared-library.md`](./spool-shared-library.md) §2 reached the same answer for `spool`. `common` is a separate module that does not require `devshard`, so nothing in `common` can import the tree. Inside `devshard`, the shared packages the tree imports import nothing under `devshard/cmd`, and the guard keeps them from importing the tree.

```
devshard/gatewayprotocol/       README.md, guard_test.go
  settlement/                   README.md, message.go, order.go, root.go, tests
  settlementwire/               README.md, marshal.go, marshal_test.go, testdata/ (settle goldens)
```

## 3. Packages

### 3.1 `settlement`

Owns the settle message (`Message`, `HostStat`, `SlotSignature`), the slot order of its lists (`HostStatsBySlot`, `SignaturesBySlot`) and the settlement root (`Root`, with `PresentStats` to drop nil entries first). `Root` hashes the host stats exactly as given and never filters, so devshardctl's root is the same expression it always computed and the new gateway takes the root of its already-filtered copy.

It does not own escrow-id parsing, signature collection or what to post when the checked signatures fall short of quorum, narration of a settlement the chain would refuse, or the devshardctl admin JSON. Each gateway keeps those.

### 3.2 `settlementwire`

Owns `Marshal(settler, message)`: the mapping from `settlement.Message` onto `MsgSettleDevshardEscrow` and its marshalling through the chain's generated type. It does not own the transaction envelope, signing or broadcasting, which stay in `devshard/cmd/gateway/chain` and `common/chain/tx`, nor the create-escrow message. It is the only package in the tree allowed to import chain types.

## 4. Allowed imports and the guard

| Package | May import |
| --- | --- |
| `settlement` | `devshard/state`, `devshard/types`, standard library |
| `settlementwire` | `settlement`, `github.com/productscience/inference/x/inference/types`, standard library |

Forbidden everywhere in the tree: `devshard/cmd/...`, `sync`, `sync/atomic`, `net/http`, `os`, `log`, `log/slog`, `devshard/logging`, `devshard/observability`, `github.com/prometheus/...`.

`devshard/gatewayprotocol/guard_test.go` checks both directions:

1. Every directory under the tree with production Go files has a row in the layer table and a `README.md`, every row has such a directory, and each import is in its row or the standard library and not forbidden.
2. Every production `.go` file of the `devshard` module that imports `devshard/gatewayprotocol/...` is under `devshard/cmd/gateway/...`, in `devshard/cmd/devshardctl`, or in the tree itself.

The reverse check keeps host packages from importing the tree directly. It does not catch a host binary that imports a `devshard/cmd/gateway` package; `go list -deps ./cmd/devshardd ./cmd/devshard-host | grep gatewayprotocol`, run from `devshard`, prints nothing while hosts do not link the tree.

## 5. Changing a rule

A protocol change is made in the tree, with a test that pins the bytes or the decision. A gateway picks it up for the parts it calls: today devshardctl calls `settlement.Root`, and `devshard/cmd/gateway` calls every function of both packages. Where the two gateways still apply a rule differently, each keeps its own version until a change of its own chooses one, with the test that decides it.
