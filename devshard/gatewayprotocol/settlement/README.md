# `settlement` — the settle message and its root

## What it owns

- `Message`, `HostStat` and `SlotSignature`: the settle payload with its binary fields decoded, one field for each field of `MsgSettleDevshardEscrow` apart from the settler, which the signer supplies.
- `Root`: the settlement state root, `state.ComputeHostStatsHash` over the payload's host stats followed by `state.ComputeStateRootFromRestHash` in `types.PhaseSettlement` with the payload's rest hash, fees and version. It hashes the host stats exactly as given and never filters them, so a caller gets the root of the map it passed.
- `PresentStats`: a copy of a host-stats map without its nil entries.
- `HostStatsBySlot` and `SignaturesBySlot`: the two lists of the message in ascending slot order. `HostStatsBySlot` expects every entry to be non-nil.

## How the gateways use it

`devshard/cmd/gateway` drops nil entries with `PresentStats` first, then takes the root, the host stats and the signatures of that copy; the chain recomputes the root over the host stats the transaction carries, so both must come from the same map. `devshard/cmd/devshardctl` takes the root of its payload as built, whose entries are non-nil by construction in `devshard/state`, and lists its host stats and signatures with the two `BySlot` functions, so both gateways send both lists in ascending slot order.

The order of the host stats is protocol: the chain gives every host-stats entry its share of the fees and the remainder of the fees over the slot count one coin per entry in list order, so a fixed order is what makes the same payload pay the same validators every time either gateway builds it. The order of the signatures does not change what the chain accepts or pays, since it checks each signature on its own, refuses duplicates and counts the votes; both gateways sort them as well, so the same payload always yields the same settle message.

## What it does not own

Escrow-id parsing, collecting signatures and what to post when they fall short of quorum, narrating a settlement the chain would refuse, the devshardctl admin JSON, and the wire encoding, which is [`settlementwire`](../settlementwire/README.md).

## Imports

`devshard/state`, `devshard/types` and the standard library.
