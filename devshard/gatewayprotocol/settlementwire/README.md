# `settlementwire` — the settle message on the wire

## What it owns

`Marshal` maps a `settlement.Message` and the settler's address onto the chain's generated `MsgSettleDevshardEscrow` and marshals it. The generated type tracks the chain's `.proto` by construction, so no field is laid out by hand. The generated type omits zero-valued scalars, as protobuf does; the chain decodes a message with them written or omitted to the same value. `Marshal` cannot fail for this message, which carries neither an `Any` nor a custom type, and the error is returned rather than dropped so a future field cannot make it silent.

## What it does not own

The transaction around the message (the `Any` wrapper, body, auth info, sign doc), signing and broadcasting stay in `devshard/cmd/gateway/chain`; the create-escrow message too. `devshard/cmd/devshardctl` encodes its settle transaction through `common/chain/tx` and does not call this package.

## The goldens

`testdata/` holds four base64 goldens: two whole messages (`settle_msg_full`, `settle_msg_empty_optional`) checked through `Marshal`, and one host-stats entry and one slot-signature entry checked through the package's mappings. They were written by `devshard/cmd/devshardctl/chain_txgold_test.go` (build tag `txgold`) from the fixed inputs `marshal_test.go` repeats. That generator writes into `devshard/cmd/gateway/chain/testdata/txgold` and does not compile today, so a golden here changes only by hand and with a reason.

## Imports

`devshard/gatewayprotocol/settlement`, `github.com/productscience/inference/x/inference/types` and the standard library. It is the one package in the tree that imports chain types.
