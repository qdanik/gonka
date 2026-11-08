# `timeoutclaim` — claim a miss or vote a timeout

## What it owns

- `Handler`: what a session posts for a nonce left unfinished, a timeout vote (`HandleTimeout`) or an error-miss claim (`HandleErrorMiss`); `*user.Session` satisfies it.
- `Resolve`: the choice between them. A non-empty Finish transaction claims an error miss with the response payload; otherwise the timeout is voted with the inference payload. The claim and the vote are different messages that the hosts judge by different checks, so the two gateways must make the same choice from the same Finish.
- `PayloadFromParams` and `PayloadFromRecord`: the inference payload a timeout vote carries, built from the params the nonce was dispatched with or from the record it committed plus the dispatched prompt. A verifier re-checks the payload field by field against the committed record, and the record is built from those same params, so both builders yield the same payload for a committed nonce.

## How the gateways use it

`devshard/cmd/devshardctl` builds the payload from its params for every vote and calls `Resolve` with the Finish it gathered for an error miss. `devshard/cmd/gateway` builds the payload from the committed record, has no payload for a nonce the escrow never committed, and calls `Resolve` with the Finish it gathered for a miss claim that carries a proof.

## What it does not own

When a miss claim is attempted and where its Finish comes from: the two gateways decide both differently and keep them. What the payload is for a nonce that never committed, which differs between the gateways too. Deadlines and the wait before a vote.

## Imports

`devshard/host`, `devshard/types`, `devshard/user` and the standard library.
