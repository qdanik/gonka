# `timeoutclaim` — claim a miss or vote a timeout

## What it owns

- `Handler`: what a session posts for a nonce left unfinished, a timeout vote (`HandleTimeout`) or an error-miss claim (`HandleErrorMiss`); `*user.Session` satisfies it.
- `Resolve`: the choice between them. A non-empty Finish transaction claims an error miss with the response payload; otherwise the timeout is voted with the inference payload. The claim and the vote are different messages that the hosts judge by different checks, so the two gateways must make the same choice from the same Finish.
- `Payload`: the inference payload a timeout vote carries, built from the nonce's live record plus the prompt it was dispatched with, and none when the nonce has no live record. A verifier re-checks the payload field by field against its own copy of that record, so the record is the authority for every field but the prompt; the record is built from the dispatch params unchanged, so for a nonce with a live record the payload equals the one the host was dispatched with. A nonce leaves the live records only when it is sealed, which happens to a closed record or to every record once the escrow moves to settlement. A verifier that has applied the seal refuses a vote for such a nonce before it reads any payload, and the session's state machine cannot apply its timeout whatever the verifiers answer, so there is nothing to carry.

## How the gateways use it

Both gateways build every vote's payload with `Payload` over their session's live records. `devshard/cmd/devshardctl` calls `Resolve` with the Finish it gathered for an error miss; `devshard/cmd/gateway` calls it with the Finish it gathered for a miss claim that carries a proof.

## What it does not own

When a miss claim is attempted and where its Finish comes from: the two gateways decide both differently and keep them. Whether a nonce with no live record is voted at all, which the session decides. Deadlines and the wait before a vote.

## Imports

`devshard/host`, `devshard/types`, `devshard/user` and the standard library.
