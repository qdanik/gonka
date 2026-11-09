# `timeoutclaim` — claim a miss or vote a timeout

## What it owns

- `Handler`: what a session posts for a nonce left unfinished, a timeout vote (`HandleTimeout`) or an error-miss claim (`HandleErrorMiss`); `*user.Session` satisfies it.
- `Resolve`: the choice between them. A non-empty Finish transaction claims an error miss with the response payload; otherwise the timeout is voted with the inference payload. The claim and the vote are different messages that the hosts judge by different checks, so the two gateways must make the same choice from the same Finish.
- `FinishTx`: the Finish an error-miss claim carries, the one the host's own reply carried for the nonce when it carried one, otherwise the one the session has queued (`FinishSource`, which `*user.Session` satisfies). A claim lands only if the record is finished when the claim applies, and a verifier checks the Finish against the committed response hash, so the reply's Finish is the one that matches whenever the session no longer queues it: a diff that already applied it, through another host's gossip or a compose before the gateway pinned it, empties the queue while the record is finished, and the reply's copy still lands the claim. Where the session never queued the reply's Finish (it failed verification or its height stamp, or the reply was refused before its mempool was read), the record is not finished and a claim from either source cannot land; the cost of the order is that a Finish failing verification is claimed and rejected where a timeout vote could have landed.
- `PayloadFromParams` and `PayloadFromRecord`: the inference payload a timeout vote carries, built from the params the nonce was dispatched with or from the record it committed plus the dispatched prompt. A verifier re-checks the payload field by field against the committed record, and the record is built from those same params, so both builders yield the same payload for a committed nonce.

## How the gateways use it

`devshard/cmd/devshardctl` builds the payload from its params for every vote and calls `Resolve` with `FinishTx` of the attempt's reply for an error miss. `devshard/cmd/gateway` builds the payload from the committed record, has no payload for a nonce the escrow never committed, and calls `Resolve` with `FinishTx` of the Finish the attempt's reply carried, kept on its miss proof, for a miss claim that carries a proof.

## What it does not own

When a miss claim is attempted: whether the payload proves the error is `streamevidence.ProvesErrorMiss`, which both gateways apply before they get here. What the payload is for a nonce that never committed, which differs between the gateways. Deadlines and the wait before a vote.

## Imports

`devshard/host`, `devshard/types`, `devshard/user` and the standard library.
