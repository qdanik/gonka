# `registry` — the live escrow set

Which escrows exist right now, what each serves, and who its hosts are.

## What it owns

- **The set itself** (`registry.go`, `escrow.go`) — escrow id to session, model, group, published or draining, behind a copy-on-write snapshot so a reader never blocks a writer.
- **Sessions** (`session.go`) — two kinds. A serving session has host clients and can dispatch; a read-only one rehydrates from local storage alone and can build a settlement but can neither serve nor finalize.
- **Membership and capacity** (`membership.go`, `views.go`) — the participant set each escrow contributes to the capacity model, and the in-flight count routing scores by.
- **Settlement handles** (`settlement.go`) — a retired escrow still resolves, because its committed nonces have no other settlement path.

## Boundaries worth knowing

- **A retired escrow does not disappear.** Its nonces still owe votes, and the session that can post them outlives its routability.
- **The in-flight hold is taken with the nonce commit and refused once retired**, so an escrow cannot start work it will not be able to settle.
