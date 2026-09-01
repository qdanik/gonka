# `burns` — charging a host for the nonce it would not take

The scheduler burns a nonce bound to a host that cannot serve it. The nonce is already committed on chain, so the escrow pays its reserve either way; without a vote, the host reaches no miss and the cost lands entirely on the escrow.

## What it owns

- **`Accountant`** — takes each burn (`Burned`), and for the burns a host actually earned, settles the nonce against it. Burns the gateway caused itself — a host under proof-of-compute, an excluded or abandoned nonce — are not charged.
- **The probe** (`probe.go`) — before charging, the burn is spent on a **real chat request** to that host. The nonce was committed with a payload, so the request is the one the escrow has already paid for. A host that answers clears itself and is charged nothing; one that stays silent earns exactly the timeout the quiet burn would have raised.
- **The gate** — one probe in flight per participant, and no more often than every five seconds. The picker burns throttled ghosts in a tight loop, and each probe competes for the very capacity the host is short of.

## What it does not own

It does not decide that a nonce is burned — that is [`scheduler`](../scheduler/). It does not record the burn in the ledger — that is [`nonces`](../nonces/). It only decides whether the host owes a vote for it.

## Boundaries

- **Charging is off by default** (`charge_refused_nonces`). It changes what the network bills for, so enabling it is an explicit operator decision.
- **One goroutine per burn.** Settling waits out the refusal deadline; a queue of burns settled in turn would hold the escrow's drain barrier for that wait once per nonce.
- **The gate never swallows the charge.** It decides whether to ask the host again, never whether the nonce is owed a vote.

## Read next

- [`docs/routing.md`](../docs/routing.md) — why a nonce is burned in the first place, and the reason each burn carries.
