# `engine` — the speculative race

One client request, several attempts on different hosts, one winner. This package owns that race from the first pick to the vote that settles the last losing nonce.

## What it owns

| File | What it holds |
| --- | --- |
| `race.go` | the coordinator: the event loop, the exits, and the outcome it reports once |
| `pick.go` | asking the scheduler for a host and launching an attempt on it |
| `attempt.go` | one attempt's life: dispatch, receipt, chunks, terminal |
| `escalation.go` | when to start another attempt — the deadline ladder measured from the host's own history |
| `crown.go`, `stream.go` | crowning the first attempt to produce content, and forwarding only its bytes |
| `deadline.go`, `drain.go` | the timers, and the barrier that outlives the client |
| `classify.go`, `outcome.go` | what the attempt ended as, in the vocabulary the ledger admits |
| `settle.go`, `session.go` | the timeout vote every unfinished nonce owes |
| `reassembly.go`, `carry.go` | rebuilding events split across chunk boundaries |

## What it does not own

It does not choose the escrow or commit the nonce — that is [`scheduler`](../scheduler/). It does not shape the request or the reply — that is [`filters`](../filters/). It does not write the ledger — it reports one outcome, and [`nonces`](../nonces/) records it.

## Boundaries worth knowing

- **A losing attempt is not a free attempt.** Its nonce is committed and costs the escrow; the race is not over until every one of them has been settled or reported.
- **The race outlives the client.** A client that hangs up does not cancel the attempts, because their nonces still owe votes. The drain barrier is what makes shutdown wait for them.
- **The outcome is reported exactly once**, from whichever goroutine ends the race.
- **An SSE error event counts as a chunk but never crowns.** A host that answers with an error has answered something, but not content.

## Read next

- [`docs/gateway-speculative-race.md`](../docs/gateway-speculative-race.md) — the ladder, the crown, the drain, and why each is shaped that way.
