# Cross-cutting documents

Each package has its own `README.md` describing what that layer owns. The documents here describe behaviour **no single package owns** — a request crossing five of them, an escrow's life, the arithmetic behind a threshold.

| Document | What it answers |
| --- | --- |
| [gateway-architecture.md](./gateway-architecture.md) | the package map and what each one is responsible for |
| [gateway-request-lifecycle.md](./gateway-request-lifecycle.md) | one request from the socket to the settled nonce |
| [gateway-speculative-race.md](./gateway-speculative-race.md) | the escalation ladder, the crown, the drain barrier |
| [gateway-routing-and-nonces.md](./gateway-routing-and-nonces.md) | the drain loop, the host gates, every burn reason |
| [gateway-escrow-lifecycle.md](./gateway-escrow-lifecycle.md) | escrow states, rotation, settlement, retirement |
| [gateway-capacity-and-health.md](./gateway-capacity-and-health.md) | admission, the AIMD window, ejection, buffered replies |
| [gateway-request-filtering.md](./gateway-request-filtering.md) | every parameter rule, every bound, and the arithmetic behind it |
| [gateway-verification-limits.md](./gateway-verification-limits.md) | what the gateway can and cannot verify about a host |
| [accounting-findings.md](./accounting-findings.md) | every finding, its threshold, and what an operator does about it |
| [findings.md](./findings.md) | findings recorded against this codebase itself |
| [gateway-invariants.md](./gateway-invariants.md) | what must never stop being true |
| [gateway-non-goals.md](./gateway-non-goals.md) | what this gateway deliberately does not do, and why |
| [gateway-operations.md](./gateway-operations.md) | every route, every environment variable, every metric |
| [proposal.md](./proposal.md) | the original design proposal |

Start from [`../README.md`](../README.md) or [`../Architecture.md`](../Architecture.md).
