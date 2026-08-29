# Cross-cutting documents

Each package has its own `README.md` describing what that layer owns. The documents here describe behaviour **no single package owns** — a request crossing five of them, an escrow's life, the arithmetic behind a threshold.

| Document | What it answers |
| --- | --- |
| [../architecture.md](./architecture.md) | the package map and what each one is responsible for |
| [request.md](./request.md) | one request from the socket to the settled nonce |
| [race.md](./race.md) | the escalation ladder, the crown, the drain barrier |
| [routing.md](./routing.md) | the drain loop, the host gates, every burn reason |
| [escrows.md](./escrows.md) | escrow states, rotation, settlement, retirement |
| [capacity.md](./capacity.md) | admission, the AIMD window, ejection, buffered replies |
| [request.md](./request.md) | every parameter rule, every bound, and the arithmetic behind it |
| [rules.md](./rules.md) | what the gateway can and cannot verify about a host |
| [accounting.md](./accounting.md) | the nonce ledger: vocabulary, surface, findings, storage |
| [findings.md](./findings.md) | findings recorded against this codebase itself |
| [rules.md](./rules.md) | what must never stop being true |
| [rules.md](./rules.md) | what this gateway deliberately does not do, and why |
| [operations.md](./operations.md) | every route, every environment variable, every metric |
| [proposal.md](./proposal.md) | the original design proposal |

Start from [`../README.md`](../README.md) or [`./architecture.md`](./architecture.md).
