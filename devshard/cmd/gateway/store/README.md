# `store` — the control plane on disk

One SQLite database at `<storageDir>/gateway.db`, holding what must survive a restart.

## What it owns

| File | What it holds |
| --- | --- |
| `devshards.go` | the escrow registry: which escrows this gateway owns and their state |
| `overrides.go` | the operator's runtime configuration overrides |
| `commitments.go`, `rotation_status.go` | rotation intent recorded before the transaction, and where each rotation got to |
| `suspicious.go` | hosts the operator pinned as suspect |
| `accounting.go` | the asynchronous per-request ledger, with its own retention |
| `retry.go` | the retry policy for a locked database |

## Boundaries worth knowing

- **This is control-plane state, not inference data.** Payloads and the nonce ledger live elsewhere.
- **Retention is enforced on both age and row count**, and a zero on either is rejected at construction, so an unbounded ledger cannot be configured.
- **A write that must survive a crash is one statement**, not a read-modify-write.
