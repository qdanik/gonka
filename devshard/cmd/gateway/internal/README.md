# `internal` — helpers with no domain of their own

Three small packages that exist so the rest of the gateway does not have to repeat itself. Nothing here knows what a nonce or an escrow is.

| Package | What it is for |
| --- | --- |
| `logkey` | the field names every structured log line uses, in one place, so a collector can rely on them and a rename is a single edit |
| `leakcheck` | asserts in tests that a package left no goroutine behind — the gateway starts many, and one that outlives its owner is a leak nobody notices in production |
| `logcapture` | captures log output inside a test, so a line that is part of a contract can be asserted on |

`internal/` is not importable from outside `cmd/gateway`, which is the point: these are conveniences, not API.
