# `env` — the only place environment variables are read

One typed table: name, type, and nothing else.

## What it owns

- **`Load`** — returns what is **set**, as pointers. A `nil` means the variable was not set; it never means zero.
- **The renaming compatibility** — a variable recorded under an older `DEVSHARD_` name is still read, and the gateway says which spelling it used.
- **`PrivateKey`** — reads a signing key by the **name** of the variable holding it. Errors and log lines name the variable, never the value.

## Boundaries

- **Defaults are not here, with one exception.** `LogFormat` answers `json` for an empty `GATEWAY_LOG_FORMAT`, because logging is configured before `config` exists. Every other default belongs to [`config`](../config/), which is what makes "unset" and "set to zero" distinguishable.
- **A key is addressed by name, not by value**, everywhere: in `GATEWAY_ESCROWS_JSON`, in the admin API, and here. A key pasted into a request body would reach the logs and the shell history.
- **An empty value counts as unset under both spellings.** The fallback only fires when the gateway's own name is blank, and a blank legacy name is blank too — so an operator can empty a legacy variable without the fallback resurrecting it.
- **One table is read elsewhere, and only one.** The runtime-params feed ([`../runtime_params.go`](../runtime_params.go)) reads its own knobs through `devshard/runtimeparams`, because they are fleet-shared `DEVSHARD_*` / `DEVSHARDD_*` names every devshard binary honours under the same spelling. Restating them here would give one knob two owners, which is the failure this package exists to prevent.
- **Every parse failure is reported at once.** `Load` accumulates problems instead of returning on the first, so one restart names every misconfigured variable rather than one per attempt. `GATEWAY_POC_MODE` is value-checked here for the same reason, even though `config` validates it again.

## The log format

`LogFormat` reads `GATEWAY_LOG_FORMAT` apart from `Load`, because the format must be applied before anything can log and `Load` is the first thing that can fail. Empty means `json`, the form a collector reads as labels; `text` keeps the `log` package's own line. `Load` still refuses any other value together with the rest of its problems, so a typo surfaces at boot instead of silently picking a format.

## The renaming table

`legacyNames` maps each gateway variable to the `devshardctl` spelling it falls back to, and `legacyDurationNames` does the same for the two devshardctl durations the gateway reads as milliseconds; a variable absent from both tables has no `devshardctl` equivalent and is read under its `GATEWAY_` name only. `PrivateKey` runs the fallback in the other direction — a devshard record that names a `DEVSHARD_`-prefixed key variable is also looked up under the `GATEWAY_` prefix, and the gateway logs which spelling it used. See [`docs/operations.md`](../docs/operations.md), "Variable names".
