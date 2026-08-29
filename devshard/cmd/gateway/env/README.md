# `env` — the only place environment variables are read

One typed table: name, type, and nothing else.

## What it owns

- **`Load`** — returns what is **set**, as pointers. A `nil` means the variable was not set; it never means zero.
- **The renaming compatibility** — a variable recorded under an older `DEVSHARD_` name is still read, and the gateway says which spelling it used.
- **`PrivateKey`** — reads a signing key by the **name** of the variable holding it. Errors and log lines name the variable, never the value.

## Boundaries worth knowing

- **Defaults are not here.** They belong to [`config`](../config/), which is what makes "unset" and "set to zero" distinguishable.
- **A key is addressed by name, not by value**, everywhere: in `GATEWAY_ESCROWS_JSON`, in the admin API, and here. A key pasted into a request body would reach the logs and the shell history.
- **An empty value counts as unset under both spellings.** The fallback only fires when the gateway's own name is blank, and a blank legacy name is blank too — so an operator can empty a legacy variable without the fallback resurrecting it.
- **Every parse failure is reported at once.** `Load` accumulates problems instead of returning on the first, so one restart names every misconfigured variable rather than one per attempt. `GATEWAY_POC_MODE` is value-checked here for the same reason, even though `config` validates it again.

## The renaming table

`legacyNames` maps each gateway variable to the `devshardctl` spelling it falls back to; a variable absent from that table has no `devshardctl` equivalent and is read under its `GATEWAY_` name only. `PrivateKey` runs the fallback in the other direction — a devshard record that names a `DEVSHARD_`-prefixed key variable is also looked up under the `GATEWAY_` prefix, and the gateway logs which spelling it used. See [`docs/operations.md`](../docs/operations.md), "Variable names".
