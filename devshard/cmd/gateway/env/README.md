# `env` — the only place environment variables are read

One typed table: name, type, and nothing else.

## What it owns

- **`Load`** — returns what is **set**, as pointers. A `nil` means the variable was not set; it never means zero.
- **The renaming compatibility** — a variable recorded under an older `DEVSHARD_` name is still read, and the gateway says which spelling it used.
- **`PrivateKey`** — reads a signing key by the **name** of the variable holding it. Errors and log lines name the variable, never the value.

## Boundaries worth knowing

- **Defaults are not here.** They belong to [`config`](../config/), which is what makes "unset" and "set to zero" distinguishable.
- **A key is addressed by name, not by value**, everywhere: in `GATEWAY_ESCROWS_JSON`, in the admin API, and here. A key pasted into a request body would reach the logs and the shell history.
