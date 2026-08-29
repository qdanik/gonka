# `config` — the immutable snapshot

Everything the gateway's behaviour depends on, in one value that is never mutated.

## What it owns

- **`Build`** — merges three layers in a fixed order: defaults, then environment, then the operator's stored overrides. It clones every map and slice it takes from its inputs, so nothing it returns shares memory with its sources.
- **`Defaults`** — the single source of every default value.
- **`Validate`** — fail-fast checks at startup; an unsafe combination is refused rather than discovered later.
- **`Holder`** — an atomic holder that publishes a whole replacement snapshot and notifies subscribers. Readers take a pointer and are never torn.

## Boundaries worth knowing

- **A snapshot is never mutated after `Build`.** Reconfiguration swaps the whole thing.
- **Defaults live here, never in [`env`](../env/).** A `nil` from the environment means "unset", which is not the same as a zero.
- **Zero is meaningful for several limits** — usually "unlimited". That is documented per field, because a zero that silently means something else is how a configuration lies.
