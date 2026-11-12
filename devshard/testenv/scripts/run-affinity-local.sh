#!/usr/bin/env bash
# Measures gateway session affinity on a GPU-free local stack: every process is native, nothing needs Docker.
# Usage: run-affinity-local.sh OUT_DIR   (env: SEEDS="1 2 3", SKIP_BUILD=1 to reuse OUT_DIR/bin, PR34_SRC=a checkout of d6c01d74 to build config B0's gateway)
# Each run is a fresh stack; affinityrun stops every process it started. Failed runs are listed, not retried.
set -uo pipefail

out=${1:?usage: run-affinity-local.sh OUT_DIR}
seeds=${SEEDS:-"1 2 3"}
devshard=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$out/bin" "$out/runs"
out=$(cd "$out" && pwd)
export GOTOOLCHAIN=${GOTOOLCHAIN:-go1.25.9}

if [ -z "${SKIP_BUILD:-}" ]; then
  for cmd in testenv/cmd/gencompose testenv/cmd/mockchain testenv/cmd/mockdapi testenv/cmd/mockopenai testenv/cmd/affinityrun cmd/devshardd cmd/gateway; do
    (cd "$devshard" && go build -o "$out/bin/$(basename "$cmd")" "./$cmd") || { echo "build of $cmd failed" >&2; exit 1; }
  done
fi

if [ -z "${SKIP_BUILD:-}" ] && [ -n "${PR34_SRC:-}" ]; then
  (cd "$PR34_SRC/devshard" && go build -o "$out/bin/gateway-pr34" ./cmd/gateway) || { echo "build of gateway-pr34 failed" >&2; exit 1; }
fi

# scenario:configs. Seeds are the outer loop and the config order rotates with the seed, so slow drift on the machine does not always land on the same config.
matrix=("equal:A B0 B C D" "unequal:A B C D E" "busy:A B C D" "noreplay:A C" "pinage:C P")
if [ ! -x "$out/bin/gateway-pr34" ]; then
  echo "no gateway-pr34 in $out/bin (set PR34_SRC): skipping config B0" >&2
  matrix[0]="equal:A B C D"
fi
failed=()
seed_index=0
for seed in $seeds; do
  for entry in "${matrix[@]}"; do
    scenario=${entry%%:*}
    read -r -a configs <<< "${entry#*:}"
    shift_by=$((seed_index % ${#configs[@]}))
    ordered=("${configs[@]:shift_by}" "${configs[@]:0:shift_by}")
    for config in "${ordered[@]}"; do
      run="$out/runs/$scenario/$config/$seed"
      [ -f "$run/result.json" ] && continue
      mkdir -p "$run"
      echo "$(date +%T) $scenario $config seed $seed"
      uptime > "$run/load-before.txt"
      "$out/bin/affinityrun" -bin "$out/bin" -out "$run" -config "$config" -scenario "$scenario" -seed "$seed" || failed+=("$scenario/$config/$seed")
      uptime > "$run/load-after.txt"
    done
  done
  seed_index=$((seed_index + 1))
done

"$out/bin/affinityrun" -aggregate "$out/runs" > "$out/tables.md"
echo "tables: $out/tables.md"
[ ${#failed[@]} -eq 0 ] || { echo "failed runs: ${failed[*]}" >&2; exit 1; }
