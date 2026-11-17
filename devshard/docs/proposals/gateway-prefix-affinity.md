# Gateway prefix affinity: bounds and measurements

## Bounds

Affinity is off by default (`affinity_enabled` false). With it on, a conversation key is pinned to the participant that last won it, and the next turn waits for that participant's nonce.

`affinity_max_burns` (default 2): the most `affinity_skip` burns one request may be charged to reach its host. The escrow pick binds a request only when its host is at most this many nonces from the cursor, and a queued request charged this many is released unbound at the next offer that would burn another for it. A burn takes 60 times the token price plus `fee_per_nonce` from the escrow.

`affinity_share_cap` (default 125): a pin is refused when its participant would hold more than this percent of its slot share of all pins. It applies only to a participant holding at least 8 pins, and only when slot weights are known.

`affinity_max_pin_age_ms` (default 3 600 000): a pin older than this is dropped at lookup and the key is routed as new.

`affinity_ttl_ms` (default 600 000) and `affinity_max_entries` (default 100 000) bound the map.

## Method

Five `devshardd` hosts, each with its own mock-dapi and mock-openai, one mock chain and the gateway, all native processes on one machine, no Docker. Two escrows of 5 slots each; each host holds one slot per escrow. Token price 10, `fee_per_nonce` 1000.

Host model: first token 60 ms, 150 µs per uncached prompt token, 5 ms per output token, 8 workers, prefix cache of 48 000 tokens. The cache is an LRU of 16-token blocks chained by prefix hash. A validation replay warms the host cache.

Load: 40 concurrent conversations of 8 turns, mean think time 3 s, system prompt 2 000 bytes, 2 400 bytes added per turn, 90 s per run. Half of the conversations send `prompt_cache_key`; the gateway keys the other half from their leading messages. Prompts derive from (seed, conversation slot, round).

Seeds 1, 2, 3. Runs are sequential and the setting order rotates with the seed. One-minute load average of the machine at the start of a run: 10 to 27.

Settings:

| name | affinity | max_burns | share cap | pin age |
|---|---|---|---|---|
| A | off | n/a | n/a | n/a |
| B | on | 1000 | 10000 | 7 d |
| C | on | 1 | 125 | 1 h |
| D | on | 2 | 125 | 1 h |

B is the three bounds at the largest values the gateway accepted when it was measured (the accepted ceilings are lower now: see the config README). A gateway built from d6c01d74 (no bounds) measured earlier on the same scenario gave hit rate 54.4% [49.9–58.9], same host 100%, burned nonces 1.19 per request.

## Results

Scenario `equal`, mean over 3 seeds, [min–max].

| metric | A off | B unbounded | C max_burns 1 | D max_burns 2 |
|---|---|---|---|---|
| prompt-cache hit rate at hosts | 22.6% [20.8–24.2] | 53.6% [50.0–56.0] | 41.7% [40.9–42.8] | 50.1% [47.5–52.3] |
| follow-ups on the previous host | 21.3% [19.4–22.4] | 100.0% [100.0–100.0] | 62.4% [60.4–64.3] | 82.5% [80.0–84.3] |
| burned nonces per request | 0 | 1.10 [1.06–1.15] | 0.227 [0.224–0.229] | 0.577 [0.565–0.583] |
| nonces assigned per request | 1.00 | 2.11 [2.06–2.16] | 1.23 [1.23–1.23] | 1.58 [1.57–1.59] |
| escrow spend per request | 32 809 [32 551–33 033] | 34 302 [34 167–34 521] | 33 265 [32 907–33 642] | 33 865 [33 711–34 049] |
| TTFT p50 (ms) | 427 [417–441] | 243 [221–253] | 321 [255–419] | 245 [219–267] |
| TTFT p95 (ms) | 913 [906–923] | 892 [858–918] | 968 [890–1086] | 893 [848–929] |
| latency p50 (ms) | 846 [834–856] | 671 [645–696] | 795 [679–951] | 691 [644–719] |
| host share, max/min executions | 1.00 [1.00–1.01] | 1.22 [1.14–1.31] | 1.06 [1.04–1.10] | 1.09 [1.04–1.12] |
| exception max_burns per run | 0 | 0 | 276 [267–287] | 127 [116–144] |
| exception share_capped per run | 0 | 0 | 16.7 [8–29] | 11.7 [11–13] |
| affinity hits per run | 0 | 785 [776–790] | 469 [453–485] | 640 [610–663] |
| requests per run | 881 | 911 | 885 | 903 |
| errors, client retries | 0, 0 | 0, 0 | 0, 0 | 0, 0 |

Hit rate with `prompt_cache_key` and with gateway-derived keys, D: 49.9% and 50.1%.

TTFT and latency are measured on a machine under other load; the hit rate, same-host share and burned nonce columns varied less between seeds than the timing columns.

## Cost of each bound

`max_burns`: at most that many `affinity_skip` burns per request. Burned nonces per request: 0.227 at 1, 0.577 at 2, 1.10 at 1000. Escrow spend per request over A: +1.4% at 1, +3.2% at 2, +4.6% at 1000.

`share_cap`: no burn cost. Scenario `unequal` (host 0 holds 4 of the 8 slots per escrow and has 4 times the workers and cache), seed 1 only, 5 hosts, same load. F is D without the share cap.

| metric | D cap 125 | F no cap | cap 100 | cap 110 |
|---|---|---|---|---|
| hit rate at hosts | 66.6% | 65.9% | 59.8% | 61.5% |
| follow-ups on the previous host | 77.4% | 75.3% | 61.9% | 69.2% |
| burned nonces per request | 0.323 | 0.329 | 0.224 | 0.252 |
| host share, max/min executions | 5.98 | 7.03 | 5.49 | 5.76 |
| exception share_capped per run | 35 | 0 | 305 | 108 |
| requests | 897 | 766 | 862 | 749 |

The slot ratio between host 0 and the others is 4. In the earlier run with the cap at 2 (200% in the present unit) `share_capped` was 0 on `unequal` with 3 seeds; with the cap at 1 (100%) it fired about 300 times per run.

`max_pin_age`: an earlier run with C and a pin age of 4 s on 45 s runs fired `pin_aged` 164 times per run [143–180] and gave hit rate 36.1% against 40.8% with a pin age of 1 h, same host 51.6% against 62.9%.

## Known effects

Validation replays warm the cache of the validating host and evict the pinned conversations' blocks. Earlier run, C, replays kept out of the caches: hit rate 49.6% [46.5–52.0] against 41.2% with replays in the same run; A stays at 22.1%.

## Reproduce

```
export GOTOOLCHAIN=go1.25.9
cd devshard
SEEDS="1 2 3" SCENARIOS=equal testenv/scripts/run-affinity-local.sh OUT_DIR
SEEDS="1" SCENARIOS=unequal testenv/scripts/run-affinity-local.sh OUT_DIR
```

The script builds the binaries into `OUT_DIR/bin`, runs the matrix in the script, with the settings and scenarios from `testenv/cmd/affinityrun/spec.go` (scenario `equal`: A B C D; `unequal`: D F G100 G110 G150; `noreplay`: A D; `pinage`: D P), and writes `OUT_DIR/tables.md`.

One run: `OUT_DIR/bin/affinityrun -bin OUT_DIR/bin -out RUN_DIR -config D -scenario equal -seed 1`.

## Not measured

Real vLLM or any GPU host, real prefix-cache block size and eviction order, whether a validator's cache is the executor's cache.

DevNet and mainnet traffic, conversation length and think-time distributions other than the one above, Zipf-skewed or churning participant sets, groups larger than 5 hosts.

Share cap values on `unequal` with more than one seed; the `noreplay` and `pinage` scenarios with `max_burns` 2.
