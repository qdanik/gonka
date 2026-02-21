#!/usr/bin/env bash

URL="http://localhost:5050/v1/chat/completions"
MODEL="Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"

N=10
MAX_TOKENS=200000
REPEATS=100
MARK_EVERY=50
SEED_BASE=42000

BASE_BLOCK="You are a stress-test prompt.
Return a structured JSON with 200 items.
Each item must contain id, short_text, long_text (200+ words), checksum.
Use diverse vocabulary. Do not be concise.
---
"

hash() {
  echo "$1" | shasum -a 256 | awk '{print $1}'
}

for k in $(seq 1 $N); do
  TS=$(date +%s)
  RAND=$(openssl rand -hex 8)
  RUN_ID="run_${TS}_${k}_${RAND}"
  HASH=$(hash "$RUN_ID")

  PROMPT="UNIQUE_REQUEST_ID: $RUN_ID
UNIQUE_HASH: $HASH

"

  i=1
  while [ $i -le $REPEATS ]; do
    PROMPT="$PROMPT$BASE_BLOCK"
    if [ $((i % MARK_EVERY)) -eq 0 ]; then
      PROMPT="$PROMPT MARKER_$i $RUN_ID $HASH
"
    fi
    i=$((i+1))
  done

  SEED=$((SEED_BASE + k))

  PAYLOAD=$(python3 - <<EOF
import json
print(json.dumps({
  "model": "$MODEL",
  "messages": [{"role":"user","content": """$PROMPT"""}],
  "max_tokens": $MAX_TOKENS,
  "seed": $SEED,
  "stream": False
}))
EOF
)

  if curl -s \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD" \
    "$URL" >/dev/null
  then
    echo "[$k/$N] ответ получен"
  else
    echo "[$k/$N] ошибка"
  fi

done