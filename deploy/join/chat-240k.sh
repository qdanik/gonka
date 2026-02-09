#!/usr/bin/env bash

# These tests control *PROMPT SIZE* (input tokens), not max_tokens.
# We generate large user messages on the fly.

MODEL="Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"
ENDPOINT="http://localhost:5050/v1/chat/completions"

# Helper: generate ~N tokens by repeating a short word (rough approximation)
# 1 English word ~= ~1 token for Qwen in practice.

gen_tokens () {
  python3 - <<PY
n=$1
print("hello " * n)
PY
}


##########################################
# === ~240k INPUT TOKENS (prompt size) ===
##########################################
PROMPT_240K=$(gen_tokens 240000)

curl -s -X POST "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg m \"$MODEL\" --arg p \"$PROMPT_240K\" '{
    model: $m,
    messages: [{role: "user", content: $p}],
    max_tokens: 16,
    seed: 42
  }')" | python3 -m json.tool

