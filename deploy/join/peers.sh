#!/bin/sh

RPC_URL="http://node1.gonka.ai:8000/chain-rpc/net_info"
CFG_FILE=".inference/config/config.toml"
MAX_MS=50
P2P_PORT=5000

command -v jq >/dev/null || { echo "jq not found"; exit 1; }

TMP_PEERS="$(mktemp)"
TMP_GOOD="$(mktemp)"
trap 'rm -f "$TMP_PEERS" "$TMP_GOOD"' EXIT

curl -fsS "$RPC_URL" \
| jq -r '
  .result.peers[]
  | select(.remote_ip != null and .remote_ip != "")
  | select(.node_info.id != null and .node_info.id != "")
  | "\(.node_info.id) \(.remote_ip)"
' | sort -u > "$TMP_PEERS"

echo "Peers found: $(wc -l < "$TMP_PEERS")"

while read -r peer_id ip; do
  ms="$(ping -c 1 -W 1 "$ip" 2>/dev/null \
        | sed -nE 's/.*time=([0-9.]+).*/\1/p' \
        | head -n1)"

  [ -z "$ms" ] && continue

  if awk -v a="$ms" -v b="$MAX_MS" 'BEGIN{exit !(a<b)}'; then
    echo "$peer_id@$ip:$P2P_PORT" >> "$TMP_GOOD"
    printf "%s ms - %s - %s\n" "$ms" "$ip" "$peer_id"
  fi
done < "$TMP_PEERS"

if [ -s "$TMP_GOOD" ]; then
  NEW_VALUE="$(sort -u "$TMP_GOOD" | paste -sd, -)"
else
  NEW_VALUE=""
fi

echo "persistent_peers:"
echo "$NEW_VALUE"

TMP_IDS="$(mktemp)"
trap 'rm -f "$TMP_PEERS" "$TMP_GOOD" "$TMP_IDS"' EXIT

printf '%s' "$NEW_VALUE" \
  | tr ',' '\n' \
  | sed 's/@.*//' \
  | sed '/^$/d' \
  | sort -u > "$TMP_IDS"

if [ -s "$TMP_IDS" ]; then
  IDS_VALUE="$(paste -sd, "$TMP_IDS")"
else
  IDS_VALUE=""
fi

echo "unconditional_peer_ids:"
echo "$IDS_VALUE"

sudo cp -a "$CFG_FILE" "$CFG_FILE.bak.$(date +%Y%m%d-%H%M%S)"

if sed --version >/dev/null 2>&1; then
  sudo sed -i "s/^persistent_peers[[:space:]]*=.*/persistent_peers = \"$NEW_VALUE\"/" "$CFG_FILE"
  sudo sed -i "s/^unconditional_peer_ids[[:space:]]*=.*/unconditional_peer_ids = \"$IDS_VALUE\"/" "$CFG_FILE"
else
  sudo sed -i "" "s/^persistent_peers[[:space:]]*=.*/persistent_peers = \"$NEW_VALUE\"/" "$CFG_FILE"
  sudo sed -i "" "s/^unconditional_peer_ids[[:space:]]*=.*/unconditional_peer_ids = \"$IDS_VALUE\"/" "$CFG_FILE"
fi

echo "Updated: $CFG_FILE"
