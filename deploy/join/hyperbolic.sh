#!/usr/bin/env bash

# ===== config =====
TARGET_IP="185.216.21.98"
API_URL="https://tracker.gonka.vip/api/v1/inference/current"
# ==================

command -v curl >/dev/null || { echo "ERROR: curl not found"; exit 1; }
command -v jq   >/dev/null || { 
  echo "installing jq ..."
  sudo apt-get update && sudo apt-get install -y jq
}

echo "Detecting br-* interface ..."
BR_IF="$(ip -br link | awk '$1 ~ /^br-/ && $2=="UP" {print $1; exit}')"
if [[ -z "${BR_IF:-}" ]]; then
  echo "ERROR: no UP br-* interface found"
  exit 1
fi

echo "Fetching inference URLs from $API_URL ..."
JSON="$(curl -fsSL "$API_URL")"
IPS="$(printf '%s' "$JSON" \
  | jq -r '.participants[]?.inference_url? // empty' \
  | grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}' \
  | grep -E '^147\.185\.(40|41)\.' \
  | sort -u || true)"

if [[ -z "${IPS:-}" ]]; then
  echo "WARN: no one url found inference_url с 147.185.40.* или 147.185.41.* в $API_URL"
  echo "BR_IF=$BR_IF"
  exit 0
fi

echo "BR_IF=$BR_IF"
echo "TARGET_IP=$TARGET_IP"
echo "Found IPs:"
echo "$IPS" | sed 's/^/  - /'

echo "Checking for existing DNAT rules to delete..."
DEL_LINES="$(
  sudo iptables -t nat -L PREROUTING -n -v --line-numbers \
    | awk '$1 ~ /^[0-9]+$/ && $4=="DNAT" && $5=="tcp" {print $1, $10}' \
    | while read -r line dst; do
        echo "$IPS" | grep -qx "$dst" && echo "$line"
      done \
    | sort -rn || true
)"

echo "DEL_LINES=$DEL_LINES"
if [[ -n "${DEL_LINES:-}" ]]; then
  echo "Deleting PREROUTING DNAT lines:"
  echo "$DEL_LINES" | sed 's/^/  - /'
  while read -r n; do
    sudo iptables -t nat -D PREROUTING "$n"
  done <<< "$DEL_LINES"
else
  echo "No matching DNAT rules to delete."
fi

echo "Adding PREROUTING DNAT rules:"
while read -r ip; do
  [[ -z "$ip" ]] && continue
  sudo iptables -t nat -C PREROUTING -i "$BR_IF" -p tcp -d "$ip" -j DNAT --to-destination "$TARGET_IP" 2>/dev/null \
    || sudo iptables -t nat -A PREROUTING -i "$BR_IF" -p tcp -d "$ip" -j DNAT --to-destination "$TARGET_IP"
done <<< "$IPS"


echo "Adding POSTROUTING MASQUERADE rule:"
sudo iptables -t nat -C POSTROUTING -p tcp -d "$TARGET_IP" -j MASQUERADE 2>/dev/null \
  || sudo iptables -t nat -A POSTROUTING -p tcp -d "$TARGET_IP" -j MASQUERADE

echo "Done."
echo
sudo iptables -t nat -L PREROUTING -n -v --line-numbers | head -n 120
