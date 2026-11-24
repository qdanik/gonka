#!/usr/bin/env bash
# Blue-green upgrade of the gateway with escrow handover. See README.md, "Upgrading without downtime".
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./upgrade.sh --image IMAGE [--drain-timeout SECONDS] [--ready-timeout SECONDS] [--skip-parked]

  --image IMAGE            the gateway image the idle colour starts on (required)
  --drain-timeout SECONDS  longest wait for an escrow to release its session and, separately, for the old
                           colour's open requests and owed timeout votes to finish (default 900)
  --ready-timeout SECONDS  longest wait for the new colour to answer /v1/models (default 300)
  --skip-parked            leave parked escrows on the old colour's storage instead of refusing the upgrade

Reads DEVSHARD_ADMIN_API_KEY from DEVSHARD_ENV_FILE (default ./config.devshard-gateway.env).
USAGE
}

COMPOSE_FILE=docker-compose.devshard-gateway.bluegreen.yml
ENV_FILE=${DEVSHARD_ENV_FILE:-./config.devshard-gateway.env}
ACTIVE_DIR=nginx-active
ACTIVE_CONF=$ACTIVE_DIR/active.conf
STORAGE_PREFIX=escrow-
HANDOVER_ROOT=/handover
POLL_SECONDS=2
CALL_TIMEOUT=30
DEFAULT_DRAIN_TIMEOUT=900
DEFAULT_READY_TIMEOUT=300
ALIVE_TIMEOUT=30

IMAGE=
DRAIN_TIMEOUT=$DEFAULT_DRAIN_TIMEOUT
READY_TIMEOUT=$DEFAULT_READY_TIMEOUT
SKIP_PARKED=0

while (($#)); do
  case $1 in
    --image) IMAGE=${2:?--image needs a value}; shift 2 ;;
    --drain-timeout) DRAIN_TIMEOUT=${2:?--drain-timeout needs a value}; shift 2 ;;
    --ready-timeout) READY_TIMEOUT=${2:?--ready-timeout needs a value}; shift 2 ;;
    --skip-parked) SKIP_PARKED=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done
[[ -n $IMAGE ]] || { usage >&2; exit 2; }
[[ $DRAIN_TIMEOUT =~ ^[0-9]+$ && $READY_TIMEOUT =~ ^[0-9]+$ ]] || { echo "timeouts are whole seconds" >&2; exit 2; }

cd "$(dirname "$0")"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { printf 'upgrade: %s\n' "$*" >&2; exit 1; }

for tool in docker curl jq; do
  command -v "$tool" >/dev/null || die "$tool is required"
done

env_value() {
  local line
  line=$(grep -E "^(export[[:space:]]+)?$1=" "$ENV_FILE" | tail -n 1 || true)
  line=${line#*=}
  line=${line%$'\r'}
  line=${line#[\'\"]}
  line=${line%[\'\"]}
  printf '%s' "$line"
}

[[ -r $ENV_FILE ]] || die "cannot read $ENV_FILE"
ADMIN_KEY=$(env_value DEVSHARD_ADMIN_API_KEY)
[[ -n $ADMIN_KEY ]] || die "DEVSHARD_ADMIN_API_KEY is empty in $ENV_FILE, so the admin routes are off"
[[ $ADMIN_KEY =~ ^[A-Za-z0-9._~+/=-]+$ ]] || die "DEVSHARD_ADMIN_API_KEY holds characters this script does not pass to curl"

RESPONSE_STATUS=
RESPONSE_BODY=

# The bearer goes to curl through a descriptor, never through its arguments or the output.
call() {
  local port=$1 method=$2 path=$3 body=${4-} output
  local args=(--silent --show-error --max-time "$CALL_TIMEOUT" --write-out $'\n%{http_code}' --request "$method")
  if [[ -n $body ]]; then
    args+=(--header 'Content-Type: application/json' --data-binary @-)
  fi
  output=$(curl "${args[@]}" -K <(printf 'header = "Authorization: Bearer %s"\n' "$ADMIN_KEY") \
    "http://127.0.0.1:$port$path" <<<"$body") || return 1
  RESPONSE_STATUS=${output##*$'\n'}
  RESPONSE_BODY=${output%$'\n'*}
}

admin() {
  call "$@" || die "no answer from port $1 for $2 $3"
  [[ $RESPONSE_STATUS == 2* ]] || die "$2 $3 on port $1 answered $RESPONSE_STATUS: $RESPONSE_BODY"
}

colour_port() { "${COMPOSE[@]}" port "gateway-$1" 8080 | sed 's/.*://'; }
service_running() { [[ -n $("${COMPOSE[@]}" ps --status running --quiet "$1") ]]; }

wait_until() { # description timeout command...
  local description=$1 timeout=$2 deadline
  shift 2
  deadline=$((SECONDS + timeout))
  until "$@"; do
    if ((SECONDS >= deadline)); then
      printf 'upgrade: gave up waiting for %s after %ss\n' "$description" "$timeout" >&2
      return 1
    fi
    sleep "$POLL_SECONDS"
  done
}

answers_models() { call "$1" GET /v1/models 2>/dev/null && [[ $RESPONSE_STATUS == 200 ]]; }

has_no_runtime() { # port id
  call "$1" GET /v1/admin/devshards 2>/dev/null && [[ $RESPONSE_STATUS == 200 ]] &&
    jq -e --arg id "$2" '.devshards | map(select(.id == $id)) | length == 1 and all(.runtime == null)' <<<"$RESPONSE_BODY" >/dev/null
}

open_work() { # port: "<open requests> <timeout votes owed>" from one scrape
  curl --silent --show-error --max-time "$CALL_TIMEOUT" "http://127.0.0.1:$1/metrics" |
    awk '
      $1 == "devshard_gateway_inflight_requests" { requests = $2 + 0; seenRequests = 1 }
      $1 == "devshard_gateway_owed_timeout_votes" { votes = $2 + 0; seenVotes = 1 }
      END { if (!seenRequests || !seenVotes) exit 1; print requests, votes }'
}

drained() { [[ $(open_work "$1") == "0 0" ]]; }

served_colour() { "${COMPOSE[@]}" exec -T nginx wget -qO- http://127.0.0.1/_colour; }
serves_colour() { [[ $(served_colour 2>/dev/null) == "$1" ]]; }

# The new colour answers /v1/models and runs every escrow imported active that it has not retired itself.
new_colour_serves() {
  answers_models "$NEW_PORT" || return 1
  call "$NEW_PORT" GET /v1/admin/devshards 2>/dev/null && [[ $RESPONSE_STATUS == 200 ]] &&
    jq -e '.devshards as $rows | all($ARGS.positional[]; . as $id | any($rows[];
      .id == $id and (.runtime != null or (.active | not) or .gone_from_chain or .settlement_pending or ((.settle_tx_hash // "") != ""))))' \
      --args ${IMPORTED_SERVING[@]+"${IMPORTED_SERVING[@]}"} <<<"$RESPONSE_BODY" >/dev/null
}

wait_for_new_colour() { wait_until "$NEW to run the escrows imported to it" "$ALIVE_TIMEOUT" new_colour_serves; }

require_new_colour() { wait_for_new_colour || die "$NEW does not run the escrows imported to it, $1"; }

switch_nginx() {
  "${COMPOSE[@]}" exec -T nginx nginx -t >/dev/null 2>&1 || return 1
  "${COMPOSE[@]}" exec -T nginx nginx -s reload >/dev/null 2>&1
}

place_active_conf() { # source
  cp "$1" "$ACTIVE_CONF.next"
  mv "$ACTIVE_CONF.next" "$ACTIVE_CONF"
}

[[ -f $ACTIVE_CONF ]] || die "$ACTIVE_CONF is missing: the first start is in README.md, \"Upgrading without downtime\""
OLD=$(grep -Eo 'gateway-(blue|green)' "$ACTIVE_CONF" | head -n 1 | cut -d- -f2 || true)
[[ $OLD == blue || $OLD == green ]] || die "$ACTIVE_CONF names neither colour"
NEW=blue
[[ $OLD == blue ]] && NEW=green
service_running "gateway-$OLD" || die "the active colour $OLD is not running"
service_running nginx || die "nginx is not running"
! service_running "gateway-$NEW" || die "the idle colour $NEW is running; stop it first (docker compose -f $COMPOSE_FILE stop gateway-$NEW)"

OLD_PORT=$(colour_port "$OLD")
answers_models "$OLD_PORT" || die "the active colour $OLD does not answer /v1/models"
open_work "$OLD_PORT" >/dev/null || die "the active colour $OLD does not export devshard_gateway_inflight_requests and devshard_gateway_owed_timeout_votes, so the script cannot tell when it is safe to stop"

NEW_STARTED=0
SWITCHED=0
FINISHED=0
SETTINGS_CHANGED=0
CONF_PLACED=0
DEACTIVATED=()
IMPORTED=()
IMPORTED_SERVING=()
OLD_SETTINGS=
PREVIOUS_CONF=$(mktemp)

reactivate() { # id deadline: the old colour refuses with 409 "still draining" while the earlier session ends its requests and refusal votes
  local id=$1 deadline=$2
  until RESPONSE_STATUS= RESPONSE_BODY= && call "$OLD_PORT" POST "/v1/admin/devshards/$id/activate" && [[ $RESPONSE_STATUS == 2* ]]; do
    [[ $RESPONSE_STATUS == 409 && $RESPONSE_BODY == *"still draining"* ]] && ((SECONDS < deadline)) || return 1
    sleep "$POLL_SECONDS"
  done
}

report_after_switch() {
  local left_on_old id idle_on_old= untouched_on_old= unconfirmed= still_on_old
  left_on_old=$(jq -nr --argjson handed "$HANDOVER" '($handed.first + $handed.rest | map(.id)) - $ARGS.positional | join(" ")' \
    --args ${IMPORTED[@]+"${IMPORTED[@]}"})
  for id in $left_on_old; do
    if [[ -d data/$NEW/$STORAGE_PREFIX$id ]]; then
      unconfirmed+=" $id"
    elif [[ " ${DEACTIVATED[*]-} " == *" $id "* ]]; then
      idle_on_old+=" $id"
    else
      untouched_on_old+=" $id"
    fi
  done
  still_on_old=$idle_on_old$untouched_on_old
  if wait_for_new_colour 2>/dev/null; then
    log "failed after the switch: nothing is rolled back; $NEW serves, $OLD still runs with what is left of its escrows, and rotation_enabled stays off on $OLD; imported so far: ${IMPORTED[*]-none}; still on $OLD:${still_on_old:- none}; copy in data/$NEW with no confirmed import (look at its list before importing again):${unconfirmed:- none} (README.md, \"Upgrading without downtime\")"
    return
  fi
  log "failed after the switch and $NEW does not run every escrow imported to it: nginx sends every request to $NEW, so those models are not answered, and nothing is rolled back; rotation_enabled stays off on $OLD, and on $NEW it is off unless the script had turned it on"
  log "imported to $NEW: ${IMPORTED[*]-none}"
  log "copy in data/$NEW with no confirmed import (treat as imported):${unconfirmed:- none}"
  log "deactivated on $OLD, not copied (inactive, no colour serves them):${idle_on_old:- none}"
  log "not yet handed over, unchanged on $OLD (which nginx does not reach):${untouched_on_old:- none}"
  log "recover: 1. read docker compose -f $COMPOSE_FILE ps and logs gateway-$NEW, then restart it (docker compose -f $COMPOSE_FILE restart gateway-$NEW); it reloads its imported escrows from data/$NEW and continues their nonces; 2. GET /v1/admin/devshards on port $NEW_PORT: an active row with no runtime is still unserved"
  log "never activate an imported or copied escrow on $OLD: $NEW holds its session and may have spent nonces that $OLD's copy would send again; do not run upgrade.sh again or move data/$NEW aside; hand over the others by hand once $NEW serves (docs/runbook.md, \"Upgrade\")"
}

rollback() {
  local status=$? id
  trap - EXIT
  if ((FINISHED || SWITCHED)); then
    rm -f "$PREVIOUS_CONF"
    set +e
    ((FINISHED)) || report_after_switch
    exit "$status"
  fi
  set +e
  log "failed before the switch: rolling back, $OLD keeps serving"
  if ((CONF_PLACED)); then
    place_active_conf "$PREVIOUS_CONF"
    switch_nginx
  fi
  if ((NEW_STARTED)); then
    if "${COMPOSE[@]}" stop "gateway-$NEW" >/dev/null; then
      mv "data/$NEW" "data/$NEW.retired-failed-$(date -u +%Y%m%dT%H%M%SZ)"
      mkdir -p "data/$NEW"
    else
      log "gateway-$NEW did not stop, so no escrow is reactivated on $OLD; stop it, then activate these on $OLD: ${DEACTIVATED[*]-none}"
      DEACTIVATED=()
    fi
  fi
  local reactivate_deadline=$((SECONDS + DRAIN_TIMEOUT))
  for id in ${DEACTIVATED[@]+"${DEACTIVATED[@]}"}; do
    reactivate "$id" "$reactivate_deadline" || log "escrow $id is not active on $OLD again ($RESPONSE_STATUS: $RESPONSE_BODY); its row reads inactive, so no colour serves it until you activate it on $OLD"
  done
  if ((SETTINGS_CHANGED)); then
    call "$OLD_PORT" PUT /v1/admin/settings "$OLD_SETTINGS"
    [[ $RESPONSE_STATUS == 2* ]] || log "the settings of $OLD were not restored ($RESPONSE_STATUS)"
  fi
  rm -f "$PREVIOUS_CONF"
  exit "$status"
}
trap rollback EXIT

log "upgrading: $OLD (port $OLD_PORT) -> $NEW on $IMAGE"

admin "$OLD_PORT" GET /v1/admin/settings
OLD_SETTINGS=$RESPONSE_BODY
admin "$OLD_PORT" GET /v1/admin/devshards
PARKED=$(jq -r '.devshards[] | select(.settlement_pending or ((.settle_tx_hash // "") != "")) | .id' <<<"$RESPONSE_BODY")
if [[ -n $PARKED ]]; then
  if ((SKIP_PARKED)); then
    log "leaving parked escrows on $OLD, which keeps running until they settle: $(tr '\n' ' ' <<<"$PARKED")"
  else
    die "parked escrows cannot be handed over: $(tr '\n' ' ' <<<"$PARKED")(settle them, or pass --skip-parked)"
  fi
fi

STORAGE_DIR=data/$NEW
if [[ -d $STORAGE_DIR && -n $(ls -A "$STORAGE_DIR") ]]; then
  RETIRED=$STORAGE_DIR.retired-$(date -u +%Y%m%dT%H%M%SZ)
  mv "$STORAGE_DIR" "$RETIRED"
  log "the idle colour's old storage is kept at $RETIRED"
fi
mkdir -p "$STORAGE_DIR" "data/$OLD" "$ACTIVE_DIR"

log "starting gateway-$NEW"
NEW_STARTED=1
env "GATEWAY_$(tr '[:lower:]' '[:upper:]' <<<"$NEW")_IMAGE=$IMAGE" "${COMPOSE[@]}" up -d --no-deps "gateway-$NEW" >/dev/null
NEW_PORT=$(colour_port "$NEW")
wait_until "gateway-$NEW to answer /v1/models" "$READY_TIMEOUT" answers_models "$NEW_PORT" ||
  die "gateway-$NEW did not answer /v1/models within ${READY_TIMEOUT}s"

NEW_SETTINGS=$(jq -c 'del(.rotation_enabled)' <<<"$OLD_SETTINGS")
admin "$NEW_PORT" PUT /v1/admin/settings "$NEW_SETTINGS"
ROTATION_WAS_ON=$(jq -r '.rotation_enabled // false' <<<"$OLD_SETTINGS")
if [[ $ROTATION_WAS_ON == true ]]; then
  SETTINGS_CHANGED=1
  admin "$OLD_PORT" POST /v1/admin/settings '{"rotation_enabled": false}'
  log "rotation is off on $OLD for the handover"
fi

admin "$OLD_PORT" GET /v1/admin/devshards
HANDOVER=$(jq -c '
  [.devshards[] | select((.gone_from_chain | not) and (.settlement_pending | not) and ((.settle_tx_hash // "") == ""))
    | {id, model, private_key_env, active, rotation_role, rotation_epoch, route_prefix: (.route_prefix // "")}] as $rows
  | ($rows | map(select(.active)) | group_by(.model) | map(sort_by([(.rotation_role == "reserve"), .id]) | .[0])) as $first
  | {first: $first, rest: ($rows - $first)}' <<<"$RESPONSE_BODY")

hand_over() { # wave name
  local wave=$1 row id deadline active
  local ids=()
  while IFS= read -r row; do
    id=$(jq -r .id <<<"$row")
    active=$(jq -r .active <<<"$row")
    if [[ $active == true ]]; then
      DEACTIVATED+=("$id")
      CALL_TIMEOUT=$((DRAIN_TIMEOUT + CALL_TIMEOUT)) admin "$OLD_PORT" POST "/v1/admin/devshards/$id/deactivate?handover=true"
    fi
    ids+=("$id")
  done < <(jq -c ".$wave[]" <<<"$HANDOVER")
  deadline=$((SECONDS + DRAIN_TIMEOUT))
  for id in ${ids[@]+"${ids[@]}"}; do
    wait_until "escrow $id to release its session on $OLD" $((deadline - SECONDS)) has_no_runtime "$OLD_PORT" "$id" ||
      die "escrow $id still holds its session on $OLD after ${DRAIN_TIMEOUT}s"
  done
  while IFS= read -r row; do
    id=$(jq -r .id <<<"$row")
    active=$(jq -r .active <<<"$row")
    admin "$OLD_PORT" GET /v1/admin/devshards
    jq -e --arg id "$id" '.devshards[] | select(.id == $id) | (.gone_from_chain or .settlement_pending or ((.settle_tx_hash // "") != "")) | not' \
      <<<"$RESPONSE_BODY" >/dev/null || die "escrow $id was parked or left the chain on $OLD during the handover; not importing it"
    if [[ $active != true && ! -d data/$OLD/$STORAGE_PREFIX$id ]]; then
      admin "$NEW_PORT" POST /v1/admin/devshards "$(jq -c '{id, model, private_key_env, activate: false}' <<<"$row")"
    else
      admin "$NEW_PORT" POST /v1/admin/devshards/import "$(jq -c --arg source "$HANDOVER_ROOT/$OLD/$STORAGE_PREFIX$id" \
        '{id, model, private_key_env, active, rotation_role, rotation_epoch, route_prefix, source_path: $source}' <<<"$row")"
      [[ $active != true ]] || IMPORTED_SERVING+=("$id")
    fi
    IMPORTED+=("$id")
    log "escrow $id handed over to $NEW"
  done < <(jq -c ".$wave[]" <<<"$HANDOVER")
}

log "wave 1: one escrow per model"
hand_over first

require_new_colour "so nginx is not switched"

log "switching nginx to $NEW"
cp "$ACTIVE_CONF" "$PREVIOUS_CONF"
place_active_conf "nginx/colour-$NEW.conf"
CONF_PLACED=1
switch_nginx || die "nginx refused the new upstream or the reload failed"
SWITCHED=1
wait_until "nginx to serve $NEW" "$READY_TIMEOUT" serves_colour "$NEW" || die "nginx did not report $NEW after the reload"
require_new_colour "after the switch"

log "wave 2: the remaining escrows"
hand_over rest

if [[ $ROTATION_WAS_ON == true ]]; then
  admin "$NEW_PORT" POST /v1/admin/settings '{"rotation_enabled": true}'
  log "rotation is on again on $NEW"
fi

admin "$OLD_PORT" GET /v1/admin/devshards
LEFTOVER=$(jq -r --argjson handed "$HANDOVER" '
  ($handed.first + $handed.rest | map(.id)) as $ids
  | .devshards[] | select((.id as $id | $ids | index($id) | not) and (.gone_from_chain | not) and (.settlement_pending | not) and ((.settle_tx_hash // "") == "")) | .id' <<<"$RESPONSE_BODY")
[[ -z $LEFTOVER ]] || die "escrows appeared on $OLD during the handover and were not moved: $(tr '\n' ' ' <<<"$LEFTOVER")(not stopping it)"
PARKED=$(jq -r '.devshards[] | select(.settlement_pending or ((.settle_tx_hash // "") != "")) | .id' <<<"$RESPONSE_BODY")
if [[ -n $PARKED ]] && ((! SKIP_PARKED)); then
  die "escrows were parked on $OLD during the handover: $(tr '\n' ' ' <<<"$PARKED")(not stopping it; settle them, then stop $OLD)"
fi

if [[ -z $PARKED ]]; then
  log "waiting for the open requests and the owed timeout votes on $OLD to finish"
  if wait_until "$OLD's requests and owed timeout votes to finish" "$DRAIN_TIMEOUT" drained "$OLD_PORT"; then
    log "$OLD is idle and owes no votes"
  else
    log "$OLD still has (open requests, owed timeout votes) = ($(open_work "$OLD_PORT" 2>/dev/null || echo unknown)) after ${DRAIN_TIMEOUT}s; stopping it cuts the requests and leaves those votes unposted, so finalization charges their nonces in full"
  fi
fi
require_new_colour "so $OLD is not stopped"
FINISHED=1
rm -f "$PREVIOUS_CONF"
if [[ -n $PARKED ]]; then
  log "done: $NEW serves on $IMAGE; $OLD keeps running to settle its parked escrows, stop it once they have settled (docker compose -f $COMPOSE_FILE stop gateway-$OLD)"
  exit 0
fi
"${COMPOSE[@]}" stop "gateway-$OLD" >/dev/null
log "done: $NEW serves on $IMAGE; $OLD is stopped and its storage stays in data/$OLD until the next upgrade moves it aside"
