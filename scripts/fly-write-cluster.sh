#!/usr/bin/env bash
set -euo pipefail

DEFAULT_WRITE_NODE_COUNT=3
DEFAULT_REPLICATION_FACTOR=3
DEFAULT_NUM_GROUPS=256
DEFAULT_DRAIN_TIMEOUT_MS=30000
DEFAULT_READY_TIMEOUT_MS=60000
DEFAULT_WAIT_TIMEOUT_SECS=300
DEFAULT_VM_SIZE="shared-cpu-1x"
DEFAULT_VM_MEMORY_MB=1024
DEFAULT_CONFIG_PATH="fly.toml"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPS_SCRIPT="$SCRIPT_DIR/fly-write-node-ops.sh"

usage() {
  cat <<'EOF'
usage:
  scripts/fly-write-cluster.sh status <app>
  scripts/fly-write-cluster.sh nuke <app>
  scripts/fly-write-cluster.sh bootstrap <app> <image> <region> [write_node_count]
  scripts/fly-write-cluster.sh set-identities <app> [machine_id...]
  scripts/fly-write-cluster.sh roll-image <app> <image> [machine_id...]

behavior:
  - manages a static write-node set directly via Fly Machines
  - pins RELEASE_NODE, CLUSTER_NODES, and STARCITE_WRITE_NODE_IDS per machine
  - avoids fly deploy rolling replacement for write-node updates

environment overrides:
  STARCITE_WRITE_REPLICATION_FACTOR  (default: 3)
  STARCITE_NUM_GROUPS                (default: 256)
  FLY_WRITE_DRAIN_TIMEOUT_MS         (default: 30000)
  FLY_WRITE_READY_TIMEOUT_MS         (default: 60000)
  FLY_WRITE_WAIT_TIMEOUT_SECS        (default: 300)
  FLY_WRITE_VM_SIZE                  (default: shared-cpu-1x)
  FLY_WRITE_VM_MEMORY_MB             (default: 1024)
  FLY_WRITE_CONFIG                   (default: fly.toml)
  FLY_WRITE_SET_IDENTITIES_DETACH    (default: 1)
  FLY_WRITE_SET_IDENTITIES_STOP_START (default: 1)
  FLY_WRITE_ROLL_STOP_START          (default: 1)
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 1
  fi
}

skip_health_checks_enabled() {
  case "${FLY_WRITE_SKIP_HEALTH_CHECKS:-}" in
    1 | true | TRUE | yes | YES) return 0 ;;
    *) return 1 ;;
  esac
}

set_identities_detach_enabled() {
  case "${FLY_WRITE_SET_IDENTITIES_DETACH:-1}" in
    0 | false | FALSE | no | NO) return 1 ;;
    *) return 0 ;;
  esac
}

set_identities_stop_start_enabled() {
  case "${FLY_WRITE_SET_IDENTITIES_STOP_START:-1}" in
    0 | false | FALSE | no | NO) return 1 ;;
    *) return 0 ;;
  esac
}

roll_stop_start_enabled() {
  case "${FLY_WRITE_ROLL_STOP_START:-1}" in
    0 | false | FALSE | no | NO) return 1 ;;
    *) return 0 ;;
  esac
}

node_id_for_slot() {
  local app="$1"
  local machine_id="$2"
  local slot="$3"

  echo "starcite-${slot}@${machine_id}.vm.${app}.internal"
}

list_machine_ids_created_order() {
  local app="$1"

  fly machine list -a "$app" --json \
    | jq -r 'sort_by(.created_at) | .[].id'
}

list_machine_ids_slot_order() {
  local app="$1"

  fly machine list -a "$app" --json \
    | jq -r '
      sort_by(
        ((.config.metadata.starcite_slot // "9999") | tonumber? // 9999),
        .created_at
      )
      | .[].id
    '
}

read_machine_ids() {
  local app="$1"
  shift

  if [ "$#" -gt 0 ]; then
    printf '%s\n' "$@"
    return
  fi

  local ids
  ids="$(list_machine_ids_slot_order "$app")"
  if [ -n "$ids" ]; then
    printf '%s\n' "$ids"
    return
  fi

  list_machine_ids_created_order "$app"
}

machine_state() {
  local app="$1"
  local machine_id="$2"

  fly machine list -a "$app" --json \
    | jq -r --arg id "$machine_id" '.[] | select(.id == $id) | .state'
}

wait_for_machine_state() {
  local app="$1"
  local machine_id="$2"
  local desired_state="$3"
  local max_attempts="${4:-60}"

  local attempt=1
  while [ "$attempt" -le "$max_attempts" ]; do
    local state
    state="$(machine_state "$app" "$machine_id")"

    if [ "$state" = "$desired_state" ]; then
      return 0
    fi

    attempt=$((attempt + 1))
    sleep 1
  done

  local final_state
  final_state="$(machine_state "$app" "$machine_id")"
  echo "machine=$machine_id failed to reach state=$desired_state (last_state=$final_state)" >&2
  return 1
}

stop_machine() {
  local app="$1"
  local machine_id="$2"

  if ! fly machine stop -a "$app" "$machine_id"; then
    local state
    state="$(machine_state "$app" "$machine_id")"
    if [ "$state" != "stopped" ]; then
      return 1
    fi
  fi

  wait_for_machine_state "$app" "$machine_id" "stopped" 120
}

start_machine() {
  local app="$1"
  local machine_id="$2"

  if ! fly machine start -a "$app" "$machine_id"; then
    local state
    state="$(machine_state "$app" "$machine_id")"
    case "$state" in
      started | starting) ;;
      *) return 1 ;;
    esac
  fi

  wait_for_machine_state "$app" "$machine_id" "started" 180
}

status() {
  local app="$1"

  fly machine list -a "$app" --json \
    | jq -r '
      sort_by(
        ((.config.metadata.starcite_slot // "9999") | tonumber? // 9999),
        .created_at
      )
      | .[]
      | "slot=\(.config.metadata.starcite_slot // "-") id=\(.id) state=\(.state) ready=\(([.checks[]?.status] | if length == 0 then false else all(. == "passing") end)) node=\(.config.env.RELEASE_NODE // "-")"
    '
}

nuke() {
  local app="$1"

  local ids
  ids="$(list_machine_ids_created_order "$app")"

  if [ -z "$ids" ]; then
    echo "no machines found for app=$app"
    return
  fi

  while IFS= read -r machine_id; do
    [ -z "$machine_id" ] && continue
    echo "destroying machine: $machine_id"
    fly machine destroy -a "$app" "$machine_id" -f
  done <<<"$ids"
}

set_identities() {
  local app="$1"
  shift

  local replication_factor="${STARCITE_WRITE_REPLICATION_FACTOR:-$DEFAULT_REPLICATION_FACTOR}"
  local num_groups="${STARCITE_NUM_GROUPS:-$DEFAULT_NUM_GROUPS}"
  local wait_timeout_secs="${FLY_WRITE_WAIT_TIMEOUT_SECS:-$DEFAULT_WAIT_TIMEOUT_SECS}"

  mapfile -t machine_ids < <(read_machine_ids "$app" "$@")

  if [ "${#machine_ids[@]}" -eq 0 ]; then
    echo "no machines found for app=$app" >&2
    exit 1
  fi

  if [ "$replication_factor" -gt "${#machine_ids[@]}" ]; then
    echo "invalid replication factor: $replication_factor > machine count ${#machine_ids[@]}" >&2
    exit 1
  fi

  local node_ids=()
  local i=0
  for machine_id in "${machine_ids[@]}"; do
    local slot=$((i + 1))
    node_ids+=("$(node_id_for_slot "$app" "$machine_id" "$slot")")
    i=$((i + 1))
  done

  local cluster_nodes
  cluster_nodes="$(IFS=,; echo "${node_ids[*]}")"

  i=0
  for machine_id in "${machine_ids[@]}"; do
    local slot=$((i + 1))
    local release_node="${node_ids[$i]}"
    echo "setting slot=$slot machine=$machine_id release_node=$release_node"

    if set_identities_stop_start_enabled; then
      echo "stopping machine=$machine_id before identity update"
      stop_machine "$app" "$machine_id"
    fi

    local update_args=(
      fly machine update "$machine_id"
      -a "$app"
      -y
      --wait-timeout "$wait_timeout_secs"
      --env RELEASE_DISTRIBUTION=name
      --env RELEASE_NODE="$release_node"
      --env CLUSTER_NODES="$cluster_nodes"
      --env STARCITE_WRITE_NODE_IDS="$cluster_nodes"
      --env STARCITE_WRITE_REPLICATION_FACTOR="$replication_factor"
      --env STARCITE_NUM_GROUPS="$num_groups"
      --metadata starcite_role=write_node
      --metadata starcite_slot="$slot"
    )

    if set_identities_stop_start_enabled; then
      update_args+=(--skip-start)
    fi

    if skip_health_checks_enabled; then
      update_args+=(--skip-health-checks)
    fi

    if set_identities_detach_enabled && ! set_identities_stop_start_enabled; then
      update_args+=(--detach)
    fi

    "${update_args[@]}"

    if set_identities_stop_start_enabled; then
      echo "starting machine=$machine_id after identity update"
      start_machine "$app" "$machine_id"
    fi

    i=$((i + 1))
  done
}

wait_for_machine_count() {
  local app="$1"
  local expected_count="$2"

  local attempts=0
  local max_attempts=60

  while [ "$attempts" -lt "$max_attempts" ]; do
    local count
    count="$(fly machine list -a "$app" --json | jq 'length')"

    if [ "$count" -eq "$expected_count" ]; then
      return
    fi

    attempts=$((attempts + 1))
    sleep 2
  done

  echo "timed out waiting for machine count=$expected_count on app=$app" >&2
  exit 1
}

bootstrap() {
  local app="$1"
  local image="$2"
  local region="$3"
  local write_node_count="${4:-$DEFAULT_WRITE_NODE_COUNT}"

  local vm_size="${FLY_WRITE_VM_SIZE:-$DEFAULT_VM_SIZE}"
  local vm_memory_mb="${FLY_WRITE_VM_MEMORY_MB:-$DEFAULT_VM_MEMORY_MB}"
  local config_path="${FLY_WRITE_CONFIG:-$DEFAULT_CONFIG_PATH}"

  nuke "$app"

  local i=1
  while [ "$i" -le "$write_node_count" ]; do
    echo "creating write machine $i/$write_node_count in region=$region"

    fly machine run "$image" \
      -a "$app" \
      -r "$region" \
      -c "$config_path" \
      --vm-size "$vm_size" \
      --vm-memory "$vm_memory_mb" \
      --autostop off \
      --autostart true \
      --detach

    i=$((i + 1))
  done

  wait_for_machine_count "$app" "$write_node_count"
  set_identities "$app"
  status "$app"
}

roll_image() {
  local app="$1"
  local image="$2"
  shift 2

  local drain_timeout_ms="${FLY_WRITE_DRAIN_TIMEOUT_MS:-$DEFAULT_DRAIN_TIMEOUT_MS}"
  local ready_timeout_ms="${FLY_WRITE_READY_TIMEOUT_MS:-$DEFAULT_READY_TIMEOUT_MS}"
  local wait_timeout_secs="${FLY_WRITE_WAIT_TIMEOUT_SECS:-$DEFAULT_WAIT_TIMEOUT_SECS}"

  if [ ! -x "$OPS_SCRIPT" ]; then
    echo "missing executable helper: $OPS_SCRIPT" >&2
    exit 1
  fi

  mapfile -t machine_ids < <(read_machine_ids "$app" "$@")

  if [ "${#machine_ids[@]}" -eq 0 ]; then
    echo "no machines found for app=$app" >&2
    exit 1
  fi

  for machine_id in "${machine_ids[@]}"; do
    echo "rolling machine=$machine_id image=$image"
    "$OPS_SCRIPT" pre-roll "$app" "$machine_id" "$drain_timeout_ms"

    if roll_stop_start_enabled; then
      echo "stopping machine=$machine_id before image update"
      stop_machine "$app" "$machine_id"
    fi

    local update_args=(
      fly machine update "$machine_id"
      -a "$app"
      -y
      --image "$image"
      --wait-timeout "$wait_timeout_secs"
    )

    if roll_stop_start_enabled; then
      update_args+=(--skip-start)
    fi

    if skip_health_checks_enabled; then
      update_args+=(--skip-health-checks)
    fi

    "${update_args[@]}"

    if roll_stop_start_enabled; then
      echo "starting machine=$machine_id after image update"
      start_machine "$app" "$machine_id"
    fi

    "$OPS_SCRIPT" post-roll "$app" "$machine_id" "$ready_timeout_ms"
  done
}

main() {
  require_cmd fly
  require_cmd jq

  local command="${1:-}"

  case "$command" in
    status)
      if [ "$#" -ne 2 ]; then
        usage
        exit 1
      fi
      status "$2"
      ;;
    nuke)
      if [ "$#" -ne 2 ]; then
        usage
        exit 1
      fi
      nuke "$2"
      ;;
    bootstrap)
      if [ "$#" -lt 4 ] || [ "$#" -gt 5 ]; then
        usage
        exit 1
      fi
      bootstrap "$2" "$3" "$4" "${5:-$DEFAULT_WRITE_NODE_COUNT}"
      ;;
    set-identities)
      if [ "$#" -lt 2 ]; then
        usage
        exit 1
      fi
      shift
      set_identities "$@"
      ;;
    roll-image)
      if [ "$#" -lt 3 ]; then
        usage
        exit 1
      fi
      shift
      roll_image "$@"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
