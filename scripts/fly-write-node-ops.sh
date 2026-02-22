#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage:
  scripts/fly-write-node-ops.sh status <app> <machine_id>
  scripts/fly-write-node-ops.sh pre-roll <app> <machine_id> [timeout_ms]
  scripts/fly-write-node-ops.sh post-roll <app> <machine_id> [timeout_ms]
  scripts/fly-write-node-ops.sh drain <app> <machine_id> [timeout_ms]
  scripts/fly-write-node-ops.sh undrain <app> <machine_id> [timeout_ms]

notes:
  - requires flyctl and ssh access to the target machine
  - runs Starcite control-plane ops through release RPC inside the machine
  - does not restart machines; run your deploy/restart command between pre-roll and post-roll
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 1
  fi
}

allow_unready() {
  case "${FLY_WRITE_ALLOW_UNREADY:-0}" in
    1 | true | TRUE | yes | YES) return 0 ;;
    *) return 1 ;;
  esac
}

rpc() {
  local app="$1"
  local machine_id="$2"
  local expr="$3"
  local max_attempts="${FLY_WRITE_RPC_MAX_ATTEMPTS:-30}"
  local sleep_seconds="${FLY_WRITE_RPC_SLEEP_SECONDS:-2}"
  local attempt=1

  while true; do
    if fly ssh console -a "$app" --machine "$machine_id" -C "/app/starcite/bin/starcite rpc \"$expr\""; then
      return 0
    fi

    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "rpc failed for machine=$machine_id after ${max_attempts} attempts" >&2
      return 1
    fi

    echo "rpc retry ${attempt}/${max_attempts} for machine=$machine_id" >&2
    attempt=$((attempt + 1))
    sleep "$sleep_seconds"
  done
}

wait_local_ready_or_continue() {
  local app="$1"
  local machine_id="$2"
  local timeout_ms="$3"

  if rpc "$app" "$machine_id" "Starcite.ControlPlane.Ops.wait_local_ready(${timeout_ms})"; then
    return 0
  fi

  if allow_unready; then
    echo "continuing despite wait_local_ready failure for machine=$machine_id" >&2
    return 0
  fi

  return 1
}

status() {
  local app="$1"
  local machine_id="$2"

  rpc "$app" "$machine_id" "IO.inspect(Starcite.ControlPlane.Ops.status(), pretty: true, limit: :infinity)"
}

drain() {
  local app="$1"
  local machine_id="$2"
  local timeout_ms="$3"

  rpc "$app" "$machine_id" "Starcite.ControlPlane.Ops.drain_node()"
  rpc "$app" "$machine_id" "Starcite.ControlPlane.Ops.wait_local_drained(${timeout_ms})"
}

undrain() {
  local app="$1"
  local machine_id="$2"
  local timeout_ms="$3"

  rpc "$app" "$machine_id" "Starcite.ControlPlane.Ops.undrain_node()"
  rpc "$app" "$machine_id" "Starcite.ControlPlane.Ops.wait_local_ready(${timeout_ms})"
}

pre_roll() {
  local app="$1"
  local machine_id="$2"
  local timeout_ms="$3"

  echo "[$machine_id] drain + wait_local_drained(${timeout_ms})"
  drain "$app" "$machine_id" "$timeout_ms"
}

post_roll() {
  local app="$1"
  local machine_id="$2"
  local timeout_ms="$3"

  echo "[$machine_id] wait_local_ready(${timeout_ms}) + undrain + wait_local_ready(${timeout_ms})"
  wait_local_ready_or_continue "$app" "$machine_id" "$timeout_ms"
  undrain "$app" "$machine_id" "$timeout_ms"
  wait_local_ready_or_continue "$app" "$machine_id" "$timeout_ms"
}

main() {
  require_cmd fly

  local command="${1:-}"

  case "$command" in
    status)
      if [ "$#" -ne 3 ]; then
        usage
        exit 1
      fi
      status "$2" "$3"
      ;;
    pre-roll)
      if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
        usage
        exit 1
      fi
      pre_roll "$2" "$3" "${4:-30000}"
      ;;
    post-roll)
      if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
        usage
        exit 1
      fi
      post_roll "$2" "$3" "${4:-60000}"
      ;;
    drain)
      if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
        usage
        exit 1
      fi
      drain "$2" "$3" "${4:-30000}"
      ;;
    undrain)
      if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
        usage
        exit 1
      fi
      undrain "$2" "$3" "${4:-60000}"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
