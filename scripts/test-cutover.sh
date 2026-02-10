#!/bin/bash
# Verify archive cutover end-to-end:
# 1) run one throughput benchmark
# 2) append probe events
# 3) confirm Postgres archived rows
# 4) confirm archive lag/tail converge to zero
#
# Assumes local 5-node cluster is already running.
# This script does NOT stop the cluster.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

wait_for_ready() {
  local port=$1
  local attempts=${2:-30}
  local interval=${3:-1}
  local url="http://localhost:${port}/health/ready"

  for ((i = 1; i <= attempts; i++)); do
    local health
    health=$(curl -s "$url" || true)
    if echo "$health" | grep -q '"status":"ok"'; then
      echo -e "  ✓ Node on port $port ready"
      return 0
    fi
    sleep "$interval"
  done

  echo -e "${RED}  ✗ Node on port $port not ready${NC}"
  return 1
}

require_cmd() {
  local name=$1

  if ! command -v "$name" >/dev/null 2>&1; then
    echo -e "${RED}Missing required command: ${name}${NC}"
    exit 1
  fi
}

echo -e "${GREEN}Running cutover verification...${NC}"
echo ""

require_cmd curl
require_cmd jq
require_cmd k6
require_cmd psql

echo "1. Waiting for cluster readiness..."
for port in 4000 4001 4002 4003 4004; do
  wait_for_ready "$port" 30 1
done
echo ""

CLUSTER_NODES="${CLUSTER_NODES:-http://localhost:4000/v1,http://localhost:4001/v1,http://localhost:4002/v1,http://localhost:4003/v1,http://localhost:4004/v1}"
RUN_ID="${RUN_ID:-cutover-verify-$(date +%s)}"
SUMMARY_PATH="logs/k6-cutover-${RUN_ID}.json"

# Keep defaults short so this stays repeatable as a verification check.
MAX_VUS="${MAX_VUS:-10}"
RAMP_DURATION="${RAMP_DURATION:-10s}"
STEADY_DURATION="${STEADY_DURATION:-20s}"
RAMP_DOWN_DURATION="${RAMP_DOWN_DURATION:-5s}"

echo "2. Running benchmark (hot-path throughput)..."
echo "   RUN_ID=${RUN_ID}"
echo "   MAX_VUS=${MAX_VUS} RAMP_DURATION=${RAMP_DURATION} STEADY_DURATION=${STEADY_DURATION} RAMP_DOWN_DURATION=${RAMP_DOWN_DURATION}"

CLUSTER_NODES="$CLUSTER_NODES" \
RUN_ID="$RUN_ID" \
MAX_VUS="$MAX_VUS" \
RAMP_DURATION="$RAMP_DURATION" \
STEADY_DURATION="$STEADY_DURATION" \
RAMP_DOWN_DURATION="$RAMP_DOWN_DURATION" \
  k6 run bench/k6/1-hot-path-throughput.js --summary-export "$SUMMARY_PATH"

append_passes=$(jq -r '.root_group.checks["append success"].passes // 0' "$SUMMARY_PATH")
append_fails=$(jq -r '.root_group.checks["append success"].fails // 0' "$SUMMARY_PATH")
append_p95=$(jq -r '.metrics.append_latency["p(95)"] // "n/a"' "$SUMMARY_PATH")
append_p99=$(jq -r '.metrics.append_latency["p(99)"] // "n/a"' "$SUMMARY_PATH")

if [ "$append_fails" -ne 0 ] || [ "$append_passes" -eq 0 ]; then
  echo -e "${RED}Benchmark append checks failed (passes=${append_passes}, fails=${append_fails})${NC}"
  exit 1
fi

append_p95_display="$append_p95"
append_p99_display="$append_p99"

if [ "$append_p95" != "n/a" ]; then
  append_p95_display="${append_p95}ms"
fi

if [ "$append_p99" != "n/a" ]; then
  append_p99_display="${append_p99}ms"
fi

echo -e "   ${GREEN}✓ Benchmark ok${NC} (append_success=${append_passes}, p95=${append_p95_display}, p99=${append_p99_display})"
echo ""

SESSION_ID="cutover-check-$(date +%s)"
PROBE_EVENTS="${PROBE_EVENTS:-40}"

echo "3. Appending probe session events..."
create_resp=$(curl -sS -X POST "http://localhost:4000/v1/sessions" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${SESSION_ID}\",\"metadata\":{\"cutover\":true,\"run_id\":\"${RUN_ID}\"}}")

if ! echo "$create_resp" | jq -e ".id == \"${SESSION_ID}\"" >/dev/null; then
  echo -e "${RED}Failed to create probe session${NC}"
  echo "Response: $create_resp"
  exit 1
fi

last_seq=0
for i in $(seq 1 "$PROBE_EVENTS"); do
  body=$(printf '{"type":"content","payload":{"text":"cutover-%s"},"actor":"agent:cutover","source":"cutover"}' "$i")
  resp=$(curl -sS -w "\n%{http_code}" -X POST "http://localhost:4000/v1/sessions/${SESSION_ID}/append" \
    -H "Content-Type: application/json" \
    -d "$body")
  code=$(echo "$resp" | tail -n1)
  json=$(echo "$resp" | sed '$d')

  if [ "$code" != "201" ]; then
    echo -e "${RED}Probe append failed at i=${i}${NC}"
    echo "Response: $json"
    exit 1
  fi

  seq=$(echo "$json" | jq -r '.seq')
  expected=$((last_seq + 1))

  if [ "$seq" -ne "$expected" ]; then
    echo -e "${RED}Probe sequence mismatch${NC}"
    echo "Expected seq=${expected}, got seq=${seq}"
    echo "Response: $json"
    exit 1
  fi

  last_seq="$seq"
done

echo -e "   ${GREEN}✓ Probe appended${NC} (session=${SESSION_ID}, last_seq=${last_seq})"
echo ""

raw_db_url="${DATABASE_URL:-${STARCITE_POSTGRES_URL:-postgres://postgres:postgres@localhost:5432/starcite_cutover}}"
db_url="${raw_db_url/#ecto:\/\//postgres://}"
postgres_timeout_sec="${POSTGRES_TIMEOUT_SEC:-120}"

echo "4. Waiting for Postgres archive convergence..."
pg_count=0
pg_min=0
pg_max=0

for ((attempt = 1; attempt <= postgres_timeout_sec; attempt++)); do
  row=$(psql "$db_url" -At -c "select count(*), coalesce(min(seq), 0), coalesce(max(seq), 0) from events where session_id='${SESSION_ID}'")
  pg_count=$(echo "$row" | cut -d'|' -f1)
  pg_min=$(echo "$row" | cut -d'|' -f2)
  pg_max=$(echo "$row" | cut -d'|' -f3)

  if [ "$pg_count" -eq "$PROBE_EVENTS" ] && [ "$pg_min" -eq 1 ] && [ "$pg_max" -eq "$PROBE_EVENTS" ]; then
    break
  fi

  if [ "$attempt" -eq "$postgres_timeout_sec" ]; then
    echo -e "${RED}Postgres did not converge in time${NC}"
    echo "count=${pg_count} min_seq=${pg_min} max_seq=${pg_max} expected=${PROBE_EVENTS}"
    exit 1
  fi

  sleep 1
done

echo -e "   ${GREEN}✓ Postgres archived${NC} (count=${pg_count}, min_seq=${pg_min}, max_seq=${pg_max})"
echo ""

raft_timeout_sec="${RAFT_TIMEOUT_SEC:-120}"
raft_output=""

echo "5. Waiting for Raft cutover state (archived_seq == last_seq and ETS drained on replicas)..."
for ((attempt = 1; attempt <= raft_timeout_sec; attempt++)); do
  verifier_name="cutover_verifier_${attempt}_$RANDOM@127.0.0.1"

  if raft_output=$(SESSION_ID="$SESSION_ID" elixir --hidden --name "$verifier_name" -e '
    sid = System.fetch_env!("SESSION_ID")
    target = :"node1@127.0.0.1"

    unless Node.connect(target) do
      IO.puts("CONNECT_FAILED")
      System.halt(2)
    end

    session_result = :rpc.call(target, Starcite.Runtime, :get_session, [sid], 5_000)
    events_result = :rpc.call(target, Starcite.Runtime, :get_events_from_cursor, [sid, 0, 1_000], 5_000)
    group_id = :rpc.call(target, Starcite.Runtime.RaftManager, :group_for_session, [sid], 5_000)
    replicas = :rpc.call(target, Starcite.Runtime.RaftManager, :replicas_for_group, [group_id], 5_000)

    sizes =
      Enum.map(replicas, fn node ->
        {node, :rpc.call(node, Starcite.Runtime.EventStore, :session_size, [sid], 5_000)}
      end)

    IO.puts("SESSION=#{inspect(session_result)}")
    IO.puts("EVENTS=#{inspect(events_result)}")
    IO.puts("GROUP_ID=#{inspect(group_id)}")
    IO.puts("REPLICAS=#{Enum.map_join(replicas, ",", &Atom.to_string/1)}")

    IO.puts(
      "SIZES=" <>
        Enum.map_join(sizes, ",", fn {node, size} -> "#{node}=#{inspect(size)}" end)
    )

    state_ok? =
      case {session_result, events_result} do
        {{:ok, session}, {:ok, events}} when is_map(session) and is_list(events) ->
          session.archived_seq == session.last_seq and events == [] and
            Enum.all?(sizes, fn {_node, size} -> size == 0 end)

        _ ->
          false
      end

    if state_ok?, do: System.halt(0), else: System.halt(1)
  ' 2>&1); then
    break
  fi

  if [ "$attempt" -eq "$raft_timeout_sec" ]; then
    echo -e "${RED}Raft cutover state did not converge in time${NC}"
    echo "$raft_output"
    exit 1
  fi

  sleep 1
done

echo -e "   ${GREEN}✓ Raft cutover confirmed${NC}"
echo "$raft_output"
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Cutover verification passed${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Run details:"
echo "  run_id: $RUN_ID"
echo "  probe_session: $SESSION_ID"
echo "  benchmark_summary: $SUMMARY_PATH"
echo ""
echo "Cluster is still running."
