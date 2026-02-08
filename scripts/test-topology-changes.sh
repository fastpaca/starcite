#!/bin/bash
# Test topology changes using session create/append primitives only.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

extract_seq() {
  echo "$1" | grep -o '"seq":[0-9]*' | head -n1 | cut -d':' -f2
}

wait_for_ready() {
  local url=$1
  local attempts=${2:-30}
  local interval=${3:-1}

  for ((i = 1; i <= attempts; i++)); do
    local health
    health=$(curl -sf "$url" || true)

    if echo "$health" | grep -q '"status":"ok"'; then
      echo -e "  ✓ ready after $((i * interval))s"
      return 0
    fi

    sleep "$interval"
  done

  echo -e "  ${YELLOW}⚠ not ready after $((attempts * interval))s${NC}"
  return 1
}

wait_for_live() {
  local url=$1
  local attempts=${2:-30}
  local interval=${3:-1}

  for ((i = 1; i <= attempts; i++)); do
    local health
    health=$(curl -sf "$url" || true)

    if echo "$health" | grep -q '"status":"ok"'; then
      echo -e "  ✓ live after $((i * interval))s"
      return 0
    fi

    sleep "$interval"
  done

  echo -e "  ${YELLOW}⚠ not live after $((attempts * interval))s${NC}"
  return 1
}

wait_for_node_down() {
  local node_pattern=$1
  local attempts=${2:-20}
  local interval=${3:-1}

  for ((i = 1; i <= attempts; i++)); do
    if ! pgrep -f "$node_pattern" > /dev/null; then
      echo -e "  ✓ ${node_pattern} stopped after $((i * interval))s"
      return 0
    fi

    sleep "$interval"
  done

  echo -e "  ${YELLOW}⚠ ${node_pattern} still running after $((attempts * interval))s; forcing kill${NC}"
  pkill -9 -f "$node_pattern" || true
  sleep 1

  if pgrep -f "$node_pattern" > /dev/null; then
    echo -e "  ${RED}✗ failed to stop ${node_pattern}${NC}"
    return 1
  fi

  echo -e "  ✓ ${node_pattern} force-stopped"
  return 0
}

append_expect() {
  local port=$1
  local session_id=$2
  local expected_seq=$3
  local text=$4

  local resp
  resp=$(curl -s -X POST "http://localhost:${port}/v1/sessions/${session_id}/append" \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"content\",\"payload\":{\"text\":\"${text}\"},\"actor\":\"agent:topology-test\",\"expected_seq\":${expected_seq}}")

  local seq
  seq=$(extract_seq "$resp")

  if [ -z "$seq" ]; then
    echo "$resp"
    return 1
  fi

  echo "$seq"
  return 0
}

echo -e "${GREEN}=== TOPOLOGY CHANGE TEST (5-node cluster) ===${NC}"
echo ""

echo "0. Waiting for baseline readiness..."
wait_for_ready "http://localhost:4000/health/ready" 30 1
wait_for_ready "http://localhost:4001/health/ready" 30 1
wait_for_ready "http://localhost:4002/health/ready" 30 1
wait_for_ready "http://localhost:4003/health/ready" 30 1
wait_for_ready "http://localhost:4004/health/ready" 30 1
echo ""

echo "1. Create test session and append initial event..."
SESSION_ID="topology-session-$(date +%s)"
CREATE_RESP=$(curl -s -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"$SESSION_ID\",\"metadata\":{}}")

if ! echo "$CREATE_RESP" | grep -q '"id"'; then
  echo -e "${RED}  ✗ failed to create session${NC}"
  echo "Response: $CREATE_RESP"
  exit 1
fi

SEQ=$(append_expect 4000 "$SESSION_ID" 0 "initial") || {
  echo -e "${RED}  ✗ initial append failed${NC}"
  exit 1
}

echo -e "  ${GREEN}✓ session created, seq=$SEQ${NC}"
echo ""

echo "2. Kill node3 and append via node2..."
NODE3_PIDS=$(pgrep -f "node3@127.0.0.1" || true)
if [ -z "$NODE3_PIDS" ]; then
  echo -e "${RED}  ✗ node3 not running${NC}"
  exit 1
fi
kill $NODE3_PIDS
wait_for_node_down "node3@127.0.0.1" 20 1
wait_for_ready "http://localhost:4001/health/ready" 20 1

NEXT_SEQ=$(append_expect 4001 "$SESSION_ID" "$SEQ" "after node3 down") || {
  echo -e "${RED}  ✗ append failed after node3 kill${NC}"
  exit 1
}
SEQ=$NEXT_SEQ
echo -e "  ${GREEN}✓ seq=$SEQ${NC}"
echo ""

echo "3. Kill node4 and append via node1 (3-node quorum path)..."
NODE4_PIDS=$(pgrep -f "node4@127.0.0.1" || true)
if [ -z "$NODE4_PIDS" ]; then
  echo -e "${RED}  ✗ node4 not running${NC}"
  exit 1
fi
kill $NODE4_PIDS
wait_for_node_down "node4@127.0.0.1" 20 1
wait_for_ready "http://localhost:4000/health/ready" 20 1

NEXT_SEQ=$(append_expect 4000 "$SESSION_ID" "$SEQ" "after node4 down") || {
  echo -e "${RED}  ✗ append failed with two nodes down${NC}"
  exit 1
}
SEQ=$NEXT_SEQ
echo -e "  ${GREEN}✓ seq=$SEQ${NC}"
echo ""

echo "4. Restart node3 and append via restarted node..."
wait_for_node_down "node3@127.0.0.1" 20 1
CLUSTER_NODES="node1@127.0.0.1,node2@127.0.0.1,node3@127.0.0.1,node4@127.0.0.1,node5@127.0.0.1" PORT=4002 elixir --name node3@127.0.0.1 -S mix phx.server > logs/node3-restart.log 2>&1 &
wait_for_live "http://localhost:4002/health/live" 45 1

NEXT_SEQ=$(append_expect 4002 "$SESSION_ID" "$SEQ" "after node3 restart") || {
  echo -e "${RED}  ✗ append failed on restarted node3${NC}"
  exit 1
}
SEQ=$NEXT_SEQ
echo -e "  ${GREEN}✓ node3 rejoined, seq=$SEQ${NC}"
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Topology primitive test complete${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
