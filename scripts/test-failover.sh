#!/bin/bash
# Test Raft leader failover with session create/append primitives.

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
  local attempts=${2:-20}
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

echo -e "${YELLOW}Testing failover with session primitives...${NC}"
echo ""

running_nodes=0
for node in node1@127.0.0.1 node2@127.0.0.1 node3@127.0.0.1 node4@127.0.0.1 node5@127.0.0.1; do
  if pgrep -f "$node" > /dev/null; then
    running_nodes=$((running_nodes + 1))
  fi
done

if [ $running_nodes -lt 3 ]; then
  echo -e "${RED}Cluster not running (only $running_nodes nodes).${NC}"
  echo "Start it first: ./scripts/start-cluster.sh"
  exit 1
fi

echo -e "${GREEN}Found $running_nodes running nodes${NC}"
echo ""

echo "1. Creating test session..."
SESSION_ID="failover-session-$(date +%s)"
CREATE_RESP=$(curl -s -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"$SESSION_ID\",\"metadata\":{}}")

if ! echo "$CREATE_RESP" | grep -q '"id"'; then
  echo -e "${RED}  ✗ failed to create session${NC}"
  echo "Response: $CREATE_RESP"
  exit 1
fi

echo -e "  ✓ Session created: $SESSION_ID"
echo ""

echo "2. Appending initial event on node1..."
RESP=$(curl -s -X POST "http://localhost:4000/v1/sessions/${SESSION_ID}/append" \
  -H "Content-Type: application/json" \
  -d '{"type":"content","payload":{"text":"before failover"},"actor":"agent:failover","expected_seq":0}')
SEQ=$(extract_seq "$RESP")

if [ "$SEQ" != "1" ]; then
  echo -e "${RED}  ✗ initial append failed${NC}"
  echo "Response: $RESP"
  exit 1
fi

echo -e "  ✓ seq=$SEQ"
echo ""

echo "3. Killing node1 (simulated failure)..."
NODE1_PIDS=$(pgrep -f "node1@127.0.0.1")
kill $NODE1_PIDS
echo -e "  ✓ Node1 killed (PIDs: $NODE1_PIDS)"
echo ""

echo "4. Waiting for cluster recovery via node2..."
wait_for_ready "http://localhost:4001/health/ready" 20 1
echo ""

echo "5. Appending via node2 after failover..."
RESP=$(curl -s -X POST "http://localhost:4001/v1/sessions/${SESSION_ID}/append" \
  -H "Content-Type: application/json" \
  -d '{"type":"content","payload":{"text":"after failover"},"actor":"agent:failover","expected_seq":1}')
SEQ=$(extract_seq "$RESP")

if [ "$SEQ" != "2" ]; then
  echo -e "${RED}  ✗ append after failover failed${NC}"
  echo "Response: $RESP"
  exit 1
fi

echo -e "  ${GREEN}✓ append succeeded, seq=$SEQ${NC}"
echo ""

echo "6. Appending from other surviving nodes..."
for port in 4002 4003 4004; do
  EXPECTED=$SEQ
  RESP=$(curl -s -X POST "http://localhost:${port}/v1/sessions/${SESSION_ID}/append" \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"content\",\"payload\":{\"text\":\"from ${port}\"},\"actor\":\"agent:failover\",\"expected_seq\":${EXPECTED}}")
  NEW_SEQ=$(extract_seq "$RESP")

  if [ -z "$NEW_SEQ" ]; then
    echo -e "${RED}  ✗ append failed on port $port${NC}"
    echo "Response: $RESP"
    exit 1
  fi

  SEQ=$NEW_SEQ
  echo -e "  ${GREEN}✓ port $port seq=$SEQ${NC}"
done
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Failover primitive test passed${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
