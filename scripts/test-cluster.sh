#!/bin/bash
# Smoke test for 5-node cluster routing using session primitives only.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
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

extract_seq() {
  echo "$1" | grep -o '"seq":[0-9]*' | head -n1 | cut -d':' -f2
}

echo -e "${GREEN}Testing 5-node cluster with session primitives...${NC}"
echo ""

echo "1. Waiting for all nodes to become ready..."
for port in 4000 4001 4002 4003 4004; do
  wait_for_ready "$port" 30 1
done
echo ""

echo "2. Creating test session via node1..."
SESSION_ID="test-session-$(date +%s)"
CREATE_RESP=$(curl -s -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"$SESSION_ID\",\"metadata\":{\"test\":true}}")

if echo "$CREATE_RESP" | grep -q '"id"'; then
  echo -e "  ✓ Session created: $SESSION_ID"
else
  echo -e "${RED}  ✗ Failed to create session${NC}"
  echo "Response: $CREATE_RESP"
  exit 1
fi
echo ""

echo "3. Appending from each node with expected_seq guards..."
EXPECTED_SEQ=0

for i in 1 2 3 4 5; do
  port=$((3999 + i))
  echo -n "  Node on port $port append: "

  RESP=$(curl -s -X POST "http://localhost:${port}/v1/sessions/${SESSION_ID}/append" \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"content\",\"payload\":{\"text\":\"message from node ${i}\"},\"actor\":\"agent:cluster-test\",\"expected_seq\":${EXPECTED_SEQ}}")

  SEQ=$(extract_seq "$RESP")
  NEXT=$((EXPECTED_SEQ + 1))

  if [ -n "$SEQ" ] && [ "$SEQ" -eq "$NEXT" ]; then
    echo -e "${GREEN}✓ seq=$SEQ${NC}"
    EXPECTED_SEQ=$SEQ
  else
    echo -e "${RED}✗ failed${NC}"
    echo "Response: $RESP"
    exit 1
  fi
done
echo ""

echo "4. Idempotency sanity check..."
KEY="idem-${SESSION_ID}"
RESP1=$(curl -s -X POST "http://localhost:4002/v1/sessions/${SESSION_ID}/append" \
  -H "Content-Type: application/json" \
  -d "{\"type\":\"state\",\"payload\":{\"state\":\"running\"},\"actor\":\"agent:cluster-test\",\"idempotency_key\":\"${KEY}\"}")
RESP2=$(curl -s -X POST "http://localhost:4003/v1/sessions/${SESSION_ID}/append" \
  -H "Content-Type: application/json" \
  -d "{\"type\":\"state\",\"payload\":{\"state\":\"running\"},\"actor\":\"agent:cluster-test\",\"idempotency_key\":\"${KEY}\"}")

SEQ1=$(extract_seq "$RESP1")
SEQ2=$(extract_seq "$RESP2")
if [ -n "$SEQ1" ] && [ "$SEQ1" = "$SEQ2" ] && echo "$RESP2" | grep -q '"deduped":true'; then
  echo -e "  ${GREEN}✓ idempotency dedupe works across nodes${NC}"
else
  echo -e "  ${RED}✗ idempotency check failed${NC}"
  echo "resp1: $RESP1"
  echo "resp2: $RESP2"
  exit 1
fi
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Cluster primitive test passed${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Validated:"
echo "  - create session"
echo "  - append routed from any node"
echo "  - expected_seq concurrency guard"
echo "  - idempotency dedupe"
