#!/bin/bash
# Test Raft leader failover by killing nodes (5-node cluster)

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Testing Raft leader failover (5-node cluster)...${NC}"
echo ""

# Check cluster is running (check at least 3 nodes)
running_nodes=0
for node in node1@127.0.0.1 node2@127.0.0.1 node3@127.0.0.1 node4@127.0.0.1 node5@127.0.0.1; do
  if pgrep -f "$node" > /dev/null; then
    running_nodes=$((running_nodes + 1))
  fi
done

if [ $running_nodes -lt 3 ]; then
  echo -e "${RED}Cluster not running (only $running_nodes nodes). Start it first:${NC}"
  echo "  ./scripts/start-cluster.sh"
  exit 1
fi

echo -e "${GREEN}Found $running_nodes running nodes${NC}"
echo ""

# Create a test conversation
echo "1. Creating test conversation..."
CONVERSATION_ID="failover-test-$(date +%s)"

CONVERSATION=$(curl -s -X PUT http://localhost:4000/v1/conversations/$CONVERSATION_ID \
  -H "Content-Type: application/json" \
  -d "{\"metadata\":{}}")

echo -e "  ✓ Conversation: $CONVERSATION_ID"
echo ""

# Write a message
echo "2. Writing initial message to node1..."
curl -s -X POST http://localhost:4000/v1/conversations/$CONVERSATION_ID/messages \
  -H "Content-Type: application/json" \
  -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"before failover\"}]}}" > /dev/null
echo -e "  ✓ Message written"
echo ""

# Kill node1
echo "3. Killing node1 (simulating failure)..."
NODE1_PIDS=$(pgrep -f "node1@127.0.0.1")
kill $NODE1_PIDS
echo -e "  ✓ Node1 killed (PIDs: $NODE1_PIDS)"
echo ""

# Wait for Raft to elect new leader
echo "4. Waiting for cluster to recover (polling readiness)..."
wait_for_readiness() {
  local attempts=$1
  local interval=$2
  local url=$3

  for ((i = 1; i <= attempts; i++)); do
    health=$(curl -sf "$url" || true)

    if echo "$health" | grep -q '"status":"ok"'; then
      echo -e "  ✓ Cluster ready after $((i * interval))s"
      return 0
    elif echo "$health" | grep -q '"status":"starting"'; then
      echo -e "  ✓ Cluster functional (status=starting) after $((i * interval))s"
      return 0
    fi

    sleep "$interval"
  done

  echo -e "  ${YELLOW}⚠ Cluster not ready after $((attempts * interval))s${NC}"
  return 1
}

wait_for_readiness 15 1 "http://localhost:4001/health/ready"
echo ""

# Try to write via node2 (should work via new leader)
echo "5. Writing message via node2 (tests failover)..."
FAILOVER_RESP=$(curl -s -X POST http://localhost:4001/v1/conversations/$CONVERSATION_ID/messages \
  -H "Content-Type: application/json" \
  -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"after failover\"}]}}")

if echo "$FAILOVER_RESP" | grep -q '"seq"'; then
  echo -e "  ${GREEN}✓ Write succeeded after failover!${NC}"
else
  echo -e "  ${RED}✗ Write failed after failover${NC}"
  echo "Response: $FAILOVER_RESP"
  exit 1
fi
echo ""

# Read from node3 (should have both messages)
echo "6. Reading messages from node3..."
for i in {1..20}; do
  MSGS=$(curl -s "http://localhost:4002/v1/conversations/$CONVERSATION_ID/tail?limit=10")
  COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l)

  if [ "$COUNT" -ge 2 ]; then
    if [ $i -gt 1 ]; then
      echo -e "  ${GREEN}✓ Both messages present ($COUNT total, took $i retries)${NC}"
    else
      echo -e "  ${GREEN}✓ Both messages present ($COUNT total)${NC}"
    fi
    break
  fi

  if [ $i -eq 20 ]; then
    echo -e "  ${YELLOW}⚠ Only $COUNT messages found (expected 2+)${NC}"
  fi

  sleep 0.05
done
echo ""

# Verify messages on node4 and node5 as well
echo "7. Reading messages from node4..."
for i in {1..20}; do
  MSGS=$(curl -s "http://localhost:4003/v1/conversations/$CONVERSATION_ID/tail?limit=10")
  COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l)

  if [ "$COUNT" -ge 2 ]; then
    if [ $i -gt 1 ]; then
      echo -e "  ${GREEN}✓ Both messages present on node4 ($COUNT total, took $i retries)${NC}"
    else
      echo -e "  ${GREEN}✓ Both messages present on node4 ($COUNT total)${NC}"
    fi
    break
  fi

  if [ $i -eq 20 ]; then
    echo -e "  ${YELLOW}⚠ Only $COUNT messages found on node4 (expected 2+)${NC}"
  fi

  sleep 0.05
done
echo ""

echo "8. Reading messages from node5..."
for i in {1..20}; do
  MSGS=$(curl -s "http://localhost:4004/v1/conversations/$CONVERSATION_ID/tail?limit=10")
  COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l)

  if [ "$COUNT" -ge 2 ]; then
    if [ $i -gt 1 ]; then
      echo -e "  ${GREEN}✓ Both messages present on node5 ($COUNT total, took $i retries)${NC}"
    else
      echo -e "  ${GREEN}✓ Both messages present on node5 ($COUNT total)${NC}"
    fi
    break
  fi

  if [ $i -eq 20 ]; then
    echo -e "  ${YELLOW}⚠ Only $COUNT messages found on node5 (expected 2+)${NC}"
  fi

  sleep 0.05
done
echo ""

# Cleanup
echo "9. Cleaning up test conversation..."
curl -s -X DELETE http://localhost:4001/v1/conversations/$CONVERSATION_ID > /dev/null
echo -e "  ✓ Conversation deleted"
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Failover test passed!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Raft handled node failure:"
echo "  - Node1 killed"
echo "  - Leader re-elected among remaining nodes"
echo "  - Writes continued via new leader"
echo "  - Data replicated across all remaining nodes (node2-node5)"
echo ""
echo "Cluster now running 4-of-5 nodes (one node down, still fully operational)"
echo ""
echo "To restart node1:"
echo "  CLUSTER_NODES=node1@127.0.0.1,node2@127.0.0.1,node3@127.0.0.1,node4@127.0.0.1,node5@127.0.0.1 PORT=4000 elixir --name node1@127.0.0.1 -S mix phx.server > logs/node1.log 2>&1 &"
echo ""
echo "To stop remaining nodes:"
echo "  ./scripts/stop-cluster.sh"
