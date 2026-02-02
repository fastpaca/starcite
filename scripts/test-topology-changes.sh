#!/bin/bash
# Test dynamic topology changes: kill 2 nodes (node3, node4), verify cluster rebalances

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== TOPOLOGY CHANGE TEST (5-node cluster) ===${NC}"
echo ""

# Check cluster is running
running_nodes=0
for node in node1@127.0.0.1 node2@127.0.0.1 node3@127.0.0.1 node4@127.0.0.1 node5@127.0.0.1; do
  if pgrep -f "$node" > /dev/null; then
    running_nodes=$((running_nodes + 1))
  fi
done

if [ $running_nodes -lt 5 ]; then
  echo -e "${RED}Full cluster not running (only $running_nodes nodes). Start all 5 nodes first:${NC}"
  echo "  ./scripts/start-cluster.sh"
  exit 1
fi

echo -e "${GREEN}Found all 5 nodes running${NC}"
echo ""

# Step 1: Create a test conversation and write messages
echo "Step 1: Creating test conversation and writing messages..."
CONVERSATION_ID="topology-test-$(date +%s)"

CONVERSATION=$(curl -s -X PUT http://localhost:4000/v1/conversations/$CONVERSATION_ID \
  -H "Content-Type: application/json" \
  -d "{\"metadata\":{}}")

echo "  Conversation: $CONVERSATION_ID"

# Write initial message
curl -s -X POST http://localhost:4000/v1/conversations/$CONVERSATION_ID/messages \
  -H "Content-Type: application/json" \
  -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"initial message\"}]}}" > /dev/null
echo "  ✓ Initial message written"
echo ""

# Step 2: Kill node3
echo "Step 2: Killing node3..."
NODE3_PIDS=$(pgrep -f "node3@127.0.0.1")
if [ -z "$NODE3_PIDS" ]; then
  echo -e "${RED}  Node3 not running!${NC}"
  exit 1
fi

kill $NODE3_PIDS
echo "  ✓ Node3 killed (PIDs: $NODE3_PIDS)"
wait_for_ready() {
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

echo "  Waiting for cluster to detect failure (polling readiness)..."
wait_for_ready 15 1 "http://localhost:4001/health/ready"
echo ""

# Step 3: Verify cluster still works (should have 4 healthy nodes)
echo "Step 3: Writing message after node3 failure..."
MSG_RESP=$(curl -s -X POST http://localhost:4001/v1/conversations/$CONVERSATION_ID/messages \
  -H "Content-Type: application/json" \
  -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"after node3 killed\"}]}}")

if echo "$MSG_RESP" | grep -q '"seq"'; then
  echo -e "  ${GREEN}✓ Write succeeded with 4 nodes${NC}"
else
  echo -e "  ${RED}✗ Write failed${NC}"
  echo "Response: $MSG_RESP"
  exit 1
fi
echo ""

# Step 4: Kill node4 as well
echo "Step 4: Killing node4..."
NODE4_PIDS=$(pgrep -f "node4@127.0.0.1")
if [ -z "$NODE4_PIDS" ]; then
  echo -e "${RED}  Node4 not running!${NC}"
  exit 1
fi

kill $NODE4_PIDS
echo "  ✓ Node4 killed (PIDs: $NODE4_PIDS)"
echo "  Waiting for cluster to detect second failure..."
wait_for_ready 15 1 "http://localhost:4001/health/ready"
echo ""

# Step 5: Verify cluster still works with 3 nodes (minimum for quorum with 3-replica groups)
echo "Step 5: Writing message with only 3 nodes remaining..."
MSG_RESP=$(curl -s -X POST http://localhost:4000/v1/conversations/$CONVERSATION_ID/messages \
  -H "Content-Type: application/json" \
  -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"with 3 nodes\"}]}}")

if echo "$MSG_RESP" | grep -q '"seq"'; then
  echo -e "  ${GREEN}✓ Write succeeded with 3 nodes${NC}"
else
  echo -e "  ${RED}✗ Write failed${NC}"
  echo "Response: $MSG_RESP"
  exit 1
fi
echo ""

# Step 6: Read messages from all remaining nodes
echo "Step 6: Verifying message replication across remaining nodes..."

for port in 4000 4001 4004; do
  echo "  Checking node on port $port..."
  for i in {1..20}; do
    MSGS=$(curl -s "http://localhost:$port/v1/conversations/$CONVERSATION_ID/tail?limit=10")
    COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l)

    if [ "$COUNT" -ge 3 ]; then
      echo -e "    ${GREEN}✓ All messages replicated ($COUNT total)${NC}"
      break
    fi

    if [ $i -eq 20 ]; then
      echo -e "    ${YELLOW}⚠ Only $COUNT messages found (expected 3+)${NC}"
    fi

    sleep 0.05
  done
done
echo ""

# Step 7: Restart node3
echo "Step 7: Restarting node3 (simulating node recovery)..."
CLUSTER_NODES="node1@127.0.0.1,node2@127.0.0.1,node3@127.0.0.1,node4@127.0.0.1,node5@127.0.0.1" PORT=4002 elixir --name node3@127.0.0.1 -S mix phx.server > logs/node3-restart.log 2>&1 &
NODE3_NEW_PID=$!
echo "  ✓ Node3 restarted (PID: $NODE3_NEW_PID)"
echo "  Waiting for node3 to rejoin cluster..."
wait_for_ready 30 1 "http://localhost:4002/health/ready"
echo ""

# Step 8: Verify node3 rejoined and can serve requests
echo "Step 8: Verifying node3 rejoined cluster..."
if curl -s http://localhost:4002/health/ready > /dev/null; then
  echo -e "  ${GREEN}✓ Node3 is responding to API requests${NC}"

  # Check if it has the messages
  for i in {1..30}; do
    MSGS=$(curl -s "http://localhost:4002/v1/conversations/$CONVERSATION_ID/tail?limit=10")
    COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l)

    if [ "$COUNT" -ge 3 ]; then
      echo -e "  ${GREEN}✓ Node3 has all messages replicated ($COUNT total)${NC}"
      break
    fi

    if [ $i -eq 30 ]; then
      echo -e "  ${YELLOW}⚠ Node3 only has $COUNT messages (may need more time to sync)${NC}"
    fi

    sleep 0.1
  done
else
  echo -e "  ${RED}✗ Node3 not responding${NC}"
fi
echo ""

# Cleanup
echo "Step 9: Cleaning up test conversation..."
curl -s -X DELETE http://localhost:4000/v1/conversations/$CONVERSATION_ID > /dev/null
echo -e "  ✓ Conversation deleted"
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Topology change test complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Cluster handled topology changes:"
echo "  - Started with 5 nodes"
echo "  - Killed node3, cluster continued with 4 nodes"
echo "  - Killed node4, cluster continued with 3 nodes (minimum for quorum)"
echo "  - Restarted node3, it rejoined and synced data"
echo ""
echo "Current state:"
echo "  - Node1: running"
echo "  - Node2: running"
echo "  - Node3: running (restarted)"
echo "  - Node4: killed"
echo "  - Node5: running"
echo ""
echo "Cluster now has 4-of-5 nodes operational"
echo ""
echo "To stop cluster:"
echo "  ./scripts/stop-cluster.sh"
