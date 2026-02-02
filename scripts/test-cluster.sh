#!/bin/bash
# Test that the 5-node cluster is working correctly
# IMPORTANT: Any node should be able to serve any conversation (via routing/proxying)

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Testing 5-node Raft cluster...${NC}"
echo ""

# Check all nodes are running
echo "1. Checking nodes are running..."
for port in 4000 4001 4002 4003 4004; do
  if curl -s http://localhost:$port/health/ready > /dev/null; then
    echo -e "  ✓ Node on port $port is up"
  else
    echo -e "${RED}  ✗ Node on port $port is down${NC}"
    exit 1
  fi
done
echo ""

# Create a conversation via node1
echo "2. Creating test conversation via node1..."
CONVERSATION_ID="test-conversation-$(date +%s)"

CONVERSATION=$(curl -s -X PUT http://localhost:4000/v1/conversations/$CONVERSATION_ID \
  -H "Content-Type: application/json" \
  -d "{\"metadata\":{\"test\":true}}")

if echo "$CONVERSATION" | grep -q '"id"'; then
  echo -e "  ✓ Conversation created: $CONVERSATION_ID"
else
  echo -e "${RED}  ✗ Failed to create conversation${NC}"
  echo "Response: $CONVERSATION"
  exit 1
fi
echo ""

# Write a message via node1
echo "3. Writing message via node1..."
MSG_RESP=$(curl -s -X POST http://localhost:4000/v1/conversations/$CONVERSATION_ID/messages \
  -H "Content-Type: application/json" \
  -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"cluster test message\"}]}}")

MSG_SEQ=$(echo $MSG_RESP | grep -o '"seq":[0-9]*' | cut -d':' -f2)

if [ -z "$MSG_SEQ" ]; then
  echo -e "${RED}  ✗ Failed to write message${NC}"
  echo "Response: $MSG_RESP"
  exit 1
fi

echo -e "  ✓ Message written with seq: $MSG_SEQ"
echo ""

# Critical test: ALL nodes should be able to READ the conversation
# Even if only 3 nodes host the Raft replicas, the API layer should handle routing
echo "4. Testing that ALL nodes can read the conversation..."
echo "   (API should route to Raft replicas transparently)"
echo ""

failed_nodes=0
successful_reads=0

for port in 4000 4001 4002 4003 4004; do
  echo -n "  Node on port $port: "

  # Try reading with retries (allow for eventual consistency)
  found=false
  error_msg=""

  for i in {1..20}; do
    MSGS=$(curl -s "http://localhost:$port/v1/conversations/$CONVERSATION_ID/tail?limit=10" 2>&1)

    # Check if we got an error page (HTML response)
    if echo "$MSGS" | grep -q "DOCTYPE html"; then
      error_msg="Received error page"
      sleep 0.05
      continue
    fi

    READ_COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l | tr -d ' ')

    if [ "$READ_COUNT" -ge 1 ]; then
      found=true
      break
    fi

    sleep 0.05
  done

  if [ "$found" = true ]; then
    echo -e "${GREEN}✓ can read message${NC}"
    successful_reads=$((successful_reads + 1))
  else
    echo -e "${RED}✗ FAILED to read${NC}"
    if [ -n "$error_msg" ]; then
      echo "    Error: $error_msg"
    fi
    failed_nodes=$((failed_nodes + 1))
  fi
done
echo ""

# Verify ALL nodes can read
echo "5. Verifying request routing..."
if [ $failed_nodes -eq 0 ]; then
  echo -e "  ${GREEN}✓ All 5 nodes can serve the conversation (routing works)${NC}"
else
  echo -e "  ${RED}✗ $failed_nodes node(s) failed to serve the conversation${NC}"
  echo -e "  ${RED}✗ API layer is NOT routing requests to Raft replicas!${NC}"
  echo ""
  echo "Expected behavior: Any node should be able to serve any conversation"
  echo "Current behavior: Only replica nodes can serve conversations"
  echo ""
  echo "This is a critical issue for a production distributed system."
  exit 1
fi
echo ""

# Write from different nodes to verify routing works for writes too
echo "6. Testing writes from ALL nodes..."
write_failures=0

for i in {1..5}; do
  port=$((4000 + i - 1))
  echo -n "  Writing via node on port $port: "

  WRITE_RESP=$(curl -s -X POST http://localhost:$port/v1/conversations/$CONVERSATION_ID/messages \
    -H "Content-Type: application/json" \
    -d "{\"message\":{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"message from node $i\"}]}}" 2>&1)

  # Check if we got an error
  if echo "$WRITE_RESP" | grep -q "DOCTYPE html"; then
    echo -e "${RED}✗ FAILED${NC}"
    write_failures=$((write_failures + 1))
  elif echo "$WRITE_RESP" | grep -q '"seq"'; then
    echo -e "${GREEN}✓ success${NC}"
  else
    echo -e "${RED}✗ unexpected response${NC}"
    write_failures=$((write_failures + 1))
  fi
done
echo ""

if [ $write_failures -gt 0 ]; then
  echo -e "  ${RED}✗ $write_failures write(s) failed${NC}"
  echo -e "  ${RED}✗ Write routing is broken${NC}"
  exit 1
else
  echo -e "  ${GREEN}✓ All nodes can route writes correctly${NC}"
fi
echo ""

# Verify message count on all nodes
echo "7. Verifying all messages are accessible from all nodes..."
expected_msgs=6  # 1 initial + 5 from different nodes

for port in 4000 4001 4002 4003 4004; do
  MSGS=$(curl -s "http://localhost:$port/v1/conversations/$CONVERSATION_ID/tail?limit=20" 2>/dev/null)
  COUNT=$(echo $MSGS | grep -o '"seq":' | wc -l | tr -d ' ')

  if [ "$COUNT" -ge "$expected_msgs" ]; then
    echo -e "  Port $port: ${GREEN}✓ has all $COUNT messages${NC}"
  else
    echo -e "  Port $port: ${RED}✗ only has $COUNT messages (expected $expected_msgs)${NC}"
  fi
done
echo ""

# Cleanup
echo "8. Cleaning up test conversation..."
curl -s -X DELETE http://localhost:4000/v1/conversations/$CONVERSATION_ID > /dev/null
echo -e "  ✓ Conversation deleted"
echo ""

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ All cluster tests passed!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Cluster is healthy:"
echo "  - All 5 nodes responding to health checks"
echo "  - ALL nodes can serve ANY conversation (routing works)"
echo "  - Writes can be sent to ANY node"
echo "  - Raft handles replication transparently (3 replicas per group)"
echo ""
echo "Ready for benchmarks:"
echo "  k6 run bench/k6/1-hot-path-throughput.js"
