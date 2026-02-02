#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

echo -e "${GREEN}=== Fastpaca Smoke Test ===${NC}"

cd "$TERRAFORM_DIR"
INSTANCE_IPS=$(terraform output -json instance_ips | jq -r '.[]')

if [ -z "$INSTANCE_IPS" ]; then
  echo -e "${RED}Error: No instance IPs found. Run terraform apply first.${NC}"
  exit 1
fi

FIRST_IP=$(echo "$INSTANCE_IPS" | head -n1)
API_BASE="http://$FIRST_IP:4000/v1"

echo "Target node: $FIRST_IP"
echo "API: $API_BASE"
echo ""

CONVERSATION_ID="bench-smoke-$(date +%s)"

echo -e "${YELLOW}Step 1: Creating conversation (${CONVERSATION_ID})...${NC}"
CREATE_RESPONSE=$(curl -s -o /tmp/conversation_create.json -w "%{http_code}" \
  -X PUT "$API_BASE/conversations/$CONVERSATION_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {"bench": true, "scenario": "smoke"}
  }')

if [ "$CREATE_RESPONSE" -lt 200 ] || [ "$CREATE_RESPONSE" -ge 300 ]; then
  echo -e "${RED}✗ Conversation creation failed (status $CREATE_RESPONSE)${NC}"
  cat /tmp/conversation_create.json
  exit 1
fi

echo -e "${GREEN}✓ Conversation created${NC}"

echo -e "${YELLOW}Step 2: Appending message...${NC}"
APPEND_RESPONSE=$(curl -s -o /tmp/conversation_append.json -w "%{http_code}" \
  -X POST "$API_BASE/conversations/$CONVERSATION_ID/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "role": "user",
      "parts": [{"type": "text", "text": "Hello from Fastpaca smoke test"}],
      "metadata": {"source": "smoke-test"}
    }
  }')

if [ "$APPEND_RESPONSE" -lt 200 ] || [ "$APPEND_RESPONSE" -ge 300 ]; then
  echo -e "${RED}✗ Append failed (status $APPEND_RESPONSE)${NC}"
  cat /tmp/conversation_append.json
  exit 1
fi

SEQ=$(jq -r '.seq // empty' /tmp/conversation_append.json)
VERSION=$(jq -r '.version // empty' /tmp/conversation_append.json)

echo -e "${GREEN}✓ Message appended (seq: ${SEQ:-unknown}, version: ${VERSION:-unknown})${NC}"

echo -e "${YELLOW}Step 3: Fetching tail messages...${NC}"
TAIL_RESPONSE=$(curl -s -o /tmp/conversation_tail.json -w "%{http_code}" \
  "$API_BASE/conversations/$CONVERSATION_ID/tail?limit=10")

if [ "$TAIL_RESPONSE" != "200" ]; then
  echo -e "${RED}✗ Tail fetch failed (status $TAIL_RESPONSE)${NC}"
  cat /tmp/conversation_tail.json
  exit 1
fi

TAIL_COUNT=$(jq '.messages | length' /tmp/conversation_tail.json)

echo -e "${GREEN}✓ Tail retrieved (${TAIL_COUNT} messages)${NC}"

echo -e "${YELLOW}Step 4: Replaying messages...${NC}"
MESSAGES_RESPONSE=$(curl -s -o /tmp/conversation_replay.json -w "%{http_code}" \
  "$API_BASE/conversations/$CONVERSATION_ID/messages?from=0&limit=10")

if [ "$MESSAGES_RESPONSE" != "200" ]; then
  echo -e "${RED}✗ Message replay failed (status $MESSAGES_RESPONSE)${NC}"
  cat /tmp/conversation_replay.json
  exit 1
fi

REPLAY_COUNT=$(jq '.messages | length' /tmp/conversation_replay.json)

echo -e "${GREEN}✓ Replay fetched (${REPLAY_COUNT} messages)${NC}"

echo ""
echo -e "${GREEN}=== Smoke Test Complete ===${NC}"
echo "Conversation ID: $CONVERSATION_ID"
echo "Sample payload:"
jq '{messages: .messages}' /tmp/conversation_tail.json

rm -f /tmp/conversation_create.json /tmp/conversation_append.json /tmp/conversation_tail.json /tmp/conversation_replay.json
