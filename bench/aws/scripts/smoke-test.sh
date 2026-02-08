#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

echo -e "${GREEN}=== Starcite Smoke Test ===${NC}"

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

SESSION_ID="bench-smoke-$(date +%s)"

echo -e "${YELLOW}Step 1: Creating session (${SESSION_ID})...${NC}"
CREATE_STATUS=$(curl -s -o /tmp/session_create.json -w "%{http_code}" \
  -X POST "$API_BASE/sessions" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"$SESSION_ID\",\"metadata\":{\"bench\":true,\"scenario\":\"smoke\"}}")

if [ "$CREATE_STATUS" -lt 200 ] || [ "$CREATE_STATUS" -ge 300 ]; then
  echo -e "${RED}✗ Session creation failed (status $CREATE_STATUS)${NC}"
  cat /tmp/session_create.json
  exit 1
fi

echo -e "${GREEN}✓ Session created${NC}"

echo -e "${YELLOW}Step 2: Appending event...${NC}"
APPEND_STATUS=$(curl -s -o /tmp/session_append.json -w "%{http_code}" \
  -X POST "$API_BASE/sessions/$SESSION_ID/append" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Hello from Starcite smoke test"},
    "actor": "agent:smoke",
    "source": "smoke-test"
  }')

if [ "$APPEND_STATUS" -lt 200 ] || [ "$APPEND_STATUS" -ge 300 ]; then
  echo -e "${RED}✗ Append failed (status $APPEND_STATUS)${NC}"
  cat /tmp/session_append.json
  exit 1
fi

SEQ=$(jq -r '.seq // empty' /tmp/session_append.json)
DEDUPED=$(jq -r '.deduped // empty' /tmp/session_append.json)

echo -e "${GREEN}✓ Event appended (seq: ${SEQ:-unknown}, deduped: ${DEDUPED:-unknown})${NC}"

echo -e "${YELLOW}Step 3: Idempotency retry...${NC}"
IDEM_KEY="smoke-${SESSION_ID}"

APPEND1_STATUS=$(curl -s -o /tmp/session_idem_1.json -w "%{http_code}" \
  -X POST "$API_BASE/sessions/$SESSION_ID/append" \
  -H "Content-Type: application/json" \
  -d "{\"type\":\"state\",\"payload\":{\"state\":\"running\"},\"actor\":\"agent:smoke\",\"idempotency_key\":\"$IDEM_KEY\"}")

APPEND2_STATUS=$(curl -s -o /tmp/session_idem_2.json -w "%{http_code}" \
  -X POST "$API_BASE/sessions/$SESSION_ID/append" \
  -H "Content-Type: application/json" \
  -d "{\"type\":\"state\",\"payload\":{\"state\":\"running\"},\"actor\":\"agent:smoke\",\"idempotency_key\":\"$IDEM_KEY\"}")

if [ "$APPEND1_STATUS" -ge 300 ] || [ "$APPEND2_STATUS" -ge 300 ]; then
  echo -e "${RED}✗ Idempotency append failed${NC}"
  cat /tmp/session_idem_1.json
  cat /tmp/session_idem_2.json
  exit 1
fi

SEQ1=$(jq -r '.seq // empty' /tmp/session_idem_1.json)
SEQ2=$(jq -r '.seq // empty' /tmp/session_idem_2.json)
DEDUPED2=$(jq -r '.deduped // false' /tmp/session_idem_2.json)

if [ "$SEQ1" = "$SEQ2" ] && [ "$DEDUPED2" = "true" ]; then
  echo -e "${GREEN}✓ Idempotency dedupe works${NC}"
else
  echo -e "${RED}✗ Idempotency dedupe failed${NC}"
  cat /tmp/session_idem_1.json
  cat /tmp/session_idem_2.json
  exit 1
fi

echo ""
echo -e "${GREEN}=== Smoke Test Complete ===${NC}"
echo "Session ID: $SESSION_ID"

rm -f /tmp/session_create.json /tmp/session_append.json /tmp/session_idem_1.json /tmp/session_idem_2.json
