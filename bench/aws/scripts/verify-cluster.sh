#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

echo -e "${GREEN}=== FleetLM Cluster Verification ===${NC}"

# Get outputs from Terraform
cd "$TERRAFORM_DIR"
INSTANCE_IPS=$(terraform output -json instance_ips | jq -r '.[]')
CLUSTER_NODES=$(terraform output -raw cluster_nodes 2>/dev/null || echo "")

if [ -z "$INSTANCE_IPS" ]; then
  echo -e "${RED}Error: No instance IPs found${NC}"
  exit 1
fi

INSTANCE_COUNT=$(echo "$INSTANCE_IPS" | wc -l | tr -d ' ')
EXPECTED_CLUSTER_SIZE=$((INSTANCE_COUNT - 1))

echo "Expected cluster size: $EXPECTED_CLUSTER_SIZE nodes (excluding self)"
echo ""

# SSH options
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"

# Check each node
INDEX=0
ALL_HEALTHY=true
for IP in $INSTANCE_IPS; do
  INDEX=$((INDEX + 1))
  NODE_NAME="fleet_lm-bench-$INDEX"

  echo -e "${YELLOW}Checking $NODE_NAME ($IP)...${NC}"

  # Check if container is running
  if ! ssh $SSH_OPTS ec2-user@$IP "docker ps | grep fleetlm" > /dev/null 2>&1; then
    echo -e "${RED}✗ Container not running${NC}"
    ALL_HEALTHY=false
    continue
  fi
  echo -e "${GREEN}✓ Container running${NC}"

  # Check if app is responding
  HEALTH_CHECK=$(ssh $SSH_OPTS ec2-user@$IP "curl -s -o /dev/null -w '%{http_code}' http://localhost:4000/health/live || echo 'FAIL'" 2>/dev/null)
  if [ "$HEALTH_CHECK" = "200" ] || [ "$HEALTH_CHECK" = "302" ]; then
    echo -e "${GREEN}✓ HTTP responding (status: $HEALTH_CHECK)${NC}"
  else
    echo -e "${RED}✗ HTTP not responding${NC}"
    ALL_HEALTHY=false
  fi

  # Check cluster connectivity
  CLUSTER_OUTPUT=$(ssh $SSH_OPTS ec2-user@$IP "docker exec fleetlm bin/fleet_lm rpc 'Node.list()'" 2>&1 || echo "FAIL")

  if echo "$CLUSTER_OUTPUT" | grep -q "FAIL"; then
    echo -e "${RED}✗ Failed to query cluster state${NC}"
    echo "  Error: $CLUSTER_OUTPUT"
    ALL_HEALTHY=false
    continue
  fi

  # Count connected nodes (list format is typically: [:"node@ip", :"node@ip"])
  CONNECTED_COUNT=$(echo "$CLUSTER_OUTPUT" | grep -o "fleet_lm@" | wc -l | tr -d ' ')

  if [ "$CONNECTED_COUNT" -eq "$EXPECTED_CLUSTER_SIZE" ]; then
    echo -e "${GREEN}✓ Cluster connected ($CONNECTED_COUNT/$EXPECTED_CLUSTER_SIZE nodes)${NC}"
    echo "  Nodes: $CLUSTER_OUTPUT"
  else
    echo -e "${RED}✗ Cluster incomplete ($CONNECTED_COUNT/$EXPECTED_CLUSTER_SIZE nodes)${NC}"
    echo "  Nodes: $CLUSTER_OUTPUT"
    ALL_HEALTHY=false
  fi

  echo ""
done

# Summary
echo -e "${GREEN}=== Verification Summary ===${NC}"
if [ "$ALL_HEALTHY" = true ]; then
  echo -e "${GREEN}✓ All nodes healthy and clustered${NC}"
  exit 0
else
  echo -e "${RED}✗ Some nodes failed health checks${NC}"
  echo ""
  echo -e "${YELLOW}Troubleshooting tips:${NC}"
  echo "  1. Check logs: ssh ec2-user@<IP> 'docker logs fleetlm'"
  echo "  2. Check container: ssh ec2-user@<IP> 'docker ps -a'"
  echo "  3. Restart node: ssh ec2-user@<IP> 'docker restart fleetlm'"
  echo "  4. Manual cluster check: ssh ec2-user@<IP> 'docker exec fleetlm bin/fleet_lm rpc \"Node.list()\"'"
  exit 1
fi
