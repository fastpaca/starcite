#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

echo -e "${GREEN}=== FleetLM Cluster Deployment ===${NC}"

if [ ! -d "$TERRAFORM_DIR" ]; then
  echo -e "${RED}Error: Terraform directory not found at $TERRAFORM_DIR${NC}"
  exit 1
fi

if [ ! -f "$TERRAFORM_DIR/terraform.tfstate" ]; then
  echo -e "${RED}Error: No terraform.tfstate found. Run 'terraform apply' first.${NC}"
  exit 1
fi

cd "$TERRAFORM_DIR"
echo -e "${YELLOW}Fetching Terraform outputs...${NC}"
INSTANCE_IPS=$(terraform output -json instance_ips | jq -r '.[]')
CLUSTER_NODES=$(terraform output -raw cluster_nodes 2>/dev/null || echo "")

if [ -z "$INSTANCE_IPS" ]; then
  echo -e "${RED}Error: No instance IPs found. Check your Terraform configuration.${NC}"
  exit 1
fi

INSTANCE_COUNT=$(echo "$INSTANCE_IPS" | wc -l | tr -d ' ')
echo -e "${GREEN}Found $INSTANCE_COUNT instance(s)${NC}"

SECRET_KEY_BASE=$(openssl rand -base64 48 | tr -d '\n')
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

INDEX=0
for IP in $INSTANCE_IPS; do
  INDEX=$((INDEX + 1))
  NODE_NAME="fleet_lm-bench-$INDEX"

  echo ""
  echo -e "${GREEN}=== Deploying to $NODE_NAME ($IP) ===${NC}"

  echo "Waiting for SSH to be available..."
  MAX_RETRIES=30
  RETRY_COUNT=0
  while ! ssh $SSH_OPTS ec2-user@$IP "echo 'SSH ready'" 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
      echo -e "${RED}Error: SSH not available after $MAX_RETRIES attempts${NC}"
      exit 1
    fi
    echo "  Attempt $RETRY_COUNT/$MAX_RETRIES..."
    sleep 10
  done

  echo "Waiting for Docker to be ready..."
  MAX_RETRIES=20
  RETRY_COUNT=0
  while ! ssh $SSH_OPTS ec2-user@$IP "docker ps" 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
      echo -e "${RED}Error: Docker not ready after $MAX_RETRIES attempts${NC}"
      exit 1
    fi
    echo "  Attempt $RETRY_COUNT/$MAX_RETRIES..."
    sleep 5
  done

  echo "Stopping any existing FleetLM containers..."
  ssh $SSH_OPTS ec2-user@$IP "docker stop fleet_lm 2>/dev/null || true"
  ssh $SSH_OPTS ec2-user@$IP "docker rm fleet_lm 2>/dev/null || true"

  echo "Pulling FleetLM image..."
  ssh $SSH_OPTS ec2-user@$IP "docker pull ghcr.io/fastpaca/fleet-lm:latest"

  PRIVATE_IP=$(ssh $SSH_OPTS ec2-user@$IP "hostname -i" | tr -d '\n')
  HAS_NVME=$(ssh $SSH_OPTS ec2-user@$IP "test -d /mnt/nvme && echo 'yes' || echo 'no'")

  if [ "$HAS_NVME" = "yes" ]; then
    echo "Instance store NVMe detected - using for log storage"
    VOLUME_MOUNT="-v /mnt/nvme:/wal"
    SLOT_LOG_DIR="/wal"
  else
    VOLUME_MOUNT=""
    SLOT_LOG_DIR=""
  fi

  echo "Starting FleetLM container..."
  ssh $SSH_OPTS ec2-user@$IP "docker run -d \
    --name fleet_lm \
    --restart unless-stopped \
    -p 4000:4000 \
    -p 4369:4369 \
    -p 9100-9155:9100-9155 \
    $VOLUME_MOUNT \
    -e SECRET_KEY_BASE='$SECRET_KEY_BASE' \
    -e PHX_HOST='$IP' \
    -e PORT=4000 \
    -e PHX_SERVER=true \
    -e CLUSTER_NODES='$CLUSTER_NODES' \
    -e SLOT_LOG_DIR='$SLOT_LOG_DIR' \
    -e MIX_ENV=prod \
    -e RELEASE_DISTRIBUTION=name \
    -e RELEASE_NODE='fleet_lm@$PRIVATE_IP' \
    ghcr.io/fastpaca/fleet-lm:latest"

  sleep 5

  echo "Checking container status..."
  if ssh $SSH_OPTS ec2-user@$IP "docker ps | grep fleet_lm" > /dev/null; then
    echo -e "${GREEN}✓ Container is running${NC}"
  else
    echo -e "${RED}✗ Container failed to start${NC}"
    echo "Container logs:"
    ssh $SSH_OPTS ec2-user@$IP "docker logs fleet_lm"
    exit 1
  fi
done

echo ""
echo -e "${GREEN}=== Deployment Complete ===${NC}"
echo ""
echo -e "${YELLOW}Cluster configuration:${NC}"
echo "  Nodes: $INSTANCE_COUNT"
if [ -n "$CLUSTER_NODES" ]; then
  echo "  CLUSTER_NODES: $CLUSTER_NODES"
else
  echo "  CLUSTER_NODES: (not configured)"
fi
echo ""
echo -e "${YELLOW}Instance access:${NC}"
INDEX=0
for IP in $INSTANCE_IPS; do
  INDEX=$((INDEX + 1))
  echo "  Node $INDEX: ssh ec2-user@$IP"
  echo "    HTTP: http://$IP:4000/v1"
done
echo ""
echo -e "${YELLOW}Useful commands:${NC}"
echo "  View logs: ssh ec2-user@<IP> 'docker logs -f fleet_lm'"
echo "  Verify cluster: ssh ec2-user@<IP> 'docker exec fleet_lm bin/fleet_lm rpc \"Node.list()\"'"
echo "  Restart node: ssh ec2-user@<IP> 'docker restart fleet_lm'"
echo ""
