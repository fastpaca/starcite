#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"
K6_DIR="$SCRIPT_DIR/../../k6"

echo -e "${GREEN}=== Starcite Remote k6 Benchmark (AWS intra-VPC) ===${NC}"

cd "$TERRAFORM_DIR"
BENCHMARK_CLIENT_IP=$(terraform output -raw benchmark_client_ip)
STARCITE_PRIVATE_IP=$(terraform output -json instance_private_ips | jq -r '.[0]')

if [ -z "$BENCHMARK_CLIENT_IP" ]; then
  echo -e "${RED}Error: No benchmark client IP found${NC}"
  exit 1
fi

if [ -z "$STARCITE_PRIVATE_IP" ]; then
  echo -e "${RED}Error: No Starcite instance IP found${NC}"
  exit 1
fi

echo "Benchmark client: $BENCHMARK_CLIENT_IP"
echo "Starcite target: $STARCITE_PRIVATE_IP (private IP, same VPC)"
echo ""

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"

echo -e "${YELLOW}Waiting for benchmark client SSH...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0
while ! ssh $SSH_OPTS ec2-user@$BENCHMARK_CLIENT_IP "echo 'ready'" 2>/dev/null; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo -e "${RED}Error: SSH not available${NC}"
    exit 1
  fi
  echo "  Attempt $RETRY_COUNT/$MAX_RETRIES..."
  sleep 10
done
echo -e "${GREEN}SSH ready${NC}"

echo -e "${YELLOW}Verifying k6 installation...${NC}"
if ! ssh $SSH_OPTS ec2-user@$BENCHMARK_CLIENT_IP "k6 version" > /dev/null 2>&1; then
  echo -e "${RED}Error: k6 not installed on benchmark client${NC}"
  echo "Wait for user_data to complete or SSH in and install manually"
  exit 1
fi
echo -e "${GREEN}k6 is installed${NC}"

echo -e "${YELLOW}Copying k6 scripts to benchmark client...${NC}"
ssh $SSH_OPTS ec2-user@$BENCHMARK_CLIENT_IP "rm -rf ~/k6"
scp $SSH_OPTS -r "$K6_DIR" ec2-user@$BENCHMARK_CLIENT_IP:~/
echo -e "${GREEN}Scripts copied${NC}"

BENCHMARK="${1:-1-hot-path-throughput}"
RUN_ID="aws-remote-$(date +%s)"
API_URL="http://$STARCITE_PRIVATE_IP:4000/v1"

echo ""
echo -e "${YELLOW}Benchmark configuration:${NC}"
echo "  Script: $BENCHMARK.js"
echo "  API URL: $API_URL (private VPC)"
echo ""

echo -e "${YELLOW}Running k6 benchmark on remote instance...${NC}"
echo ""

ssh $SSH_OPTS ec2-user@$BENCHMARK_CLIENT_IP "cd ~/k6 && \
  API_URL='$API_URL' \
  RUN_ID='$RUN_ID' \
  k6 run $BENCHMARK.js"

echo ""
echo -e "${GREEN}=== Remote Benchmark Complete ===${NC}"
echo ""
echo -e "${YELLOW}Note: Test ran from AWS VPC (no WAN latency).${NC}"
