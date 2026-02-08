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

echo -e "${GREEN}=== Starcite k6 Benchmark ===${NC}"

if ! command -v k6 &> /dev/null; then
  echo -e "${RED}Error: k6 is not installed${NC}"
  echo "Install it from: https://k6.io/docs/get-started/installation/"
  exit 1
fi

cd "$TERRAFORM_DIR"
INSTANCE_IPS=$(terraform output -json instance_ips | jq -r '.[]')

if [ -z "$INSTANCE_IPS" ]; then
  echo -e "${RED}Error: No instance IPs found. Run terraform apply first.${NC}"
  exit 1
fi

FIRST_IP=$(echo "$INSTANCE_IPS" | head -n1)
API_URL="http://$FIRST_IP:4000/v1"

BENCHMARK="${1:-1-hot-path-throughput}"
SCRIPT_PATH="$K6_DIR/$BENCHMARK.js"

if [ ! -f "$SCRIPT_PATH" ]; then
  echo -e "${RED}Error: Benchmark script not found: $SCRIPT_PATH${NC}"
  echo "Available benchmarks:"
  ls -1 "$K6_DIR"/*.js 2>/dev/null || echo "  (none found)"
  exit 1
fi

echo "Target instance: $FIRST_IP"
echo "API URL: $API_URL"
echo ""

echo -e "${YELLOW}Benchmark configuration:${NC}"
echo "  Script: $BENCHMARK.js"
echo ""

cd "$K6_DIR"
RUN_ID="aws-local-$(date +%s)"

echo -e "${YELLOW}Starting k6 benchmark...${NC}"
echo ""

k6 run \
  -e API_URL="$API_URL" \
  -e RUN_ID="$RUN_ID" \
  "$BENCHMARK.js"

echo ""
echo -e "${GREEN}=== Benchmark Complete ===${NC}"
