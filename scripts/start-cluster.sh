#!/bin/bash
# Start a 5-node local Raft cluster for testing

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting 5-node Fastpaca Raft cluster...${NC}"
echo ""

# Check if already running
if pgrep -f "node1@127.0.0.1" > /dev/null; then
  echo -e "${YELLOW}Warning: Cluster appears to be already running${NC}"
  echo "Run './scripts/stop-cluster.sh' first"
  exit 1
fi

# Create logs directory
mkdir -p logs

# Clean any stale Raft data (both priv/raft and node directories)
rm -rf priv/raft/* node*@127.0.0.1 2>/dev/null || true

echo -e "${GREEN}Raft data cleaned${NC}"
echo ""

# Start all nodes with CLUSTER_NODES env for libcluster discovery
CLUSTER_NODES="node1@127.0.0.1,node2@127.0.0.1,node3@127.0.0.1,node4@127.0.0.1,node5@127.0.0.1"
ARCHIVER_ENABLED="${FASTPACA_ARCHIVER_ENABLED:-false}"

if [ "$ARCHIVER_ENABLED" != "false" ] && [ -z "${DATABASE_URL:-}" ] && [ -z "${FASTPACA_POSTGRES_URL:-}" ]; then
  echo -e "${YELLOW}Warning: FASTPACA_ARCHIVER_ENABLED=$ARCHIVER_ENABLED but no DATABASE_URL/FASTPACA_POSTGRES_URL set${NC}"
  echo "  Archiver will attempt to start without a configured database"
  echo ""
fi

echo -e "${GREEN}Starting node1@127.0.0.1 on port 4000...${NC}"
FASTPACA_ARCHIVER_ENABLED="$ARCHIVER_ENABLED" CLUSTER_NODES="$CLUSTER_NODES" PORT=4000 elixir --name node1@127.0.0.1 -S mix phx.server > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "  PID: $NODE1_PID"

echo -e "${GREEN}Starting node2@127.0.0.1 on port 4001...${NC}"
FASTPACA_ARCHIVER_ENABLED="$ARCHIVER_ENABLED" CLUSTER_NODES="$CLUSTER_NODES" PORT=4001 elixir --name node2@127.0.0.1 -S mix phx.server > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "  PID: $NODE2_PID"

echo -e "${GREEN}Starting node3@127.0.0.1 on port 4002...${NC}"
FASTPACA_ARCHIVER_ENABLED="$ARCHIVER_ENABLED" CLUSTER_NODES="$CLUSTER_NODES" PORT=4002 elixir --name node3@127.0.0.1 -S mix phx.server > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "  PID: $NODE3_PID"

echo -e "${GREEN}Starting node4@127.0.0.1 on port 4003...${NC}"
FASTPACA_ARCHIVER_ENABLED="$ARCHIVER_ENABLED" CLUSTER_NODES="$CLUSTER_NODES" PORT=4003 elixir --name node4@127.0.0.1 -S mix phx.server > logs/node4.log 2>&1 &
NODE4_PID=$!
echo "  PID: $NODE4_PID"

echo -e "${GREEN}Starting node5@127.0.0.1 on port 4004...${NC}"
FASTPACA_ARCHIVER_ENABLED="$ARCHIVER_ENABLED" CLUSTER_NODES="$CLUSTER_NODES" PORT=4004 elixir --name node5@127.0.0.1 -S mix phx.server > logs/node5.log 2>&1 &
NODE5_PID=$!
echo "  PID: $NODE5_PID"

echo ""
echo -e "${GREEN}Cluster started!${NC}"
echo ""
echo "Nodes (libcluster CLUSTER_NODES=$CLUSTER_NODES):"
echo "  node1@127.0.0.1 - http://localhost:4000 (PID: $NODE1_PID)"
echo "  node2@127.0.0.1 - http://localhost:4001 (PID: $NODE2_PID)"
echo "  node3@127.0.0.1 - http://localhost:4002 (PID: $NODE3_PID)"
echo "  node4@127.0.0.1 - http://localhost:4003 (PID: $NODE4_PID)"
echo "  node5@127.0.0.1 - http://localhost:4004 (PID: $NODE5_PID)"
echo ""
echo "Logs:"
echo "  tail -f logs/node1.log"
echo "  tail -f logs/node2.log"
echo "  tail -f logs/node3.log"
echo "  tail -f logs/node4.log"
echo "  tail -f logs/node5.log"
echo ""
echo "Test cluster connectivity:"
echo "  ./scripts/test-cluster.sh"
echo ""
echo "Run benchmarks against node1:"
echo "  k6 run bench/k6/1-hot-path-throughput.js"
echo ""
echo "Stop cluster:"
echo "  ./scripts/stop-cluster.sh"
