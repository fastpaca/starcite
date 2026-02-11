#!/bin/bash
# Start a 5-node local Raft cluster for testing

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

wait_for_ready() {
  local port=$1
  local attempts=${2:-60}
  local interval=${3:-1}
  local url="http://localhost:${port}/health/ready"
  local health=""

  for ((i = 1; i <= attempts; i++)); do
    health=$(curl -s "$url" || true)
    if echo "$health" | grep -q '"status":"ok"'; then
      echo -e "  ✓ Node on port $port ready"
      return 0
    fi
    sleep "$interval"
  done

  echo -e "${RED}  ✗ Node on port $port not ready after ${attempts}s${NC}"
  if [ -n "$health" ]; then
    echo "    Last response: $health"
  fi
  return 1
}

wait_for_pid() {
  local pid=$1
  local name=$2
  local attempts=${3:-20}
  local interval=${4:-1}

  for ((i = 1; i <= attempts; i++)); do
    if kill -0 "$pid" 2>/dev/null; then
      echo -e "  ✓ ${name} process up (PID: $pid)"
      return 0
    fi
    sleep "$interval"
  done

  echo -e "${RED}  ✗ ${name} failed to stay up (PID: $pid)${NC}"
  return 1
}

echo -e "${GREEN}Starting 5-node Starcite Raft cluster...${NC}"
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

echo -e "${GREEN}Compiling once before booting nodes (avoids multi-node compile races)...${NC}"
if ! mix compile > logs/compile.log 2>&1; then
  echo -e "${RED}Compile failed. See logs/compile.log${NC}"
  exit 1
fi
echo -e "${GREEN}Compile complete${NC}"
echo ""

# Start all nodes with CLUSTER_NODES env for libcluster discovery
CLUSTER_NODES="node1@127.0.0.1,node2@127.0.0.1,node3@127.0.0.1,node4@127.0.0.1,node5@127.0.0.1"
ARCHIVE_DB_URL="${DATABASE_URL:-${STARCITE_POSTGRES_URL:-ecto://postgres:postgres@localhost:5432/starcite_dev}}"
MIX_NO_COMPILE=1

echo -e "${GREEN}Starting node1@127.0.0.1 on port 4000...${NC}"
nohup env MIX_NO_COMPILE="$MIX_NO_COMPILE" DATABASE_URL="$ARCHIVE_DB_URL" CLUSTER_NODES="$CLUSTER_NODES" PORT=4000 elixir --name node1@127.0.0.1 -S mix phx.server > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "  PID: $NODE1_PID"
sleep 0.2

echo -e "${GREEN}Starting node2@127.0.0.1 on port 4001...${NC}"
nohup env MIX_NO_COMPILE="$MIX_NO_COMPILE" DATABASE_URL="$ARCHIVE_DB_URL" CLUSTER_NODES="$CLUSTER_NODES" PORT=4001 elixir --name node2@127.0.0.1 -S mix phx.server > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "  PID: $NODE2_PID"
sleep 0.2

echo -e "${GREEN}Starting node3@127.0.0.1 on port 4002...${NC}"
nohup env MIX_NO_COMPILE="$MIX_NO_COMPILE" DATABASE_URL="$ARCHIVE_DB_URL" CLUSTER_NODES="$CLUSTER_NODES" PORT=4002 elixir --name node3@127.0.0.1 -S mix phx.server > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "  PID: $NODE3_PID"
sleep 0.2

echo -e "${GREEN}Starting node4@127.0.0.1 on port 4003...${NC}"
nohup env MIX_NO_COMPILE="$MIX_NO_COMPILE" DATABASE_URL="$ARCHIVE_DB_URL" CLUSTER_NODES="$CLUSTER_NODES" PORT=4003 elixir --name node4@127.0.0.1 -S mix phx.server > logs/node4.log 2>&1 &
NODE4_PID=$!
echo "  PID: $NODE4_PID"
sleep 0.2

echo -e "${GREEN}Starting node5@127.0.0.1 on port 4004...${NC}"
nohup env MIX_NO_COMPILE="$MIX_NO_COMPILE" DATABASE_URL="$ARCHIVE_DB_URL" CLUSTER_NODES="$CLUSTER_NODES" PORT=4004 elixir --name node5@127.0.0.1 -S mix phx.server > logs/node5.log 2>&1 &
NODE5_PID=$!
echo "  PID: $NODE5_PID"

echo ""
echo "Verifying all node processes are running..."
wait_for_pid "$NODE1_PID" "node1@127.0.0.1"
wait_for_pid "$NODE2_PID" "node2@127.0.0.1"
wait_for_pid "$NODE3_PID" "node3@127.0.0.1"
wait_for_pid "$NODE4_PID" "node4@127.0.0.1"
wait_for_pid "$NODE5_PID" "node5@127.0.0.1"

echo ""
echo "Waiting for all nodes to become ready..."
for port in 4000 4001 4002 4003 4004; do
  if ! wait_for_ready "$port" 60 1; then
    echo ""
    echo -e "${RED}Cluster failed readiness checks. Recent logs:${NC}"
    for log_file in logs/node1.log logs/node2.log logs/node3.log logs/node4.log logs/node5.log; do
      echo "----- $log_file -----"
      tail -n 40 "$log_file" || true
    done
    exit 1
  fi
done

echo -e "${GREEN}Cluster started and ready!${NC}"
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
