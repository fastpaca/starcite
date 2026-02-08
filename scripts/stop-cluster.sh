#!/bin/bash
# Stop the local 5-node Raft cluster

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping Starcite Raft cluster...${NC}"

# Find and kill all nodes
for node in node1@127.0.0.1 node2@127.0.0.1 node3@127.0.0.1 node4@127.0.0.1 node5@127.0.0.1; do
  pids=$(pgrep -f "$node" || true)
  if [ -n "$pids" ]; then
    echo "Stopping $node (PIDs: $pids)"
    kill $pids 2>/dev/null || true
  else
    echo "$node not running"
  fi
done

# Wait for processes to die
sleep 2

# Force kill if still running
for node in node1@127.0.0.1 node2@127.0.0.1 node3@127.0.0.1 node4@127.0.0.1 node5@127.0.0.1; do
  pids=$(pgrep -f "$node" || true)
  if [ -n "$pids" ]; then
    echo "Force killing $node (PIDs: $pids)"
    kill -9 $pids 2>/dev/null || true
  fi
done

echo -e "${GREEN}Cluster stopped${NC}"
