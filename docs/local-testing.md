# Local Testing

Local cluster drills use the checked-in Compose topology directly:

```bash
docker build --no-cache -t starcite-it:integration .
docker compose -f docker-compose.integration.yml -p starcite-khepri-it up -d
```

The integration cluster is:

- `node1` API `http://localhost:4001`, ops `http://localhost:4101`
- `node2` API `http://localhost:4002`, ops `http://localhost:4102`
- `node3` API `http://localhost:4003`, ops `http://localhost:4103`

## Readiness

Wait until every node reports ready:

```bash
curl -sS http://localhost:4101/health/ready
curl -sS http://localhost:4102/health/ready
curl -sS http://localhost:4103/health/ready
```

Verify Khepri membership from any container:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'IO.inspect(:khepri_cluster.members(Starcite.Routing.Store.store_id()))'
```

## Session Seeding

Create a session and append an event through the live HTTP API:

```bash
curl -sS -X POST http://localhost:4001/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"id":"ops-smoke-1"}'

curl -sS -X POST http://localhost:4001/v1/sessions/ops-smoke-1/append \
  -H 'content-type: application/json' \
  -d '{"type":"content","payload":{"text":"hello"},"producer_id":"ops-writer","producer_seq":1}'
```

Inspect the authoritative routing claim:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'IO.inspect(Starcite.Routing.Store.get_assignment("ops-smoke-1", favor: :consistency))'
```

## Rolling Drain Drill

Run this one node at a time.

Drain the node:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'Starcite.Operations.drain_node(Node.self())'
```

Wait until the node reports fully drained:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'Starcite.Operations.wait_local_drained(30000)'
```

Check the authoritative drain state before restart:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'IO.inspect(Starcite.Operations.node_status(Node.self()))'

docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'IO.inspect(Starcite.Operations.drain_status(Node.self()))'
```

At this point you want:

- node status `:drained`
- `active_owned_sessions: 0`
- `moving_sessions: 0`

Restart the drained node:

```bash
docker compose -f docker-compose.integration.yml -p starcite-khepri-it restart node1
```

Wait for it to rejoin as ready:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'Starcite.Operations.wait_local_ready(30000)'
```

Verify it rejoined the ready set:

```bash
docker exec starcite-khepri-it-node1-1 /app/starcite/bin/starcite rpc \
  'IO.inspect(Starcite.Operations.ready_nodes())'
```

Repeat the same sequence for `node2` and `node3`.

## Pass Criteria

A rolling drill is healthy when all of the following hold:

1. Each node reaches `:drained` before restart.
2. No assignment is left in `status: :moving`.
3. Post-drain appends to moved sessions still succeed.
4. After restart, the node returns to `ready_nodes`.
5. New session claims eventually land on the returned node.
6. Khepri membership remains stable across the full cycle.

## Teardown

```bash
docker compose -f docker-compose.integration.yml -p starcite-khepri-it down -v --remove-orphans
```
