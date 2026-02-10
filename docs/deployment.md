# Deployment

Starcite is designed for self-hosted clustered deployment.

## Hardware guidance

- CPU: 2+ vCPUs per node
- Memory: 4+ GB RAM per node
- Disk: fast SSD/NVMe for Raft logs
- Network: low RTT between cluster nodes

Set `STARCITE_RAFT_DATA_DIR` to persistent storage so Raft state survives restarts.

## Single node

```bash
docker run -d \
  -p 4000:4000 \
  -v starcite_data:/data \
  ghcr.io/fastpaca/starcite:latest
```

## Three-node cluster

```bash
docker run -d \
  -p 4000:4000 \
  -v /var/lib/starcite:/data \
  -e CLUSTER_NODES=starcite-1@starcite.internal,starcite-2@starcite.internal,starcite-3@starcite.internal \
  -e NODE_NAME=starcite-1 \
  ghcr.io/fastpaca/starcite:latest
```

Repeat with `NODE_NAME=starcite-2` and `NODE_NAME=starcite-3`.

Use a load balancer in front of nodes for API traffic.

## Local cluster (development)

```bash
./scripts/start-cluster.sh   # start 5-node local cluster
./scripts/test-cluster.sh    # verify primitives
./scripts/stop-cluster.sh    # tear down
```

## Postgres archive (optional)

```bash
-e STARCITE_ARCHIVER_ENABLED=true \
-e DATABASE_URL=postgres://user:password@host/db \
-e STARCITE_ARCHIVE_FLUSH_INTERVAL_MS=5000 \
-e DB_POOL_SIZE=10
```

Query archived events:

```sql
SELECT * FROM events WHERE session_id = $1 ORDER BY seq;
```

## Metrics

Prometheus metrics on `/metrics`.

| Metric | Description |
| --- | --- |
| `starcite_events_append_total` | Total appended events |
| `starcite_events_payload_bytes` | Payload bytes appended |
| `starcite_archive_pending_rows` | Rows waiting to flush |
| `starcite_archive_pending_sessions` | Sessions with pending rows |
| `starcite_archive_flush_duration_ms` | Flush cycle duration |
| `starcite_archive_attempted_total` | Rows attempted |
| `starcite_archive_inserted_total` | Rows inserted |
| `starcite_archive_bytes_attempted_total` | Bytes attempted |
| `starcite_archive_bytes_inserted_total` | Bytes inserted |
| `starcite_archive_lag` | Archive lag |
| `starcite_archive_tail_size` | Hot tail size |
| `starcite_archive_trimmed_total` | Rows trimmed after archive |

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `NODE_NAME` | random | Node identifier |
| `CLUSTER_NODES` | none | Comma-separated peer node names |
| `DNS_CLUSTER_QUERY` | none | DNS name for libcluster discovery |
| `DNS_CLUSTER_NODE_BASENAME` | `starcite` | Base name for DNS nodes |
| `DNS_POLL_INTERVAL_MS` | `5000` | DNS poll interval |
| `STARCITE_RAFT_DATA_DIR` | `priv/raft` | Raft logs and snapshots path |
| `STARCITE_PAYLOAD_PLANE` | `legacy` | Payload mirroring mode (`legacy`, `dual_write`) |
| `STARCITE_TAIL_SOURCE` | `legacy` | Tail read source (`legacy`, `ets`) |
| `STARCITE_ARCHIVER_ENABLED` | `false` | Enable Postgres archiver |
| `DATABASE_URL` | none | Postgres URL (archive) |
| `STARCITE_POSTGRES_URL` | none | Alternate Postgres URL |
| `STARCITE_ARCHIVE_FLUSH_INTERVAL_MS` | `5000` | Archive flush interval |
| `DB_POOL_SIZE` | `10` | Postgres pool size |
| `PORT` | `4000` | HTTP server port |
| `PHX_SERVER` | unset | Start endpoint in release mode |
| `SECRET_KEY_BASE` | none | Phoenix secret (required in prod) |
| `PHX_HOST` | `example.com` | Public host for endpoint URLs |

Redis PubSub (when adapter is `:redis`):

| Variable | Default | Description |
| --- | --- | --- |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
