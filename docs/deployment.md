---
title: Deployment
sidebar_position: 3
---

# Deployment

Fastpaca is built to run in your own infrastructure. This page covers recommended runtimes, storage, and operational knobs.

---

## Hardware & storage

- **CPU:** 2+ vCPUs per node (Raft + JSON encoding are CPU-bound).
- **Memory:** 4 GB RAM per node for typical workloads. Increase if you retain very large tails.
- **Disk:** Fast SSD/NVMe for the Raft log (append-heavy). Mount `/data` on dedicated storage.
- Set `FASTPACA_RAFT_DATA_DIR` to the mounted volume path so Raft logs survive restarts.
- **Network:** Low-latency links between nodes. For production, keep Raft replicas within the same AZ or region (<5 ms RTT).

---

## Single-node development

```bash
docker run -d \
  -p 4000:4000 \
  -v fastpaca_data:/data \
  ghcr.io/fastpaca/context-store:latest
```

The node serves REST on `:4000`, websockets on the same port (Phoenix channel at `/socket`), and Prometheus metrics on `/metrics`.

Data persists across restarts as long as the `fastpaca_data` volume remains.

---

## Three-node production cluster

Create a DNS entry (or static host list) that resolves to all nodes, e.g. `fastpaca.internal`.

On each node:

```bash
docker run -d \
  -p 4000:4000 \
  -v /var/lib/fastpaca:/data \
  -e CLUSTER_NODES=fastpaca-1@fastpaca.internal,fastpaca-2@fastpaca.internal,fastpaca-3@fastpaca.internal \
  -e NODE_NAME=fastpaca-1 \
  ghcr.io/fastpaca/context-store:latest
```

Repeat with `NODE_NAME=fastpaca-2/3`. Nodes discover peers through `CLUSTER_NODES` and form a Raft cluster.

### Placement guidelines

- Run exactly three replicas for quorum (tolerates one node failure).
- Pin each replica to separate AZs only if network RTT remains low.
- Use Kubernetes StatefulSets, Nomad groups, or bare metal with systemd; the binary is self-contained.

---

## Optional archival (Postgres)

Fastpaca does not require external storage for correctness. Configure an archive if you need:

- Long-term history beyond the Raft tail.
- Analytics / BI queries on the full log.
- Faster cold-start recovery for very old conversations.

The Postgres archiver is built in. It persists messages and then acknowledges a high-water mark to Raft so the tail can trim older segments while retaining a safety buffer.

Archiver environment variables:

```bash
-e FASTPACA_ARCHIVER_ENABLED=true \
-e DATABASE_URL=postgres://user:password@host/db \
-e FASTPACA_ARCHIVE_FLUSH_INTERVAL_MS=5000 \
-e DB_POOL_SIZE=10
```

Tail retention is configured via application settings:

```elixir
config :fastpaca,
  tail_keep: 1_000
```

See Storage & Audit for schema and audit details: ./storage.md

---

## Metrics & observability

Prometheus metrics are exposed on `/metrics`. Key series:

- `fastpaca_messages_append_total` – total messages appended (by role/source)
- `fastpaca_messages_token_count` – token count per appended message (distribution)
- `fastpaca_archive_pending_rows` – rows pending in the archive queue (ETS)
- `fastpaca_archive_pending_conversations` – conversations pending in the archive queue
- `fastpaca_archive_flush_duration_ms` – flush tick duration
- `fastpaca_archive_attempted_total` / `fastpaca_archive_inserted_total` – rows attempted/inserted
- `fastpaca_archive_lag` – per-conversation lag (last_seq - archived_seq)
- `fastpaca_archive_tail_size` – Raft tail size after trim
- `fastpaca_archive_trimmed_total` – entries trimmed from Raft tail

Distribution bucket boundaries are defined in `lib/fastpaca/observability/prom_ex/metrics.ex`.

Logs follow JSON structure with fields like `type`, `conversation_id`, and `seq`. Forward them to your logging stack for audit trails.

---

## Backups & retention

- Raft log and snapshots reside in `/data`. Snapshot the volume regularly (EBS/GCE disk snapshots).
- If Postgres is enabled, use standard database backups.
- Periodically export conversations for legal or compliance requirements by querying the archive.

---

## Scaling out

- **More throughput:** Add additional nodes; Raft group assignment is deterministic and redistributed automatically via coordinator pattern (lowest node ID manages topology).
- **Sharding:** Not required for most workloads - 256 Raft groups provide sufficient horizontal fan-out.
- **Read replicas:** Not needed; every node can serve reads. Use Postgres replicas if you run heavy analytics.
- **Coordinator failover:** If the coordinator node fails, the next-lowest node automatically becomes coordinator. No manual intervention required.

---

## Configuration summary

| Variable | Default | Description |
| --- | --- | --- |
| `NODE_NAME` | random | Node identifier used by clustering and Redis PubSub (if enabled) |
| `CLUSTER_NODES` | none | Comma-separated list of peer Erlang node names (e.g. `node@host`) |
| `DNS_CLUSTER_QUERY` | none | DNS name for libcluster DNS poll strategy |
| `DNS_CLUSTER_NODE_BASENAME` | `fastpaca` | Base name for DNS cluster nodes |
| `DNS_POLL_INTERVAL_MS` | `5000` | DNS poll interval for cluster discovery |
| `FASTPACA_RAFT_DATA_DIR` | `priv/raft` | Filesystem path for Raft logs and snapshots |
| `FASTPACA_ARCHIVER_ENABLED` | `false` | Enable Postgres archiver |
| `FASTPACA_POSTGRES_URL` | none | Alternate database URL if `DATABASE_URL` is not set |
| `FASTPACA_ARCHIVE_FLUSH_INTERVAL_MS` | `5000` | Archive flush tick interval |
| `DB_POOL_SIZE` | `10` | Postgres connection pool size |
| `REDIS_HOST` | `localhost` | Redis host when PubSub adapter is set to `:redis` |
| `REDIS_PORT` | `6379` | Redis port when PubSub adapter is set to `:redis` |

Consult the sample configuration file in the repository for all options.
