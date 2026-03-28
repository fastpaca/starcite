<h1 align="center">Starcite</h1>

<p align="center">
  Bulletproof streaming for AI sessions.
</p>

<p align="center">
  <a href="https://github.com/fastpaca/starcite/stargazers"><img src="https://img.shields.io/github/stars/fastpaca/starcite?style=social" alt="Stars"></a>
  <a href="https://github.com/fastpaca/starcite/actions/workflows/test.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg" alt="Tests"></a>
  <a href="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg" alt="Docker Build"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
</p>

Starcite is a durable, ordered session log for AI applications. Every message,
tool call, and status update is persisted before the write is acknowledged.
Consumers replay from any cursor and continue live on the same connection.

If a browser refreshes, a WebSocket drops, or work moves between agents,
the session stays intact.

## When To Use Starcite

**Use it when** you've hit the failure modes that every production AI UI eventually hits:

- Messages vanish when users refresh or switch tabs mid-stream
- Duplicates appear on reconnect because the server replays what the client already rendered
- Tool results show up without the tool calls that triggered them (client missed events in between)
- Multiple agents write to one session and ordering breaks
- Two tabs open the same session and show different messages
- Every deploy kills all active streams and you're writing reconnect logic

If you're patching these one by one, you're building an [accidental message broker in your frontend](https://starcite.ai/blog/why-agent-uis-lose-messages-on-refresh).
Starcite replaces that with one primitive: an ordered, immutable session log
where catch-up and live streaming are the same operation — the cursor is the
only variable.

**Don't use it when** you're prototyping and just need to stream a model
response to a browser. Use [Vercel AI SDK](https://sdk.vercel.ai/docs), SSE, or a plain WebSocket — that's the right tool for that job. Starcite is for when you've outgrown that.

## Get Started

### Run Locally With Docker Compose

```bash
git clone https://github.com/fastpaca/starcite
cd starcite
docker compose up -d
```

This starts a single-node stack for local development. The public API listens on
`PORT` (default `4000`). Ops endpoints such as `/health/live` and `/health/ready`
are served only on the separate listener configured by `STARCITE_OPS_PORT`.

Stop and reset:

```bash
docker compose down -v
```

For production topology and cluster operations, see
[Self-hosting](docs/self-hosting.md).

### Use A Hosted Instance

The hosted CLI flow below uses hosted credentials. Self-hosted HTTP and
WebSocket access in this repo defaults to bearer JWT auth.

```bash
# install
bun install -g starcite

# configure
starcite config set endpoint https://<your-instance>.starcite.io
starcite config set api-key <YOUR_API_KEY>

starcite create --id ses_demo --title "Draft contract"
starcite append ses_demo --agent researcher --text "Found 8 relevant cases..."
starcite tail ses_demo --cursor 0 --limit 1
```

For one-off use without installing globally:

```bash
bunx starcite --help
```

## API At A Glance

Three operations: **create** a session, **append** an event, **tail** from a cursor.

Self-hosted Starcite defaults to JWT auth. The examples below assume
`STARCITE_TOKEN` is a bearer JWT with the scopes you need. Omit auth headers only
when you explicitly run with `STARCITE_AUTH_MODE=none`.

<details>
<summary>curl</summary>

```bash
export STARCITE_TOKEN=<jwt>

# 1) Create a session
curl -X POST http://localhost:4000/v1/sessions \
  -H "Authorization: Bearer ${STARCITE_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ses_demo",
    "title": "Draft contract",
    "metadata": {"workflow": "contract"}
  }'

# 2) Append an event
curl -X POST http://localhost:4000/v1/sessions/ses_demo/append \
  -H "Authorization: Bearer ${STARCITE_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Reviewing clause 4.2..."},
    "producer_id": "agent-drafter-1",
    "producer_seq": 1,
    "expected_seq": 0
  }'

# Response: {"seq":1,"last_seq":1,"deduped":false}

# 3) Tail from a cursor (Phoenix Channel over one shared WebSocket)
# connect a Phoenix socket to ws://localhost:4000/v1/socket
# pass params { token: STARCITE_TOKEN }
# then join topic "tail:ses_demo" with payload {"cursor": 0}
```

</details>

<details>
<summary>CLI</summary>

```bash
# assumes the CLI is already configured with credentials for your Starcite instance
starcite create --id ses_demo --title "Draft contract"
starcite append ses_demo --agent drafter --text "Reviewing clause 4.2..."
starcite tail ses_demo --cursor 0
```

</details>

<details>
<summary>TypeScript SDK</summary>

```ts
import { Starcite } from "@starcite/sdk";

const starcite = new Starcite({ baseUrl: process.env.STARCITE_BASE_URL });
const session = starcite.session({ token: "<session-jwt>" });

// append
await session.append({ text: "Reviewing clause 4.2...", source: "user" });

// tail — replay + live
session.on("event", (event) => console.log(event.seq, event.type));
```

</details>

<details>
<summary>React (or ai-sdk)</summary>

`useStarciteChat` is a drop-in replacement for AI SDK's `useChat` — backed by
the durable session log instead of ephemeral client state.

```tsx
import { Starcite } from "@starcite/sdk";
import { useStarciteChat } from "@starcite/react";

const starcite = new Starcite({
  baseUrl: process.env.NEXT_PUBLIC_STARCITE_BASE_URL,
});

export function Chat({ token }: { token: string }) {
  const session = starcite.session({ token });
  const { messages, sendMessage, status } = useStarciteChat({
    session,
    id: session.id,
  });

  return (
    <form onSubmit={(e) => { e.preventDefault(); sendMessage({ text: "hello" }); }}>
      <button type="submit">Send</button>
      <pre>{JSON.stringify(messages, null, 2)}</pre>
    </form>
  );
}
```

There's a working [Next.js example app](https://github.com/fastpaca/starcite-clients/tree/main/examples/nextjs-chat-ui) you can run locally.

</details>

Append supports `expected_seq` for optimistic concurrency and `producer_id`/`producer_seq`
for dedup. Tail replays ordered history, then continues live on the same connection.

[REST details](docs/api/rest.md) · [WebSocket details](docs/api/websocket.md)

## How It Works

1. You create a session. Starcite assigns it to a replicated cluster that holds the log in memory.
2. Any producer appends an event. Starcite assigns the next `seq`, persists it across replicas, and acknowledges.
3. Any consumer joins `tail:<session_id>` on `/v1/socket` with `cursor=N`. Starcite replays everything after `N`, then keeps streaming live events on the same channel.
4. A background process archives committed events to S3 and advances archive progress in Postgres — writes are never blocked by archival.

The result:

- Every write is durable before you get an ack
- Every reader sees the same order (monotonic `seq` per session)
- Catch-up and live streaming are the same operation
- Multiple producers can write from anywhere without sticky sessions
- Duplicate writes are detected via `(producer_id, producer_seq)`

**Latency:** 24ms p99 / ~120ms p99.9 append-to-ack on a 3-node EC2 cluster.

For internals (Raft groups, replication topology, snapshots), see
[Architecture](docs/architecture.md).

## Trade-offs

Starcite is a distributed system. You should know what you're signing up for:

**Starcite vs. just using Postgres:**
Postgres can do ordered inserts and polling. If you have a single database, low write volume, and can tolerate polling latency, a well-indexed Postgres table might be all you need. Starcite adds Raft-based replication, real-time tail via WebSocket, and cursor-based resume — but it's another system to run.

**Starcite vs. Redis Streams / Pub/Sub:**
Redis Streams give you ordered, cursor-resumable reads (`XREAD`) and are battle-tested. If you already run Redis and your sessions fit in memory, this can work well. The gap is session-scoped semantics (you build that yourself), durable archival, and the fact that Redis persistence is best-effort unless you configure it carefully.

**Starcite vs. Kafka:**
Kafka is a proven distributed log. If you already operate Kafka, you can model sessions as topics or keyed partitions. The overhead is operational complexity and the impedance mismatch between Kafka's throughput-optimized design and the latency/session-scoped semantics of interactive AI sessions.

**When Starcite is overkill:**
If you're building a single-user chat prototype, streaming one model response at a time, you don't need any of this. Use the AI SDK's built-in streaming. Come back when you're handling reconnects, multiple agents, or production UX that can't lose messages.

## Clients

Official clients live in [`fastpaca/starcite-clients`](https://github.com/fastpaca/starcite-clients):

- [`starcite` CLI](https://github.com/fastpaca/starcite-clients/tree/main/packages/starcite-cli) — terminal workflows
- [`@starcite/sdk`](https://github.com/fastpaca/starcite-clients/tree/main/packages/typescript-sdk) — TypeScript SDK for backends and browsers
- [`@starcite/react`](https://github.com/fastpaca/starcite-clients/tree/main/packages/starcite-react) — drop-in `useChat` replacement backed by the durable session log

Or use the HTTP + WebSocket API directly from any language.

## What Starcite Does Not Do

- Agent discovery or capability negotiation
- Prompt construction or completion orchestration
- Token budgeting or window management
- Tool or function-calling abstractions
- OAuth credential issuance

Your orchestrator stays yours.

## Documentation

- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)
- [Architecture](docs/architecture.md)
- [Self-hosting](docs/self-hosting.md)
- [Why Agent UIs Lose Messages on Refresh](https://starcite.ai/blog/why-agent-uis-lose-messages-on-refresh) (blog)

## Development

```bash
git clone https://github.com/fastpaca/starcite && cd starcite
mix deps.get
mix compile
mix phx.server  # http://localhost:4000
mix precommit   # format + compile (warnings-as-errors) + test
```

## Contributing

1. Fork and create a branch.
2. Add tests for behavior changes.
3. Run `mix precommit` before opening a PR.

## License

Apache 2.0
