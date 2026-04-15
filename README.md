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

Starcite keeps AI session history intact when the real world gets messy. Every
message, tool call, and status update lands in one ordered stream you can
replay, resume, and keep tailing live.

Refresh the browser, reconnect a socket, switch tabs, restart a worker, or let
multiple agents write at once: the session stays coherent.

## When To Use Starcite

**Use it when** the hard problem is no longer token streaming. It's keeping one
correct session timeline across browsers, reconnects, workers, and agents.

- Messages vanish when users refresh or switch tabs mid-stream
- Duplicates appear on reconnect because the server replays what the client already rendered
- Tool results show up without the tool calls that triggered them (client missed events in between)
- Multiple agents write to one session and ordering breaks
- Two tabs open the same session and show different messages
- Every deploy kills all active streams and you're writing reconnect logic

If you're patching these one by one, you're rebuilding a message system in
frontend state and reconnect code. Starcite replaces that with one primitive:
an ordered session log where replay and live delivery are the same operation
and the cursor is the contract.

**Don't use it when** you're prototyping and just need to stream one model
response to one browser. Use [Vercel AI SDK](https://sdk.vercel.ai/docs), SSE,
or a plain WebSocket. Starcite starts paying for itself when reconnects,
multi-writer ordering, and session integrity become product requirements.

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

Core operations: **create** a session, **append** an event, **list/get** session
metadata, **archive/unarchive** a session, and **tail** from a cursor.

The core loop is simple: create a session once, append events from any
producer, then tail from a cursor for replay plus live updates.

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

# Response: {"seq":1,"last_seq":1,"deduped":false,"cursor":1,"committed_cursor":0}

# 3) Archive the session from active list views
curl -X POST http://localhost:4000/v1/sessions/ses_demo/archive \
  -H "Authorization: Bearer ${STARCITE_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}'

# 4) Tail from a cursor over WebSocket
# connect to ws://localhost:4000/v1/socket
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
the session log instead of ephemeral client state.

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

Append supports `expected_seq` for optimistic concurrency and
`producer_id`/`producer_seq` for dedup. Session headers support
`PATCH /v1/sessions/:id` with shallow metadata merge and
`expected_version` compare-and-swap. Session listing excludes archived
sessions by default, while archived sessions remain readable by id and tail.
Tail replays ordered history, then continues live on the same connection.
Append replies include both the appended `cursor` and `committed_cursor`.

[REST details](docs/api/rest.md) · [WebSocket details](docs/api/websocket.md) · [Changelog](CHANGELOG.md) · [Release process](docs/releasing.md)

## How It Works

1. Create a session for one conversation, workflow, or run.
2. Append events from users, agents, tools, or backend workers. Starcite assigns the session order and gives you a cursorable timeline.
3. Tail from any cursor to replay what you missed, then keep receiving live events on the same connection.
4. Reconnect with the last cursor you processed. Archived sessions drop out of default list views, but their timelines remain readable by id and tail.

What this buys you:

- One ordered truth per session
- Retry-safe multi-producer writes via `(producer_id, producer_seq)` dedup
- Replay and live delivery under one cursor model
- No special reconnect path beyond cursor resume
- Session history that stays readable even when the client or producer goes away

For internals, see
[Architecture](docs/architecture.md).

## Trade-offs

Starcite is a distributed system. It earns its keep when the session itself is
part of the product, not just a transient transport.

**Starcite vs. just using Postgres:**
Postgres can do ordered inserts and polling. If you have a single database, low
write volume, and can tolerate polling latency, a well-indexed Postgres table
might be all you need. Starcite adds real-time tail over WebSocket,
cursor-based resume, built-in session semantics, and a cleaner contract for
multiple producers. If polling and single-writer assumptions are fine,
Postgres is simpler.

**Starcite vs. Redis Streams / Pub/Sub:**
Redis Streams give you ordered, cursor-resumable reads (`XREAD`) and are
battle-tested. If you already run Redis and your sessions fit in memory, this
can work well. The gap is session-scoped auth and metadata semantics, one
session-focused API for replay plus live tail, and a higher-level contract for
AI session history instead of raw stream primitives.

**Starcite vs. Kafka:**
Kafka is a proven distributed log. If you already operate Kafka, you can model
sessions as topics or keyed partitions. The cost is operational weight and the
impedance mismatch between Kafka's throughput-oriented model and the
low-latency, per-session semantics of interactive AI products.

**When Starcite is overkill:**
If you're building a single-user chat prototype and streaming one model
response at a time, you probably don't need any of this. Use the AI SDK's
built-in streaming. Come back when reconnects, multiple agents, and session
consistency stop being optional.

## Clients

Official clients live in [`fastpaca/starcite-clients`](https://github.com/fastpaca/starcite-clients):

- [`starcite` CLI](https://github.com/fastpaca/starcite-clients/tree/main/packages/starcite-cli) — terminal workflows
- [`@starcite/sdk`](https://github.com/fastpaca/starcite-clients/tree/main/packages/typescript-sdk) — TypeScript SDK for backends and browsers
- [`@starcite/react`](https://github.com/fastpaca/starcite-clients/tree/main/packages/starcite-react) — drop-in `useChat` replacement backed by the session log

The REST mutation surface for session headers is `PATCH /v1/sessions/:id`.
Official SDKs should expose that as a first-class method such as
`updateSession(sessionId, { title, metadata, expectedVersion })` or an
equivalent session-scoped helper.

Or use the HTTP + WebSocket API directly from any language.

## What Starcite Does Not Do

- Agent discovery or capability negotiation
- Prompt construction or completion orchestration
- Token budgeting or window management
- Tool or function-calling abstractions
- OAuth credential issuance

Starcite owns session ordering, replay, and delivery semantics. Your
orchestrator still owns the rest.

## Documentation

- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)
- [Architecture](docs/architecture.md)
- [Self-hosting](docs/self-hosting.md)
- [Why Agent UIs Lose Messages on Refresh](https://starcite.ai/blog/why-agent-uis-lose-messages-on-refresh) (blog)

## Development

```bash
git clone https://github.com/fastpaca/starcite && cd starcite
mise install
mise trust .mise.toml
mise exec -- mix deps.get
mise exec -- mix compile
mise exec -- mix phx.server  # http://localhost:4000
mise exec -- mix precommit   # format + compile (warnings-as-errors) + test
```

## Contributing

1. Fork and create a branch.
2. Add tests for behavior changes.
3. Run `mise exec -- mix precommit` before opening a PR.

## License

Apache 2.0
