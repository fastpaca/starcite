<h1 align="center">Starcite</h1>

<p align="center">
  Durable ordered event streams for AI applications.
</p>

<p align="center">
  <a href="https://github.com/fastpaca/starcite/actions/workflows/test.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg" alt="Tests"></a>
  <a href="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg" alt="Docker Build"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://elixir-lang.org/"><img src="https://img.shields.io/badge/Elixir-1.18.4-blue.svg" alt="Elixir"></a>
</p>

[Run Locally](#run-locally-with-docker-compose) • [Use Hosted](#use-the-hosted-version) • [Use With](#use-with) • [Clients & CLI](https://github.com/fastpaca/starcite-clients) • [REST API](docs/api/rest.md) • [WebSocket API](docs/api/websocket.md) • [Architecture](docs/architecture.md) • [Self-hosting](docs/self-hosting.md)

## Run Locally With Docker Compose

```bash
git clone https://github.com/fastpaca/starcite
cd starcite
docker compose up -d
curl -sS http://localhost:4000/health/live
```

This starts a single-node Starcite stack locally:

- Starcite API on `http://localhost:4000`
- Postgres on `localhost:5433`
- MinIO on `localhost:9000`

Stop and reset it with:

```bash
docker compose down -v
```

For production topology and cluster operations, see
[Self-hosting](docs/self-hosting.md).

## Use The Hosted Version

```bash
npm install -g starcite
starcite config set endpoint https://<your-instance>.starcite.io
starcite config set api-key <YOUR_API_KEY>
starcite create --id ses_demo --title "Draft contract"
starcite append ses_demo --agent researcher --text "Found 8 relevant cases..."
starcite tail ses_demo --cursor 0 --limit 1
```

Starcite is a clustered session-stream service for AI applications. It stores
the events that happen inside an AI session as one durable, append-only stream.

That can be agent messages, model output, tool results, state transitions,
human input, or workflow updates.

If a browser refreshes, a WebSocket drops, a service retries, or work moves
between agents and runtimes, Starcite keeps the authoritative stream intact.
Appends are committed before they are acknowledged. Consumers reconnect and pick
up from the last processed `seq`.

Starcite gives you:

- one ordered stream per session
- committed appends with monotonic `seq`
- replay from any cursor, then live continuation on the same connection
- optimistic concurrency with `expected_seq`
- deterministic de-duplication with `(producer_id, producer_seq)`
- hot in-memory reads with cold archive fallback behind one API

It replaces brittle reconnect handling, ad hoc cursor sync, and custom event
reassembly with three primitives: create session, append event, tail stream.

## Use With

Official clients and the terminal CLI live in
[`fastpaca/starcite-clients`](https://github.com/fastpaca/starcite-clients).

Pick the surface you want. They all speak the same session model:

- [`starcite` CLI](https://github.com/fastpaca/starcite-clients/tree/main/packages/starcite-cli) for terminal workflows
- [`@starcite/sdk`](https://github.com/fastpaca/starcite-clients/tree/main/packages/typescript-sdk) for app and browser integration
- [`@starcite/react`](https://github.com/fastpaca/starcite-clients/tree/main/packages/starcite-react) for durable session chat hooks

<details>
<summary>CLI</summary>

Install globally:

```bash
npm install -g starcite
```

Or run once without installing:

```bash
npx starcite --help
```

Use it when you want to create sessions, append events, and tail a shared
timeline from the terminal.

```bash
starcite config set endpoint https://<your-instance>.starcite.io
starcite config set api-key <YOUR_API_KEY>
starcite create --id ses_demo --title "Draft contract"
starcite sessions list --limit 5
starcite append ses_demo --agent researcher --text "Found 8 relevant cases..."
starcite tail ses_demo --cursor 0 --limit 1
```

</details>

<details>
<summary>TypeScript</summary>

Install:

```bash
npm install @starcite/sdk
```

Use it when you want to create sessions, append events, and live-sync a session
log from Node.js or the browser.

```ts
import { Starcite } from "@starcite/sdk";

const starcite = new Starcite({
  baseUrl: process.env.STARCITE_BASE_URL,
});

const session = starcite.session({ token: "<session-jwt>" });

session.on("event", (event) => {
  console.log(event.seq, event.type);
});

await session.append({
  text: "Reviewing clause 4.2...",
  source: "user",
});
```

</details>

<details>
<summary>React</summary>

Install:

```bash
npm install @starcite/react @starcite/sdk ai react
```

Use it when you want a durable chat-style UI driven by the canonical session
log.

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
    <form
      onSubmit={(event) => {
        event.preventDefault();
        void sendMessage({ text: "hello" });
      }}
    >
      <button type="submit">Send</button>
      <p>Status: {status}</p>
      <pre>{JSON.stringify(messages, null, 2)}</pre>
    </form>
  );
}
```

</details>

<details>
<summary>A2A</summary>

Starcite sits underneath A2A. A2A defines how agents talk. Starcite stores what
happened as one durable ordered stream.

Typical layering:

```text
A2A client / agent
        |
   A2A server or gateway
        |
     Starcite
```

Typical mapping:

- A2A `contextId` -> one Starcite session
- A2A messages -> Starcite events
- A2A task status / artifact updates -> typed Starcite events in the same session
- A2A streaming -> tail replay + live continuation

This keeps A2A at the protocol layer and Starcite at the persistence and replay
layer.

If an A2A gateway receives a task update, it can append it into the session log:

```json
{
  "type": "a2a.task.status",
  "payload": {
    "state": "input-required",
    "message": "Please confirm the budget cap."
  },
  "metadata": {
    "protocol": "a2a",
    "context_id": "ctx_123",
    "task_id": "task_456"
  }
}
```

When another consumer reconnects, it tails the same session and rebuilds the
same A2A interaction state from the ordered event history.

</details>

## Why It Exists

AI systems feel brittle when streaming state is implicit.

Tabs close. WebSockets drop. Servers redeploy. Agents hand work to each other.
Retries race. Different consumers disagree about what happened.

Starcite makes that interaction stream explicit and authoritative.

## What You Use It For

Use Starcite when you need:

- a durable conversation log for agents and users
- replayable workflow state for long-running AI jobs
- resumable streams after disconnects or redeploys
- one ordered history shared by browsers, workers, agents, and backend services
- deterministic recovery instead of ad hoc retry and cursor logic

One Starcite session is one ordered interaction log. In practice that might be:

- a user + agent conversation
- an agent task or workflow run
- a collaborative session shared by multiple agents and humans
- any stream of AI events that must be replayable later

## Core Guarantees

- Reliable stream recovery: replay from any cursor after disconnects or restarts
- Durable writes: appends are committed before acknowledgement
- Deterministic ordering: strictly monotonic `seq` per session
- Concurrency safety: optional `expected_seq` checks on append
- Idempotency: `(producer_id, producer_seq)` de-duplication
- Same stream contract everywhere: humans, agents, and services consume the same ordered history
- Low-latency append path: sub-150ms p99 commit path with Raft-backed ordering

## Where It Fits

Starcite sits below the protocol or runtime layer. It is the persistence and
replay layer, not the orchestration layer.

It fits beneath:

- agent-to-agent protocols and gateways
- streaming model runtimes
- human-in-the-loop workflows
- multi-agent applications that need recovery, auditability, and deterministic history

Think about the stack like this:

```text
Agent protocol / app runtime / UI
             |
       append + tail
             |
          Starcite
             |
  Raft ordering + hot event store + archive
```

## How It Works

1. A session is deterministically routed to a Raft group.
2. The leader assigns the next `seq`, replicates to quorum, and acknowledges the append.
3. Tail readers replay from the hot store and archive as needed, then continue live.
4. A background archiver flushes committed events to S3 or Postgres without blocking writes.

The public API stays intentionally small:

- create a session
- append an event
- tail from a cursor

That small surface area is the point. Starcite is meant to be the reliable
stream underneath your application protocol, not another protocol to learn.

## API At A Glance

```bash
# 1) Create a session
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ses_demo",
    "title": "Draft contract",
    "metadata": {"tenant_id": "acme"}
  }'

# 2) Append an event
curl -X POST http://localhost:4000/v1/sessions/ses_demo/append \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Reviewing clause 4.2..."},
    "actor": "agent:drafter",
    "producer_id": "agent-drafter-1",
    "producer_seq": 42,
    "expected_seq": 1
  }'

# Response
# {"seq":42,"last_seq":42,"deduped":false}

# 3) Tail from a cursor (WebSocket)
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

REST and WebSocket details:

- `POST /v1/sessions`
- `GET /v1/sessions`
- `POST /v1/sessions/:id/append`
- `GET /v1/sessions/:id/tail`

Append supports optimistic concurrency with `expected_seq` and deterministic
de-duplication with `(producer_id, producer_seq)`. Tail replays ordered history
and then continues live on the same socket.

## What Starcite Does Not Do

Starcite is deliberately narrow. It does not do:

- agent discovery or capability negotiation
- prompt construction or completion orchestration
- token budgeting or window management
- tool or function-calling abstractions
- OAuth credential issuance
- cross-system agent lifecycle orchestration

## Documentation

- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)
- [Architecture](docs/architecture.md)
- [Self-hosting](docs/self-hosting.md)

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
