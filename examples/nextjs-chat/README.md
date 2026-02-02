# Fastpaca × Next.js Chat Example

Minimal chat app demonstrating Fastpaca conversation logs with ai-sdk and Next.js.

![](https://fastpaca.com/images/context-store-demo.gif)

## Features

- **Pure ai-sdk frontend** - Uses `useChat`, no Fastpaca client code in the UI
- **Backend integration** - All Fastpaca calls in the API route
- **Prompt assembly** - Builds the model prompt from the latest messages (tail)
- **gpt-4o-mini** - Fast, affordable model for the demo

## Setup

```bash
cd examples/nextjs-chat
cp .env.example .env.local
# Edit .env.local and add your OPENAI_API_KEY

npm install
npm run dev
```

## How it Works

### Frontend (Standard ai-sdk)
```tsx
// app/page.tsx
import { useChat } from '@ai-sdk/react';

const { messages, input, handleSubmit } = useChat();
// That's it! No Fastpaca code in frontend
```

### Backend (Fastpaca Integration)
```typescript
// app/api/chat/route.ts
export async function POST(req: Request) {
  const { messages, conversationId } = await req.json();

  // 1. Get conversation ID from session/user
  const convo = await fastpaca.conversation(conversationId, {
    metadata: { source: 'nextjs-chat' }
  });

  // 2. Append last user message to Fastpaca
  await convo.append(messages[messages.length - 1]);

  // 3. Build prompt from the latest messages
  const { messages: convoMessages } = await convo.tail({ limit: 50 });

  // 4. Stream to OpenAI
  return streamText({
    model: openai('gpt-4o-mini'),
    messages: convertToModelMessages(convoMessages),
  }).toUIMessageStreamResponse({
    onFinish: async ({ responseMessage }) => {
      await convo.append(responseMessage);
    },
  });
}
```

### Prompt Assembly
Fastpaca does not build LLM context windows. This demo uses the latest 50 messages as the prompt. Adjust that limit or replace it with your own summarization / retrieval strategy.

## Architecture

```
Browser → useChat (ai-sdk)
            ↓
      POST /api/chat (Next.js)
            ↓
      Fastpaca REST API
       - Append message
       - Tail read (latest messages)
            ↓
      OpenAI gpt-4o-mini
            ↓
      Stream → Browser
```

All Fastpaca logic is hidden in the backend. Frontend is pure ai-sdk.

## Files

- `app/page.tsx` - Chat UI (standard `useChat`)
- `app/api/chat/route.ts` - Fastpaca integration + streaming

## Running

1. Start Fastpaca: `mix phx.server` (from repo root)
2. Start Next.js: `npm run dev`
3. Open: http://localhost:3000
