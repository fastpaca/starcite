import { openai } from '@ai-sdk/openai';
import { streamText, convertToModelMessages, UIMessage } from 'ai';
import { createClient } from '@fleetlm/client';

export const maxDuration = 30;

const FLEETLM_URL = process.env.FLEETLM_URL || 'http://localhost:4000/v1';

// Create FleetLM client
const fleetlm = createClient({ baseUrl: FLEETLM_URL });

export async function POST(req: Request) {
  const { messages, conversationId } = await req.json();

  if (!conversationId) {
    return Response.json({ error: 'conversationId is required' }, { status: 400 });
  }

  // 1. Get or create conversation (idempotent PUT if config provided)
  const convo = await fleetlm.conversation(conversationId, {
    metadata: { source: 'nextjs-chat' },
  });

  // 2. Append user message (last message in array)
  const lastMessage = messages[messages.length - 1] as UIMessage | undefined;
  if (lastMessage) {
    await convo.append(lastMessage);
  }

  // 3. Fetch recent messages for prompt assembly
  const { messages: convoMessages } = await convo.tail({ limit: 50 });

  // 4. Stream response
  return streamText({
    model: openai('gpt-4o-mini'),
    messages: convertToModelMessages(convoMessages),
  }).toUIMessageStreamResponse({
    onFinish: async ({ responseMessage }) => {
      // FleetLMMessage accepts any object with role and parts
      await convo.append(responseMessage);
    },
  });
}
