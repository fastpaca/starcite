import { createClient } from '@fleetlm/client';

const FLEETLM_URL = process.env.FLEETLM_URL || 'http://localhost:4000/v1';

// Create FleetLM client
const fleetlm = createClient({ baseUrl: FLEETLM_URL });

export async function POST(req: Request) {
  try {
    const { conversationId } = await req.json();

    if (!conversationId) {
      return Response.json({ error: 'conversationId is required' }, { status: 400 });
    }

    // Get the conversation handle (no server call)
    const convo = await fleetlm.conversation(conversationId);

    // Fetch message history
    const { messages } = await convo.tail({ limit: 100 });

    return Response.json({ messages });
  } catch (error: any) {
    // If conversation doesn't exist yet, return empty array
    if (error.message?.includes('Not Found') || error.message?.includes('not found')) {
      return Response.json({ messages: [] });
    }

    console.error('Failed to fetch history:', error);
    return Response.json({ error: 'Failed to fetch history' }, { status: 500 });
  }
}
