import type { AppendResponse, FleetLMMessage } from './types';

/**
 * Conversation class - represents a single conversation (append-only message log)
 *
 * This is a message backend substrate. It stores and streams messages
 * but does NOT manage prompt windows, token budgets, or compaction.
 */
export class Conversation {
  constructor(
    private conversationId: string,
    private baseUrl: string,
    private apiKey?: string
  ) {}

  /**
   * Append a message to the conversation
   *
   * @param message - Any object with `role` and `parts` (FleetLMMessage-compatible)
   * @param opts - Optional parameters including token count and version guard
   */
  async append(
    message: { role: string; parts: readonly { type: string; [key: string]: unknown }[] },
    opts?: { ifVersion?: number; tokenCount?: number; metadata?: Record<string, unknown> }
  ): Promise<AppendResponse> {
    const response = await fetch(`${this.baseUrl}/conversations/${this.conversationId}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(this.apiKey && { Authorization: `Bearer ${this.apiKey}` }),
      },
      body: JSON.stringify({
        message: {
          role: message.role,
          parts: message.parts,
          ...(typeof opts?.tokenCount === 'number' ? { token_count: opts.tokenCount } : {}),
          ...(opts?.metadata ? { metadata: opts.metadata } : {}),
        },
        if_version: opts?.ifVersion,
      }),
    });

    if (!response.ok) {
      throw new Error(`Failed to append message: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Get messages from the tail (newest first) with pagination
   *
   * @param opts.offset - Number of messages to skip from tail (0 = most recent)
   * @param opts.limit - Maximum messages to return
   */
  async tail(opts?: { offset?: number; limit?: number }): Promise<{ messages: FleetLMMessage[] }> {
    const params = new URLSearchParams();
    if (typeof opts?.offset === 'number') params.set('offset', String(opts.offset));
    if (typeof opts?.limit === 'number') params.set('limit', String(opts.limit));

    const url = `${this.baseUrl}/conversations/${this.conversationId}/tail${params.toString() ? `?${params}` : ''}`;
    const response = await fetch(url, {
      headers: {
        ...(this.apiKey && { Authorization: `Bearer ${this.apiKey}` }),
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch tail: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Replay messages by sequence number range
   *
   * Use this for replaying from a known position (e.g., after a gap event in websocket).
   *
   * @param opts.from - Starting sequence number (inclusive)
   * @param opts.limit - Maximum messages to return
   */
  async replay(opts?: { from?: number; limit?: number }): Promise<{ messages: FleetLMMessage[] }> {
    const params = new URLSearchParams();
    if (typeof opts?.from === 'number') params.set('from', String(opts.from));
    if (typeof opts?.limit === 'number') params.set('limit', String(opts.limit));

    const url = `${this.baseUrl}/conversations/${this.conversationId}/messages${params.toString() ? `?${params}` : ''}`;
    const response = await fetch(url, {
      headers: {
        ...(this.apiKey && { Authorization: `Bearer ${this.apiKey}` }),
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to replay messages: ${response.statusText}`);
    }

    return response.json();
  }
}
