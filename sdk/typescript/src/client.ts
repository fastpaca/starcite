import { Conversation } from './conversation';
import type { FleetLMClientConfig, ConversationInfo } from './types';

/**
 * FleetLM client - message backend for AI agents
 *
 * This is a message substrate. It stores and streams messages
 * but does NOT manage prompt windows, token budgets, or compaction.
 * Prompt assembly is handled by Cria or other prompt systems.
 */
export class FleetLMClient {
  private baseUrl: string;
  private apiKey?: string;

  constructor(config: FleetLMClientConfig = {}) {
    this.baseUrl = config.baseUrl || 'http://localhost:4000/v1';
    this.apiKey = config.apiKey;
  }

  /**
   * Get or create a conversation
   *
   * If options are provided, calls PUT /v1/conversations/:id (idempotent create/update).
   * Otherwise returns a Conversation instance immediately.
   *
   * @example
   * ```ts
   * // Create/update conversation with metadata
   * const conv = await fleetlm.conversation('chat-123', {
   *   metadata: { user_id: 'u_123', channel: 'web' }
   * });
   *
   * // Get existing conversation (no server call)
   * const conv = fleetlm.conversation('chat-123');
   * ```
   */
  async conversation(
    id: string,
    opts?: {
      metadata?: Record<string, unknown>;
    }
  ): Promise<Conversation> {
    // If options provided, create/update the conversation
    if (opts) {
      const response = await fetch(`${this.baseUrl}/conversations/${id}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          ...(this.apiKey && { Authorization: `Bearer ${this.apiKey}` }),
        },
        body: JSON.stringify({
          metadata: opts.metadata,
        }),
      });

      if (!response.ok) {
        throw new Error(`Failed to create/update conversation: ${response.statusText}`);
      }
    }

    return new Conversation(id, this.baseUrl, this.apiKey);
  }

  /**
   * Get conversation info
   */
  async getConversation(id: string): Promise<ConversationInfo> {
    const response = await fetch(`${this.baseUrl}/conversations/${id}`, {
      headers: {
        ...(this.apiKey && { Authorization: `Bearer ${this.apiKey}` }),
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to get conversation: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Tombstone a conversation (soft delete, prevents new writes)
   */
  async tombstoneConversation(id: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/conversations/${id}`, {
      method: 'DELETE',
      headers: {
        ...(this.apiKey && { Authorization: `Bearer ${this.apiKey}` }),
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to tombstone conversation: ${response.statusText}`);
    }
  }
}

/**
 * Create a FleetLM client
 */
export function createClient(config?: FleetLMClientConfig): FleetLMClient {
  return new FleetLMClient(config);
}
