/**
 * Loose message types to avoid coupling to ai-sdk.
 * Shape matches our API: role + parts with a type discriminator.
 */
export type FastpacaMessagePart = {
  type: string;
  [key: string]: unknown;
};

export interface FastpacaMessage {
  seq?: number;
  role: string;
  parts: FastpacaMessagePart[];
  metadata?: Record<string, unknown>;
  token_count?: number;
  inserted_at?: string;
}

/**
 * Conversation metadata
 */
export interface ConversationInfo {
  id: string;
  version: number;
  tombstoned: boolean;
  last_seq: number;
  metadata?: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

/**
 * Append response
 */
export interface AppendResponse {
  seq: number;
  version: number;
  token_count?: number;
}

/**
 * Client configuration
 */
export interface FastpacaClientConfig {
  baseUrl?: string;
  apiKey?: string;
}
