/**
 * Fastpaca Scenario 2: Conversation Read/Write Mix
 *
 * Exercises alternating append + tail reads to model a busy chat UI.
 *
 * Profile:
 * - 50 constant VUs (default)
 * - Each VU alternates: append → fetch tail → append → fetch messages replay
 * - Payload: configurable text size to simulate user/tool output
 *
 * Thresholds (Pass/Fail):
 * - http_req_failed < 1% (stability)
 * - write_latency p95 < 120ms
 * - read_latency p95 < 160ms
 *
 * Usage:
 *   k6 run bench/k6/2-rest-read-write-mix.js
 *
 *   # Heavier read mix / larger payload
 *   k6 run -e VUS=100 -e PAYLOAD_SIZE=2048 bench/k6/2-rest-read-write-mix.js
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './lib.js';

// ============================================================================
// Metrics
// ============================================================================

const writesTotal = new Counter('writes_total');
const readsTotal = new Counter('reads_total');
const writeLatency = new Trend('write_latency', true);
const readLatency = new Trend('read_latency', true);
const messagesReadCount = new Trend('messages_read_count', true);

// ============================================================================
// Test Configuration
// ============================================================================

const vus = Number(__ENV.VUS || 50);
const payloadSize = Number(__ENV.PAYLOAD_SIZE || 1024);
const duration = __ENV.DURATION || '2m';

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '60s',
  scenarios: {
    rest_mix: {
      executor: 'constant-vus',
      vus,
      duration,
    },
  },
  thresholds: {
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true }],
    write_latency: [{ threshold: 'p(95)<120', abortOnFail: false }],
    read_latency: [{ threshold: 'p(95)<160', abortOnFail: false }],
  },
};

// ============================================================================
// Setup
// ============================================================================

export function setup() {
  console.log('Setting up conversation read/write mix benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`VUs: ${vus}`);
  console.log(`Payload size: ${payloadSize} bytes`);
  console.log(`Duration: ${duration}`);

  const conversations = [];

  for (let i = 0; i < vus; i++) {
    const convId = lib.conversationId('rw-mix', i + 1);
    lib.ensureConversation(convId, {
      metadata: {
        bench: true,
        scenario: 'rest_mix',
        slot: i,
        run_id: lib.config.runId,
      },
    });
    conversations.push({ conversationId: convId, lastSeq: 0, version: 0 });
  }

  return {
    runId: lib.config.runId,
    payloadSize,
    conversations,
  };
}

// ============================================================================
// Main Test
// ============================================================================

export default function (data) {
  const vuIndex = (__VU - 1) % data.conversations.length;
  const conv = data.conversations[vuIndex];

  if (!conv) {
    return;
  }

  const { conversationId } = conv;
  const textContent = 'x'.repeat(data.payloadSize);
  const phase = __ITER % 4;

  if (phase === 0 || phase === 2) {
    // Append message
    const { res, json } = lib.appendMessage(
      conversationId,
      {
        role: phase === 0 ? 'user' : 'assistant',
        parts: [{ type: 'text', text: textContent }],
        metadata: {
          bench: true,
          scenario: 'rest_mix',
          phase: phase === 0 ? 'user' : 'assistant',
          vu: __VU,
          iter: __ITER,
        },
      }
    );

    writeLatency.add(res.timings.duration);
    writesTotal.add(1);

    check(res, {
      'append ok': (r) => r.status >= 200 && r.status < 300,
    });

    if (json) {
      if (typeof json.seq === 'number') {
        conv.lastSeq = json.seq;
      }
      if (typeof json.version === 'number') {
        conv.version = json.version;
      }
    }
    return;
  }

  if (phase === 1) {
    // Fetch most recent messages via tail endpoint
    const { res, json } = lib.getTail(conversationId, { limit: 50 });
    readLatency.add(res.timings.duration);
    readsTotal.add(1);

    check(res, {
      'tail fetch ok': (r) => r.status === 200,
    });

    if (json && Array.isArray(json.messages)) {
      messagesReadCount.add(json.messages.length);
    }
    return;
  }

  // phase === 3: replay from start of conversation
  const { res, json } = lib.getMessages(conversationId, { from_seq: 1, limit: 50 });
  readLatency.add(res.timings.duration);
  readsTotal.add(1);

  check(res, {
    'messages fetch ok': (r) => r.status === 200,
  });

  if (json && Array.isArray(json.messages)) {
    messagesReadCount.add(json.messages.length);
  }
}
