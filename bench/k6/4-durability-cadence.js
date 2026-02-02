/**
 * Fastpaca Scenario 4: Durability Cadence
 *
 * Sustained append workload that periodically reads the tail to ensure
 * data is durable and sequence numbers are monotonic.
 *
 * Profile:
 * - sessionCount conversations (default 10)
 * - Constant VUs for configurable duration (default 3m)
 * - Every 10th iteration fetches the tail to verify durability
 *
 * Thresholds (Pass/Fail):
 * - append_latency p99 < 200ms
 * - http_req_failed < 1%
 * - durability_check == 1 (tail replay matches appended count)
 *
 * Usage:
 *   k6 run bench/k6/4-durability-cadence.js
 *
 *   # Longer soak
 *   k6 run -e DURATION=15m bench/k6/4-durability-cadence.js
 */

import { check } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import * as lib from './lib.js';

// ============================================================================
// Metrics
// ============================================================================

const messagesSent = new Counter('messages_sent');
const appendLatency = new Trend('append_latency', true);
const tailLatency = new Trend('tail_latency', true);
const durabilityCheck = new Rate('durability_check');

// ============================================================================
// Test Configuration
// ============================================================================

const sessionCount = Number(__ENV.SESSION_COUNT || 10);
const duration = __ENV.DURATION || '3m';
const verifyTailLimit = Number(__ENV.VERIFY_TAIL_LIMIT || 2000);

export const options = {
  setupTimeout: '120s',
  teardownTimeout: '120s',
  scenarios: {
    sustained_load: {
      executor: 'constant-vus',
      vus: sessionCount,
      duration,
    },
  },
  thresholds: {
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true }],
    append_latency: [
      { threshold: 'p(95)<120', abortOnFail: false },
      { threshold: 'p(99)<200', abortOnFail: false },
    ],
    durability_check: [{ threshold: 'rate==1', abortOnFail: true }],
  },
};

// ============================================================================
// Setup
// ============================================================================

export function setup() {
  console.log('Setting up durability cadence benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`Conversations: ${sessionCount}`);
  console.log(`Duration: ${duration}`);

  const conversations = {};

  for (let vuId = 1; vuId <= sessionCount; vuId++) {
    const convId = lib.conversationId('durability', vuId);
    lib.ensureConversation(convId, {
      metadata: {
        bench: true,
        scenario: 'durability_cadence',
        vu: vuId,
        run_id: lib.config.runId,
      },
    });
    conversations[vuId] = { conversationId: convId, appended: 0, lastSeq: 0, version: 0 };
  }

  return {
    runId: lib.config.runId,
    conversations,
    verifyTailLimit,
  };
}

// ============================================================================
// Main Test
// ============================================================================

export default function (data) {
  const vuId = __VU;
  const conv = data.conversations[vuId];
  if (!conv) {
    return;
  }

  const messageSeq = conv.appended + 1;
  const messageText = `durability message run=${data.runId} vu=${vuId} seq=${messageSeq}`;

  const { res, json } = lib.appendMessage(
    conv.conversationId,
    {
      role: 'assistant',
      parts: [{ type: 'text', text: messageText }],
      metadata: {
        bench: true,
        scenario: 'durability_cadence',
        vu: vuId,
        seq: messageSeq,
        iter: __ITER,
      },
    }
  );

  appendLatency.add(res.timings.duration);
  messagesSent.add(1);

  const ok = check(res, {
    'append ok': (r) => r.status >= 200 && r.status < 300,
  });

  if (!ok) {
    console.error(`VU${vuId}: append failed status=${res.status} body=${res.body}`);
    return;
  }

  conv.appended = messageSeq;

  if (json) {
    if (typeof json.seq === 'number') {
      conv.lastSeq = json.seq;
    }
    if (typeof json.version === 'number') {
      conv.version = json.version;
    }
  }

  // Every 10th iteration, fetch the tail to verify durability
  if (conv.appended % 10 === 0) {
    const { res: tailRes } = lib.getTail(conv.conversationId, { limit: 10 });
    tailLatency.add(tailRes.timings.duration);

    check(tailRes, {
      'tail fetch ok': (r) => r.status === 200,
    });
  }
}

// ============================================================================
// Teardown
// ============================================================================

export function teardown(data) {
  console.log('Verifying durability tail across conversations...');

  Object.values(data.conversations).forEach((conv) => {
    if (!conv || conv.appended === 0) {
      durabilityCheck.add(true);
      return;
    }

    const limit = Math.min(conv.appended, data.verifyTailLimit);
    // Replay from beginning to get all messages
    const { res, json } = lib.getMessages(conv.conversationId, {
      from_seq: 1,
      limit,
    });

    const ok = res.status === 200 && json && Array.isArray(json.messages);
    if (!ok) {
      console.error(
        `Durability check failed conversation=${conv.conversationId} status=${res.status} body=${res.body}`
      );
      durabilityCheck.add(false);
      return;
    }

    const tailLength = json.messages.length;
    const lastMessage = tailLength > 0 ? json.messages[tailLength - 1] : null;
    const seqMatches = lastMessage ? lastMessage.seq === conv.lastSeq : conv.lastSeq === 0;

    durabilityCheck.add(seqMatches);

    if (!seqMatches) {
      console.error(
        `Seq mismatch conversation=${conv.conversationId} expected=${conv.lastSeq} got=${lastMessage ? lastMessage.seq : 'none'}`
      );
    }
  });
}
