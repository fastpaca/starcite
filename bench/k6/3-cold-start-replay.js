/**
 * Fastpaca Scenario 3: Cold-Start Replay
 *
 * Simulates a worker reconnecting after downtime: first build backlog,
 * then replay the full transcript while new writes continue.
 *
 * Profile:
 * - backlogSessions conversations (default 5)
 * - Phase 1: accumulate backlog for BACKLOG_DURATION
 * - Phase 2: replay entire conversation while continuing to append
 *
 * Thresholds (Pass/Fail):
 * - replay_latency p95 < 500ms
 * - active_write_latency p95 < 200ms
 * - http_req_failed < 1%
 *
 * Usage:
 *   k6 run bench/k6/3-cold-start-replay.js
 *
 *   # Larger backlog
 *   k6 run -e BACKLOG_SIZE=5000 -e BACKLOG_SESSIONS=10 bench/k6/3-cold-start-replay.js
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './lib.js';

// ============================================================================
// Metrics
// ============================================================================

const backlogWrites = new Counter('backlog_writes');
const activeWrites = new Counter('active_writes');
const replaysTotal = new Counter('replays_total');
const replayLatency = new Trend('replay_latency', true);
const activeWriteLatency = new Trend('active_write_latency', true);
const messagesReplayed = new Trend('messages_replayed', true);

// ============================================================================
// Test Configuration
// ============================================================================

const backlogSessions = Number(__ENV.BACKLOG_SESSIONS || 5);
const backlogSize = Number(__ENV.BACKLOG_SIZE || 1000);
const backlogDuration = __ENV.BACKLOG_DURATION || '30s';
const replayDuration = __ENV.REPLAY_DURATION || '1m';

export const options = {
  setupTimeout: '120s',
  teardownTimeout: '60s',
  scenarios: {
    accumulate_backlog: {
      executor: 'constant-vus',
      vus: backlogSessions,
      duration: backlogDuration,
      exec: 'accumulateBacklog',
      startTime: '0s',
    },
    replay_under_load: {
      executor: 'constant-vus',
      vus: backlogSessions,
      duration: replayDuration,
      exec: 'replayUnderLoad',
      startTime: backlogDuration,
    },
  },
  thresholds: {
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true }],
    replay_latency: [{ threshold: 'p(95)<500', abortOnFail: false }],
    active_write_latency: [{ threshold: 'p(95)<200', abortOnFail: false }],
  },
};

// ============================================================================
// Setup
// ============================================================================

export function setup() {
  console.log('Setting up cold-start replay benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`Conversations: ${backlogSessions}`);
  console.log(`Target backlog size: ${backlogSize} messages`);
  console.log(`Backlog phase: ${backlogDuration}`);
  console.log(`Replay phase: ${replayDuration}`);

  const conversations = {};

  for (let vuId = 1; vuId <= backlogSessions; vuId++) {
    const convId = lib.conversationId('cold-replay', vuId);
    lib.ensureConversation(convId, {
      metadata: {
        bench: true,
        scenario: 'cold_replay',
        vu: vuId,
        run_id: lib.config.runId,
      },
    });
    conversations[vuId] = { conversationId: convId, lastSeq: 0, version: 0 };
  }

  return {
    runId: lib.config.runId,
    backlogSize,
    conversations,
  };
}

// ============================================================================
// Phase 1: Accumulate Backlog
// ============================================================================

export function accumulateBacklog(data) {
  const vuId = __VU;
  const conv = data.conversations[vuId];
  if (!conv) {
    return;
  }

  const messageText = `backlog message vu=${vuId} iter=${__ITER} run=${data.runId}`;

  const { res, json } = lib.appendMessage(
    conv.conversationId,
    {
      role: 'user',
      parts: [{ type: 'text', text: messageText }],
      metadata: {
        bench: true,
        scenario: 'cold_replay',
        phase: 'backlog',
        vu: vuId,
        iter: __ITER,
      },
    }
  );

  backlogWrites.add(1);

  check(res, {
    'backlog append ok': (r) => r.status >= 200 && r.status < 300,
  });

  if (json) {
    if (typeof json.seq === 'number') {
      conv.lastSeq = json.seq;
    }
    if (typeof json.version === 'number') {
      conv.version = json.version;
    }
  }
}

// ============================================================================
// Phase 2: Replay Under Load
// ============================================================================

export function replayUnderLoad(data) {
  const vuId = __VU;
  const conv = data.conversations[vuId];
  if (!conv) {
    return;
  }

  if (__ITER === 0) {
    // Replay from beginning of conversation
    const { res, json } = lib.getMessages(conv.conversationId, {
      from_seq: 1,
      limit: data.backlogSize,
    });

    replayLatency.add(res.timings.duration);
    replaysTotal.add(1);

    const ok = check(res, {
      'replay fetch ok': (r) => r.status === 200,
    });

    if (ok && json && Array.isArray(json.messages)) {
      messagesReplayed.add(json.messages.length);
      console.log(
        `VU${vuId}: Replayed ${json.messages.length} messages in ${res.timings.duration}ms from conversation ${conv.conversationId}`
      );
    } else if (!ok) {
      console.error(`VU${vuId}: replay failed status=${res.status} body=${res.body}`);
    }
  }

  const appendText = `active message vu=${vuId} iter=${__ITER} run=${data.runId}`;
  const { res, json } = lib.appendMessage(
    conv.conversationId,
    {
      role: 'assistant',
      parts: [{ type: 'text', text: appendText }],
      metadata: {
        bench: true,
        scenario: 'cold_replay',
        phase: 'active',
        vu: vuId,
        iter: __ITER,
      },
    }
  );

  activeWriteLatency.add(res.timings.duration);
  activeWrites.add(1);

  check(res, {
    'active append ok': (r) => r.status >= 200 && r.status < 300,
  });

  if (json) {
    if (typeof json.seq === 'number') {
      conv.lastSeq = json.seq;
    }
    if (typeof json.version === 'number') {
      conv.version = json.version;
    }
  }
}
