/**
 * Fastpaca Scenario 1: Hot-Path Append Throughput
 *
 * Measures write throughput directly against the conversation append API.
 *
 * Profile:
 * - 20 concurrent VUs writing into dedicated conversations
 * - Pipeline depth controls how many appends each VU issues per iteration
 * - Ramp shape: 30s ramp → 1m40 steady → 10s ramp down
 *
 * Thresholds (Pass/Fail):
 * - append_latency p95 < 40ms (healthy single-node target)
 * - append_latency p99 < 120ms (guardrail under duress)
 * - checks > 99% (append success rate)
 *
 * Usage:
 *   # Default (20 VUs, pipeline depth 5)
 *   k6 run bench/k6/1-hot-path-throughput.js
 *
 *   # Higher load
 *   k6 run -e PIPELINE_DEPTH=10 -e MAX_VUS=50 bench/k6/1-hot-path-throughput.js
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './lib.js';

// ============================================================================
// Metrics
// ============================================================================

const messagesSent = new Counter('messages_sent');
const appendLatency = new Trend('append_latency', true);
const monotonicSeqViolations = new Counter('monotonic_seq_violations');

// ============================================================================
// Test Configuration
// ============================================================================

const maxVUs = Number(__ENV.MAX_VUS || 20);
const rampDuration = __ENV.RAMP_DURATION || '30s';
const steadyDuration = __ENV.STEADY_DURATION || '1m40s';
const rampDownDuration = __ENV.RAMP_DOWN_DURATION || '10s';

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '60s',
  scenarios: {
    throughput: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: rampDuration, target: maxVUs },
        { duration: steadyDuration, target: maxVUs },
        { duration: rampDownDuration, target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    checks: [{ threshold: 'rate>0.99', abortOnFail: true }],
    append_latency: [
      { threshold: 'p(95)<40', abortOnFail: false },
      { threshold: 'p(99)<120', abortOnFail: false },
    ],
  },
};

// ============================================================================
// Setup
// ============================================================================

export function setup() {
  const pipelineDepth = Number(__ENV.PIPELINE_DEPTH || 5);

  console.log('Setting up append throughput benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`Max VUs: ${maxVUs}`);
  console.log(`Pipeline depth: ${pipelineDepth}`);
  console.log(`Ramp: ${rampDuration} → Steady: ${steadyDuration} → Down: ${rampDownDuration}`);

  // Wait for cluster to be ready before creating conversations
  lib.waitForClusterReady(90);

  const conversations = {};

  for (let vuId = 1; vuId <= maxVUs; vuId++) {
    const convId = lib.conversationId('hot-path', vuId);
    lib.ensureConversation(convId, {
      metadata: { bench: true, scenario: 'hot_path', vu: vuId, run_id: lib.config.runId },
    });
    conversations[vuId] = { conversationId: convId, lastSeq: 0 };
  }

  return {
    runId: lib.config.runId,
    pipelineDepth,
    conversations,
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

  const { conversationId } = conv;
  const pipelineDepth = data.pipelineDepth;
  let lastSeq = conv.lastSeq || 0;

  for (let i = 0; i < pipelineDepth; i++) {
    const text = `fastpaca hot-path run=${data.runId} vu=${vuId} iter=${__ITER} idx=${i}`;
    const { res, json } = lib.appendMessage(
      conversationId,
      {
        role: 'user',
        parts: [{ type: 'text', text }],
        metadata: {
          bench: true,
          scenario: 'hot_path',
          vu: vuId,
          iter: __ITER,
          idx: i,
        },
      }
    );

    appendLatency.add(res.timings.duration);
    messagesSent.add(1);

    const ok = check(res, {
      'append success': (r) => r.status >= 200 && r.status < 300,
    });

    if (!ok) {
      console.error(`VU${vuId}: append failed status=${res.status} body=${res.body}`);
      continue;
    }

    if (json && typeof json.seq === 'number') {
      if (json.seq <= lastSeq) {
        monotonicSeqViolations.add(1);
      }
      lastSeq = json.seq;
    }
  }

  conv.lastSeq = lastSeq;
}
