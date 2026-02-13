/**
 * Starcite k6: Hot-Path Append Throughput
 *
 * Measures write throughput against session append API.
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './k6-lib.js';

const eventsSent = new Counter('events_sent');
const appendLatency = new Trend('append_latency', true);
const monotonicSeqViolations = new Counter('monotonic_seq_violations');

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

export function setup() {
  const pipelineDepth = Number(__ENV.PIPELINE_DEPTH || 5);

  console.log('Setting up append throughput benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`Max VUs: ${maxVUs}`);
  console.log(`Pipeline depth: ${pipelineDepth}`);

  lib.waitForClusterReady(90);

  const sessions = {};

  for (let vuId = 1; vuId <= maxVUs; vuId++) {
    const id = lib.sessionId('hot-path', vuId);
    lib.ensureSession(id, {
      metadata: { bench: true, scenario: 'hot_path', vu: vuId, run_id: lib.config.runId },
    });
    sessions[vuId] = { sessionId: id, lastSeq: 0 };
  }

  return {
    runId: lib.config.runId,
    pipelineDepth,
    sessions,
  };
}

export default function (data) {
  const vuId = __VU;
  const session = data.sessions[vuId];

  if (!session) return;

  const { sessionId } = session;
  const pipelineDepth = data.pipelineDepth;
  let lastSeq = session.lastSeq || 0;

  for (let i = 0; i < pipelineDepth; i++) {
    const text = `starcite hot-path run=${data.runId} vu=${vuId} iter=${__ITER} idx=${i}`;
    const { res, json } = lib.appendEvent(sessionId, {
      type: 'content',
      payload: { text },
      actor: `agent:vu:${vuId}`,
      source: 'benchmark',
      metadata: {
        bench: true,
        scenario: 'hot_path',
        vu: vuId,
        iter: __ITER,
        idx: i,
      },
    });

    appendLatency.add(res.timings.duration);
    eventsSent.add(1);

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

  session.lastSeq = lastSeq;
}
