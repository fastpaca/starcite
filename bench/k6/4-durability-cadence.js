/**
 * Starcite Scenario 4: Sustained Ordered Appends
 *
 * Sustained append workload with strict expected_seq guards.
 */

import { check } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import * as lib from './lib.js';

const eventsSent = new Counter('events_sent');
const appendLatency = new Trend('append_latency', true);
const orderingCheck = new Rate('ordering_check');

const sessionCount = Number(__ENV.SESSION_COUNT || 10);
const duration = __ENV.DURATION || '3m';

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
    ordering_check: [{ threshold: 'rate==1', abortOnFail: true }],
  },
};

export function setup() {
  console.log('Setting up sustained ordered append benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);

  lib.waitForClusterReady(90);

  const sessions = {};

  for (let vuId = 1; vuId <= sessionCount; vuId++) {
    const id = lib.sessionId('durability', vuId);
    lib.ensureSession(id, {
      metadata: {
        bench: true,
        scenario: 'durability_cadence',
        vu: vuId,
        run_id: lib.config.runId,
      },
    });
    sessions[vuId] = { sessionId: id, lastSeq: 0 };
  }

  return {
    runId: lib.config.runId,
    sessions,
  };
}

export default function (data) {
  const vuId = __VU;
  const session = data.sessions[vuId];
  if (!session) return;

  const expected = session.lastSeq;
  const { res, json } = lib.appendEvent(
    session.sessionId,
    {
      type: 'content',
      payload: { text: `durability run=${data.runId} vu=${vuId} iter=${__ITER}` },
      actor: `agent:vu:${vuId}`,
      source: 'benchmark',
      metadata: {
        bench: true,
        scenario: 'durability_cadence',
        vu: vuId,
        iter: __ITER,
      },
    },
    { expectedSeq: expected }
  );

  appendLatency.add(res.timings.duration);
  eventsSent.add(1);

  const ok = check(res, {
    'append success': (r) => r.status >= 200 && r.status < 300,
  });

  if (!ok) {
    orderingCheck.add(false);
    return;
  }

  if (json && typeof json.seq === 'number') {
    const ordered = json.seq === expected + 1;
    orderingCheck.add(ordered);
    session.lastSeq = json.seq;
  } else {
    orderingCheck.add(false);
  }
}
