/**
 * Starcite Scenario 2: Append Mix
 *
 * Exercises append traffic with mixed payload sizes and optimistic concurrency.
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './lib.js';

const writesTotal = new Counter('writes_total');
const writeLatency = new Trend('write_latency', true);
const expectedSeqConflicts = new Counter('expected_seq_conflicts');

const vus = Number(__ENV.VUS || 50);
const payloadSize = Number(__ENV.PAYLOAD_SIZE || 1024);
const duration = __ENV.DURATION || '2m';

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '60s',
  scenarios: {
    append_mix: {
      executor: 'constant-vus',
      vus,
      duration,
    },
  },
  thresholds: {
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true }],
    write_latency: [{ threshold: 'p(95)<120', abortOnFail: false }],
  },
};

export function setup() {
  console.log('Setting up append mix benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`VUs: ${vus}`);

  const sessions = [];

  for (let i = 0; i < vus; i++) {
    const sessionId = lib.sessionId('append-mix', i + 1);
    lib.ensureSession(sessionId, {
      metadata: {
        bench: true,
        scenario: 'append_mix',
        slot: i,
        run_id: lib.config.runId,
      },
    });
    sessions.push({ sessionId, lastSeq: 0 });
  }

  return {
    runId: lib.config.runId,
    payloadSize,
    sessions,
  };
}

export default function (data) {
  const vuIndex = (__VU - 1) % data.sessions.length;
  const session = data.sessions[vuIndex];
  if (!session) return;

  const textContent = 'x'.repeat(data.payloadSize);
  const actor = __ITER % 2 === 0 ? `agent:vu:${__VU}` : `user:vu:${__VU}`;

  const { res, json } = lib.appendEvent(
    session.sessionId,
    {
      type: 'content',
      payload: { text: textContent },
      actor,
      source: 'benchmark',
      metadata: {
        bench: true,
        scenario: 'append_mix',
        vu: __VU,
        iter: __ITER,
      },
    },
    { expectedSeq: session.lastSeq }
  );

  writeLatency.add(res.timings.duration);
  writesTotal.add(1);

  const ok = check(res, {
    'append ok or expected conflict': (r) =>
      (r.status >= 200 && r.status < 300) || (r.status === 409 && r.body.includes('expected_seq_conflict')),
  });

  if (!ok) {
    console.error(`VU${__VU}: append failed status=${res.status} body=${res.body}`);
    return;
  }

  if (res.status === 409) {
    expectedSeqConflicts.add(1);
    return;
  }

  if (json && typeof json.seq === 'number') {
    session.lastSeq = json.seq;
  }
}
