/**
 * FleetLM Scenario 3: Idempotency Under Load
 *
 * Simulates retries by reusing idempotency keys while appending concurrent traffic.
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './lib.js';

const writesTotal = new Counter('writes_total');
const dedupedTotal = new Counter('deduped_total');
const conflictsTotal = new Counter('idempotency_conflicts_total');
const appendLatency = new Trend('append_latency', true);

const sessionCount = Number(__ENV.BACKLOG_SESSIONS || 5);
const duration = __ENV.REPLAY_DURATION || '1m';
const enableConflictCheck = __ENV.ENABLE_CONFLICT_CHECK === 'true';

export const options = {
  setupTimeout: '120s',
  teardownTimeout: '60s',
  scenarios: {
    idempotency_load: {
      executor: 'constant-vus',
      vus: sessionCount,
      duration,
    },
  },
  thresholds: {
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true }],
    append_latency: [{ threshold: 'p(95)<200', abortOnFail: false }],
  },
};

export function setup() {
  console.log('Setting up idempotency benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);

  const sessions = {};

  for (let vuId = 1; vuId <= sessionCount; vuId++) {
    const id = lib.sessionId('idempotency', vuId);
    lib.ensureSession(id, {
      metadata: {
        bench: true,
        scenario: 'idempotency',
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

  const key = `k-${vuId}-${__ITER}`;
  const payload = { text: `idempotency run=${data.runId} vu=${vuId} iter=${__ITER}` };

  const first = lib.appendEvent(session.sessionId, {
    type: 'state',
    payload,
    actor: `agent:vu:${vuId}`,
    source: 'benchmark',
    idempotency_key: key,
  });

  appendLatency.add(first.res.timings.duration);
  writesTotal.add(1);

  check(first.res, {
    'first append ok': (r) => r.status >= 200 && r.status < 300,
  });

  const retry = lib.appendEvent(session.sessionId, {
    type: 'state',
    payload,
    actor: `agent:vu:${vuId}`,
    source: 'benchmark',
    idempotency_key: key,
  });

  appendLatency.add(retry.res.timings.duration);
  writesTotal.add(1);

  const retryOk = check(retry.res, {
    'retry append ok': (r) => r.status >= 200 && r.status < 300,
  });

  if (retryOk && retry.json && retry.json.deduped === true) {
    dedupedTotal.add(1);
  }

  // Optional conflict exercise: same key + different payload => 409 idempotency_conflict.
  if (enableConflictCheck && __ITER % 10 === 0) {
    const conflictKey = `conflict-${vuId}-${__ITER}`;

    lib.appendEvent(session.sessionId, {
      type: 'state',
      payload: { text: 'baseline payload' },
      actor: `agent:vu:${vuId}`,
      source: 'benchmark',
      idempotency_key: conflictKey,
    });

    const conflict = lib.appendEvent(session.sessionId, {
      type: 'state',
      payload: { text: 'different payload' },
      actor: `agent:vu:${vuId}`,
      source: 'benchmark',
      idempotency_key: conflictKey,
    });

    if (conflict.res.status === 409 && conflict.res.body.includes('idempotency_conflict')) {
      conflictsTotal.add(1);
    }
  }

  if (first.json && typeof first.json.seq === 'number') {
    session.lastSeq = Math.max(session.lastSeq, first.json.seq);
  }
}
