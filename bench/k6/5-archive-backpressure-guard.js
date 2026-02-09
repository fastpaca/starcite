/**
 * Starcite Scenario 5: Archive Backpressure Guard
 *
 * Intentionally drives append pressure and verifies that overload is surfaced
 * as controlled `429 archive_backpressure` responses instead of unbounded growth.
 *
 * Recommended server-side settings for this scenario:
 *   STARCITE_ARCHIVER_ENABLED=true
 *   STARCITE_ARCHIVE_FLUSH_INTERVAL_MS=15000
 *   STARCITE_MAX_UNARCHIVED_EVENTS=200
 */

import { Counter, Rate, Trend } from 'k6/metrics';
import * as lib from './lib.js';

const appendAttempts = new Counter('append_attempts');
const appendAcceptedTotal = new Counter('append_accepted_total');
const archiveBackpressure429Total = new Counter('archive_backpressure_429_total');
const appendUnexpectedFailuresTotal = new Counter('append_unexpected_failures_total');

const appendAcceptedRate = new Rate('append_accepted_rate');
const archiveBackpressure429Rate = new Rate('archive_backpressure_429_rate');
const appendUnexpectedFailureRate = new Rate('append_unexpected_failure_rate');

const appendLatency = new Trend('append_latency', true);
const backpressureLatency = new Trend('backpressure_latency', true);

const vus = Number(__ENV.VUS || 80);
const pipelineDepth = Number(__ENV.PIPELINE_DEPTH || 10);
const rampDuration = __ENV.RAMP_DURATION || '10s';
const steadyDuration = __ENV.STEADY_DURATION || '20s';
const rampDownDuration = __ENV.RAMP_DOWN_DURATION || '5s';

const requireBackpressure = (__ENV.REQUIRE_BACKPRESSURE || 'true') !== 'false';
const minBackpressureCount = Number(__ENV.MIN_BACKPRESSURE_COUNT || 1);
const minBackpressureRate = Number(__ENV.MIN_BACKPRESSURE_RATE || 0.01);
const maxUnexpectedFailures = Number(__ENV.MAX_UNEXPECTED_FAILURES || 0);
const maxAcceptedP95Ms = Number(__ENV.MAX_ACCEPTED_P95_MS || 40);

const thresholds = {
  append_accepted_total: [{ threshold: 'count>0', abortOnFail: true }],
  append_unexpected_failures_total: [
    { threshold: `count<=${maxUnexpectedFailures}`, abortOnFail: true },
  ],
  append_unexpected_failure_rate: [{ threshold: 'rate==0', abortOnFail: true }],
  append_latency: [{ threshold: `p(95)<${maxAcceptedP95Ms}`, abortOnFail: false }],
};

if (requireBackpressure) {
  thresholds.archive_backpressure_429_total = [
    { threshold: `count>=${minBackpressureCount}`, abortOnFail: true },
  ];
  thresholds.archive_backpressure_429_rate = [
    { threshold: `rate>${minBackpressureRate}`, abortOnFail: true },
  ];
}

export const options = {
  setupTimeout: '120s',
  teardownTimeout: '60s',
  scenarios: {
    archive_backpressure_guard: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: rampDuration, target: vus },
        { duration: steadyDuration, target: vus },
        { duration: rampDownDuration, target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds,
};

export function setup() {
  console.log('Setting up archive backpressure guard benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`VUs: ${vus}`);
  console.log(`Pipeline depth: ${pipelineDepth}`);
  console.log(`Backpressure required: ${requireBackpressure}`);

  lib.waitForClusterReady(90);

  const sessions = {};

  for (let vuId = 1; vuId <= vus; vuId++) {
    const id = lib.sessionId('backpressure', vuId, lib.config.runId);
    lib.ensureSession(id, {
      metadata: {
        bench: true,
        scenario: 'archive_backpressure_guard',
        vu: vuId,
        run_id: lib.config.runId,
      },
    });

    sessions[vuId] = { sessionId: id, lastSeq: 0 };
  }

  return { runId: lib.config.runId, sessions };
}

function isArchiveBackpressure(res, json) {
  if (res.status !== 429) return false;

  if (json && json.error === 'archive_backpressure') {
    return true;
  }

  return typeof res.body === 'string' && res.body.includes('archive_backpressure');
}

export default function (data) {
  const session = data.sessions[__VU];
  if (!session) return;

  for (let i = 0; i < pipelineDepth; i++) {
    const { res, json } = lib.appendEvent(session.sessionId, {
      type: 'content',
      payload: {
        text: `backpressure run=${data.runId} vu=${__VU} iter=${__ITER} idx=${i}`,
      },
      actor: `agent:vu:${__VU}`,
      source: 'benchmark',
      metadata: {
        bench: true,
        scenario: 'archive_backpressure_guard',
        vu: __VU,
        iter: __ITER,
        idx: i,
      },
    });

    appendAttempts.add(1);

    if (res.status >= 200 && res.status < 300) {
      appendAcceptedTotal.add(1);
      appendAcceptedRate.add(true);
      archiveBackpressure429Rate.add(false);
      appendUnexpectedFailureRate.add(false);
      appendLatency.add(res.timings.duration);

      if (json && typeof json.seq === 'number') {
        session.lastSeq = Math.max(session.lastSeq, json.seq);
      }

      continue;
    }

    if (isArchiveBackpressure(res, json)) {
      archiveBackpressure429Total.add(1);
      archiveBackpressure429Rate.add(true);
      appendAcceptedRate.add(false);
      appendUnexpectedFailureRate.add(false);
      backpressureLatency.add(res.timings.duration);
      continue;
    }

    appendUnexpectedFailuresTotal.add(1);
    appendUnexpectedFailureRate.add(true);
    appendAcceptedRate.add(false);
    archiveBackpressure429Rate.add(false);
  }
}
