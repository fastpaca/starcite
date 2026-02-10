/**
 * Starcite Scenario 1: Hot-Path Append Throughput
 *
 * Measures write throughput against session append API.
 */

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import * as lib from './lib.js';

const eventsSent = new Counter('events_sent');
const appendLatency = new Trend('append_latency', true);
const monotonicSeqViolations = new Counter('monotonic_seq_violations');
const archiveLagMax = new Trend('archive_lag_max', false);
const archiveLagAvg = new Trend('archive_lag_avg', false);
const archivePendingRows = new Trend('archive_pending_rows', false);
const archiveQueueAgeSeconds = new Trend('archive_queue_age_seconds', false);
const archiveLagSamples = new Counter('archive_lag_samples');
const archiveLagProbeFailures = new Counter('archive_lag_probe_failures');
const archiveLagMissingSessions = new Counter('archive_lag_missing_sessions');
const beamMemoryAllocatedBytes = new Trend('beam_memory_allocated_bytes', false);
const beamMemoryProcessesBytes = new Trend('beam_memory_processes_bytes', false);
const beamMemoryEtsBytes = new Trend('beam_memory_ets_bytes', false);
const beamMemoryBinaryBytes = new Trend('beam_memory_binary_bytes', false);
const beamMemorySamples = new Counter('beam_memory_samples');
const beamMemoryMissing = new Counter('beam_memory_missing');

const maxVUs = Number(__ENV.MAX_VUS || 20);
const rampDuration = __ENV.RAMP_DURATION || '30s';
const steadyDuration = __ENV.STEADY_DURATION || '1m40s';
const rampDownDuration = __ENV.RAMP_DOWN_DURATION || '10s';
const lagPollEveryIters = Number(__ENV.LAG_POLL_EVERY_ITERS || 10);

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
  const sessionIds = [];

  for (let vuId = 1; vuId <= maxVUs; vuId++) {
    const id = lib.sessionId('hot-path', vuId, lib.config.runId);
    lib.ensureSession(id, {
      metadata: { bench: true, scenario: 'hot_path', vu: vuId, run_id: lib.config.runId },
    });
    sessions[vuId] = { sessionId: id, lastSeq: 0 };
    sessionIds.push(id);
  }

  return {
    runId: lib.config.runId,
    pipelineDepth,
    sessions,
    sessionIds,
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

  if (__VU === 1 && lagPollEveryIters > 0 && __ITER % lagPollEveryIters === 0) {
    const snapshot = lib.getArchiveLagSnapshot(data.sessionIds || []);

    if (snapshot && snapshot.ok) {
      archiveLagSamples.add(1);
      archiveLagMax.add(snapshot.lagMax);
      archiveLagAvg.add(snapshot.lagAvg);
      archivePendingRows.add(snapshot.pendingRows);
      archiveQueueAgeSeconds.add(snapshot.queueAgeSeconds);

      if (snapshot.beamMemoryMissing) {
        beamMemoryMissing.add(1);
      } else {
        beamMemorySamples.add(1);
        beamMemoryAllocatedBytes.add(snapshot.beamMemoryAllocatedBytes);
        beamMemoryProcessesBytes.add(snapshot.beamMemoryProcessesBytes);
        beamMemoryEtsBytes.add(snapshot.beamMemoryEtsBytes);
        beamMemoryBinaryBytes.add(snapshot.beamMemoryBinaryBytes);
      }

      if (snapshot.missingSessions > 0) {
        archiveLagMissingSessions.add(snapshot.missingSessions);
      }
    } else {
      archiveLagProbeFailures.add(1);
    }
  }

  session.lastSeq = lastSeq;
}
