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
const producersPerVu = Number(__ENV.PRODUCERS_PER_VU || 1);
const sessionCount = Number(__ENV.SESSION_COUNT || maxVUs);
const executor = (__ENV.EXECUTOR || 'ramping-vus').trim().toLowerCase();
const rate = Number(__ENV.RATE || 1000);
const duration = __ENV.DURATION || '30s';
const timeUnit = __ENV.TIME_UNIT || '1s';
const preAllocatedVUs = Number(__ENV.PRE_ALLOCATED_VUS || maxVUs);
const skipClusterReadyCheck = (__ENV.SKIP_CLUSTER_READY_CHECK || 'false').trim() === 'true';
const vuRuntime = {};

const throughputScenario =
  executor === 'constant-arrival-rate'
    ? {
        executor: 'constant-arrival-rate',
        rate,
        timeUnit,
        duration,
        preAllocatedVUs,
        maxVUs,
      }
    : {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: rampDuration, target: maxVUs },
          { duration: steadyDuration, target: maxVUs },
          { duration: rampDownDuration, target: 0 },
        ],
        gracefulRampDown: '5s',
      };

export const options = {
  setupTimeout: __ENV.SETUP_TIMEOUT || '180s',
  teardownTimeout: '60s',
  scenarios: {
    throughput: throughputScenario,
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

  if (!Number.isInteger(producersPerVu) || producersPerVu < 1) {
    throw new Error(`PRODUCERS_PER_VU must be >= 1 (got ${producersPerVu})`);
  }

  if (!Number.isInteger(maxVUs) || maxVUs < 1) {
    throw new Error(`MAX_VUS must be >= 1 (got ${maxVUs})`);
  }

  if (!Number.isInteger(sessionCount) || sessionCount < 1) {
    throw new Error(`SESSION_COUNT must be >= 1 (got ${sessionCount})`);
  }

  if (executor === 'constant-arrival-rate') {
    if (!Number.isInteger(rate) || rate < 1) {
      throw new Error(`RATE must be >= 1 for constant-arrival-rate (got ${rate})`);
    }

    if (!Number.isInteger(preAllocatedVUs) || preAllocatedVUs < 1) {
      throw new Error(
        `PRE_ALLOCATED_VUS must be >= 1 for constant-arrival-rate (got ${preAllocatedVUs})`
      );
    }
  }

  console.log('Setting up append throughput benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`Executor: ${executor}`);
  console.log(`Max VUs: ${maxVUs}`);
  console.log(`Session count: ${sessionCount}`);

  if (executor === 'constant-arrival-rate') {
    console.log(`Rate: ${rate}/${timeUnit}`);
    console.log(`Duration: ${duration}`);
    console.log(`Pre-allocated VUs: ${preAllocatedVUs}`);
  }

  console.log(`Producers per VU: ${producersPerVu}`);
  console.log(`Pipeline depth: ${pipelineDepth}`);

  if (skipClusterReadyCheck) {
    console.log('Skipping cluster ready check');
  } else {
    lib.waitForClusterReady(90);
  }

  const sessions = {};
  const sessionPrefix = `hot-path-${lib.config.runId}`;

  for (let index = 1; index <= sessionCount; index++) {
    const id = lib.sessionId(sessionPrefix, index);
    lib.ensureSession(id, {
      metadata: { bench: true, scenario: 'hot_path', slot: index, run_id: lib.config.runId },
    });

    sessions[index] = {
      sessionId: id,
      lastSeq: 0,
    };
  }

  return {
    runId: lib.config.runId,
    pipelineDepth,
    sessionCount,
    sessions,
  };
}

export default function (data) {
  const vuId = __VU;
  const session = vuStateFor(data, vuId);

  if (!session) return;

  const { sessionId } = session;
  const pipelineDepth = data.pipelineDepth;
  let lastSeq = session.lastSeq;

  for (let i = 0; i < pipelineDepth; i++) {
    const producerSlot = i % session.producerIds.length;
    const producerId = session.producerIds[producerSlot];
    const producerSeq = readProducerSeq(session, producerSlot);
    const text = `starcite hot-path run=${data.runId} vu=${vuId} p=${producerSlot} seq=${producerSeq}`;

    const { res, json } = lib.appendEvent(sessionId, {
      type: 'content',
      payload: { text },
      actor: `agent:vu:${vuId}`,
      producer_id: producerId,
      producer_seq: producerSeq,
      source: 'benchmark',
      metadata: {
        bench: true,
        scenario: 'hot_path',
        vu: vuId,
        producer_slot: producerSlot,
        producer_seq: producerSeq,
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

      if (json.deduped !== true) {
        advanceProducerSeq(session, producerSlot);
      }

      lastSeq = json.seq;
    }
  }

  session.lastSeq = lastSeq;
}

function buildProducerIds(vuId, count) {
  return Array.from({ length: count }, (_, idx) => `writer:vu:${vuId}:p:${idx + 1}`);
}

function vuStateFor(data, vuId) {
  if (vuRuntime[vuId]) {
    return vuRuntime[vuId];
  }

  const sessionSlot = ((vuId - 1) % data.sessionCount) + 1;
  const session = data.sessions[sessionSlot];

  if (!session) {
    return null;
  }

  const runtime = {
    sessionId: session.sessionId,
    lastSeq: session.lastSeq || 0,
    producerIds: buildProducerIds(vuId, producersPerVu),
    nextProducerSeq: createSequencer(Array.from({ length: producersPerVu }, () => 1)),
  };

  vuRuntime[vuId] = runtime;
  return runtime;
}

function createSequencer(initialValues) {
  const supportsAtomics =
    typeof SharedArrayBuffer !== 'undefined' &&
    typeof Int32Array !== 'undefined' &&
    typeof Atomics !== 'undefined';

  if (supportsAtomics) {
    const buffer = new SharedArrayBuffer(initialValues.length * Int32Array.BYTES_PER_ELEMENT);
    const values = new Int32Array(buffer);

    for (let i = 0; i < initialValues.length; i++) {
      values[i] = Number(initialValues[i] || 1);
    }

    return { values, atomics: true };
  }

  return { values: initialValues.slice(), atomics: false };
}

function readProducerSeq(session, slot) {
  if (session.nextProducerSeq.atomics) {
    return Atomics.load(session.nextProducerSeq.values, slot);
  }

  return session.nextProducerSeq.values[slot];
}

function advanceProducerSeq(session, slot) {
  if (session.nextProducerSeq.atomics) {
    Atomics.add(session.nextProducerSeq.values, slot, 1);
    return;
  }

  session.nextProducerSeq.values[slot] += 1;
}
