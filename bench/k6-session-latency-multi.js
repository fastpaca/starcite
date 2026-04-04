import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
  buildBenchmarkEvent,
  estimateAppendRequestBytes,
  estimatePayloadBytes,
  eventCorpusSummary,
} from './k6-event-corpus.js';

const appendLatency = new Trend('append_latency', true);
const appendOk = new Counter('append_ok');
const appendErr = new Counter('append_err');
const append503 = new Counter('append_503');
const append409 = new Counter('append_409');
const appendRequestBytes = new Trend('append_request_bytes');
const eventPayloadBytes = new Trend('event_payload_bytes');

const defaultApi = (__ENV.API_URL || 'http://node1:4000/v1').replace(/\/$/, '');
const clusterNodes = parseNodes(__ENV.CLUSTER_NODES, defaultApi);
const benchmarkActor = (__ENV.BENCH_EVENT_ACTOR || __ENV.BENCH_ACTOR || '').trim();

const vuRuntime = {};

export const options = {
  setupTimeout: __ENV.SETUP_TIMEOUT || '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)'],
  scenarios: {
    append_latency_multi: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 10000),
      timeUnit: __ENV.TIME_UNIT || '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || 3000),
      maxVUs: Number(__ENV.MAX_VUS || 15000),
    },
  },
  thresholds: {
    checks: ['rate>0.95'],
  },
};

export function setup() {
  const sessionCount = Number(__ENV.SESSION_COUNT || 1024);
  const createRetries = Number(__ENV.CREATE_RETRIES || 50);
  const retrySleepMs = Number(__ENV.CREATE_RETRY_SLEEP_MS || 50);
  const runId = __ENV.RUN_ID || `${Date.now()}-${Math.floor(Math.random() * 1000000)}`;

  if (!Number.isInteger(sessionCount) || sessionCount < 1) {
    throw new Error(`SESSION_COUNT must be >= 1 (got ${sessionCount})`);
  }

  const sessions = [];

  console.log(
    `Event corpus: ${eventCorpusSummary.source} entries=${eventCorpusSummary.eventCount} payload_p50=${eventCorpusSummary.payloadP50}B payload_p95=${eventCorpusSummary.payloadP95}B append_body_p50=${eventCorpusSummary.appendBodyP50}B append_body_p95=${eventCorpusSummary.appendBodyP95}B`
  );

  for (let i = 1; i <= sessionCount; i++) {
    let created = false;
    let attemptedSessionId = null;

    for (let attempt = 1; attempt <= createRetries; attempt++) {
      const sessionId = `multi-${runId}-${i}-${attempt}`;
      attemptedSessionId = sessionId;
      const node = clusterNodes[(i + attempt) % clusterNodes.length];
      const res = http.post(
        `${node}/sessions`,
        JSON.stringify({
          id: sessionId,
          creator_principal: { tenant_id: 'bench', id: 'bench-user', type: 'user' },
          metadata: { bench: true, scenario: 'multi_session_contention', slot: i, run_id: runId },
        }),
        { headers: { 'Content-Type': 'application/json' }, timeout: '10s' }
      );

      if (res.status === 201 || res.status === 409) {
        created = true;
        sessions.push(sessionId);
        break;
      }

      sleep(retrySleepMs / 1000);
    }

    if (!created) {
      throw new Error(`failed to create session ${attemptedSessionId} after ${createRetries} attempts`);
    }
  }

  return { sessions, clusterNodes, runId };
}

export default function (data) {
  const runtime = runtimeForVu(data.sessions.length);
  const sessionIndex = ((__ITER + __VU - 1) % data.sessions.length);
  const sessionId = data.sessions[sessionIndex];
  const producerId = `bench-vu-${__VU}-s-${sessionIndex}`;
  const producerSeq = runtime.nextSeqBySession[sessionIndex];
  const targetNode = data.clusterNodes[(__ITER + __VU) % data.clusterNodes.length];

  const body = buildBenchmarkEvent({
    runId: data.runId,
    scenario: 'multi_session_contention',
    vuId: __VU,
    sessionId,
    sessionIndex,
    producerSlot: 0,
    producerId,
    producerSeq,
    actorOverride: benchmarkActor !== '' ? benchmarkActor : null,
    metadata: {
      bench: true,
      scenario: 'multi_session_contention',
      vu: __VU,
      session_slot: sessionIndex,
    },
  });

  eventPayloadBytes.add(estimatePayloadBytes(body.payload));
  appendRequestBytes.add(estimateAppendRequestBytes(body));

  const res = http.post(
    `${targetNode}/sessions/${sessionId}/append`,
    JSON.stringify(body),
    { headers: { 'Content-Type': 'application/json' }, timeout: __ENV.REQUEST_TIMEOUT || '20s' }
  );

  appendLatency.add(res.timings.duration);

  if (res.status === 503) append503.add(1);
  if (res.status === 409) append409.add(1);

  const ok = check(res, {
    'append status is 2xx': (r) => r.status >= 200 && r.status < 300,
  });

  if (ok) {
    appendOk.add(1);
    runtime.nextSeqBySession[sessionIndex] = producerSeq + 1;
  } else {
    appendErr.add(1);
  }
}

function runtimeForVu(sessionCount) {
  if (vuRuntime[__VU]) {
    return vuRuntime[__VU];
  }

  const runtime = {
    nextSeqBySession: Array.from({ length: sessionCount }, () => 1),
  };

  vuRuntime[__VU] = runtime;
  return runtime;
}

function parseNodes(rawNodes, fallbackNode) {
  if (!rawNodes || rawNodes.trim() === '') {
    return [fallbackNode];
  }

  const nodes = rawNodes
    .split(',')
    .map((node) => node.trim().replace(/\/$/, ''))
    .filter((node) => node !== '');

  if (nodes.length === 0) {
    return [fallbackNode];
  }

  return nodes;
}
