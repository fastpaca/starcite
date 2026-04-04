import http from 'k6/http';
import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
  buildBenchmarkEvent,
  estimateAppendRequestBytes,
  estimatePayloadBytes,
  eventCorpusSummary,
} from './k6-event-corpus.js';

const appendLatency = new Trend('append_latency', true);
const rywLatency = new Trend('ryw_latency', true);
const appendOk = new Counter('append_ok');
const appendErr = new Counter('append_err');
const rywOk = new Counter('ryw_ok');
const rywErr = new Counter('ryw_err');
const rywTimeout = new Counter('ryw_timeout');
const rywGap = new Counter('ryw_gap');
const wsErr = new Counter('ws_err');
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
    tail_ryw_latency: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: __ENV.TIME_UNIT || '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || 100),
      maxVUs: Number(__ENV.MAX_VUS || 1000),
    },
  },
  thresholds: {
    checks: ['rate>0.95'],
  },
};

export function setup() {
  const sessionCount = Number(__ENV.SESSION_COUNT || 1);
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
      const sessionId = `tail-ryw-${runId}-${i}-${attempt}`;
      attemptedSessionId = sessionId;
      const node = clusterNodes[(i + attempt) % clusterNodes.length];
      const res = http.post(
        `${node}/sessions`,
        JSON.stringify({
          id: sessionId,
          creator_principal: { tenant_id: 'bench', id: 'bench-user', type: 'user' },
          metadata: { bench: true, scenario: 'tail_ryw_latency', slot: i, run_id: runId },
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
  const sessionCount = data.sessions.length;
  const runtime = runtimeForVu(sessionCount);
  const sessionIndex = ((__ITER + __VU - 1) % sessionCount);
  const sessionId = data.sessions[sessionIndex];
  const appendNode = data.clusterNodes[(__ITER + __VU) % data.clusterNodes.length];
  const tailNode = data.clusterNodes[(__ITER + __VU + 1) % data.clusterNodes.length];
  const producerSeq = runtime.nextSeqBySession[sessionIndex];
  const producerId = `bench-vu-${__VU}-s-${sessionIndex}`;
  const cursor = runtime.cursorBySession[sessionIndex];
  const wsUrl = tailUrl(tailNode, sessionId, cursor);
  const wsTimeoutMs = Number(__ENV.WS_TIMEOUT_MS || 10000);

  const body = buildBenchmarkEvent({
    runId: data.runId,
    scenario: 'tail_ryw_latency',
    vuId: __VU,
    sessionId,
    sessionIndex,
    producerSlot: 0,
    producerId,
    producerSeq,
    actorOverride: benchmarkActor !== '' ? benchmarkActor : null,
    metadata: {
      bench: true,
      scenario: 'tail_ryw_latency',
      vu: __VU,
      session_slot: sessionIndex,
    },
  });

  eventPayloadBytes.add(estimatePayloadBytes(body.payload));
  appendRequestBytes.add(estimateAppendRequestBytes(body));

  const outcome = {
    appendAccepted: false,
    latestSeq: cursor,
    status: 'init',
  };

  let appendStartedAtMs = 0;
  let awaitedSeq = null;

  const res = ws.connect(wsUrl, { timeout: `${wsTimeoutMs}ms` }, function (socket) {
    socket.on('open', function () {
      appendStartedAtMs = Date.now();

      const appendRes = http.post(
        `${appendNode}/sessions/${sessionId}/append`,
        JSON.stringify(body),
        { headers: { 'Content-Type': 'application/json' }, timeout: __ENV.REQUEST_TIMEOUT || '20s' }
      );

      appendLatency.add(appendRes.timings.duration);

      if (appendRes.status >= 200 && appendRes.status < 300) {
        appendOk.add(1);
        outcome.appendAccepted = true;

        const payload = safeParse(appendRes.body);
        awaitedSeq = payload && payload.seq;
        outcome.latestSeq = maxSeq(outcome.latestSeq, payload);

        if (!Number.isInteger(awaitedSeq) || awaitedSeq < 1) {
          outcome.status = 'invalid_append_reply';
          socket.close();
        }
      } else {
        appendErr.add(1);
        outcome.status = `append_${appendRes.status}`;
        socket.close();
      }
    });

    socket.on('message', function (message) {
      const frame = parseFrame(message);

      if (frame.type === 'gap') {
        rywGap.add(1);
        outcome.status = 'gap';
        socket.close();
        return;
      }

      if (frame.maxSeq !== null) {
        outcome.latestSeq = Math.max(outcome.latestSeq, frame.maxSeq);
      }

      if (awaitedSeq !== null && frame.seqs.includes(awaitedSeq)) {
        rywLatency.add(Date.now() - appendStartedAtMs);
        rywOk.add(1);
        outcome.status = 'ok';
        socket.close();
      }
    });

    socket.on('error', function () {
      wsErr.add(1);
      if (outcome.status === 'init') {
        outcome.status = 'ws_error';
      }
      socket.close();
    });

    socket.setTimeout(function () {
      if (outcome.appendAccepted) {
        rywTimeout.add(1);
        outcome.status = 'timeout';
      } else if (outcome.status === 'init') {
        outcome.status = 'ws_timeout';
      }

      socket.close();
    }, wsTimeoutMs);
  });

  check(res, {
    'tail websocket upgraded': (r) => r && r.status === 101,
  });

  if (outcome.appendAccepted) {
    runtime.nextSeqBySession[sessionIndex] = producerSeq + 1;
  }

  runtime.cursorBySession[sessionIndex] = Math.max(runtime.cursorBySession[sessionIndex], outcome.latestSeq);

  if (outcome.status !== 'ok' && outcome.status !== 'timeout' && outcome.status !== 'gap' && !outcome.status.startsWith('append_')) {
    rywErr.add(1);
  }
}

function runtimeForVu(sessionCount) {
  if (vuRuntime[__VU]) {
    return vuRuntime[__VU];
  }

  const runtime = {
    nextSeqBySession: Array.from({ length: sessionCount }, () => 1),
    cursorBySession: Array.from({ length: sessionCount }, () => 0),
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

  return nodes.length > 0 ? nodes : [fallbackNode];
}

function tailUrl(node, sessionId, cursor) {
  const base = node.replace(/^http/, 'ws').replace(/\/v1$/, '');
  return `${base}/v1/sessions/${sessionId}/tail?cursor=${cursor}&batch_size=1`;
}

function parseFrame(message) {
  const parsed = safeParse(message);

  if (Array.isArray(parsed)) {
    const seqs = parsed
      .map((event) => event && event.seq)
      .filter((seq) => Number.isInteger(seq) && seq > 0);

    return {
      type: 'events',
      seqs,
      maxSeq: seqs.length > 0 ? Math.max(...seqs) : null,
    };
  }

  if (parsed && parsed.type === 'gap') {
    return { type: 'gap', seqs: [], maxSeq: null };
  }

  if (parsed && Number.isInteger(parsed.seq) && parsed.seq > 0) {
    return { type: 'events', seqs: [parsed.seq], maxSeq: parsed.seq };
  }

  return { type: 'unknown', seqs: [], maxSeq: null };
}

function maxSeq(current, payload) {
  if (payload && payload.cursor && Number.isInteger(payload.cursor.seq)) {
    return Math.max(current, payload.cursor.seq);
  }

  if (payload && Number.isInteger(payload.seq)) {
    return Math.max(current, payload.seq);
  }

  return current;
}

function safeParse(body) {
  try {
    return JSON.parse(body);
  } catch (_err) {
    return null;
  }
}
