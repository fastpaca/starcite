import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
  buildBenchmarkEvent,
  estimateAppendRequestBytes,
  estimatePayloadBytes,
} from './k6-event-corpus.js';

const appendLatency = new Trend('append_latency', true);
const appendOk = new Counter('append_ok');
const appendErr = new Counter('append_err');
const appendRequestBytes = new Trend('append_request_bytes');
const eventPayloadBytes = new Trend('event_payload_bytes');

const apiUrl = (__ENV.API_URL || 'http://node1:4000/v1').replace(/\/$/, '');
const sessionId = __ENV.SESSION_ID;
const benchmarkActor = (__ENV.BENCH_EVENT_ACTOR || __ENV.BENCH_ACTOR || '').trim();
const runId = __ENV.RUN_ID || 'session-latency';

if (!sessionId) {
  throw new Error('SESSION_ID is required');
}

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)'],
  scenarios: {
    append_latency: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 300),
      timeUnit: __ENV.TIME_UNIT || '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || 50),
      maxVUs: Number(__ENV.MAX_VUS || 300),
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const producerId = `bench-vu-${__VU}`;
  const producerSeq = __ITER + 1;
  const body = buildBenchmarkEvent({
    runId,
    scenario: 'session_latency',
    vuId: __VU,
    sessionId,
    sessionIndex: 1,
    producerSlot: 0,
    producerId,
    producerSeq,
    actorOverride: benchmarkActor !== '' ? benchmarkActor : null,
    metadata: { bench: true, scenario: 'session_latency', vu: __VU },
  });

  eventPayloadBytes.add(estimatePayloadBytes(body.payload));
  appendRequestBytes.add(estimateAppendRequestBytes(body));

  const res = http.post(
    `${apiUrl}/sessions/${sessionId}/append`,
    JSON.stringify(body),
    { headers: { 'Content-Type': 'application/json' } }
  );

  appendLatency.add(res.timings.duration);

  const ok = check(res, {
    'append status is 2xx': (r) => r.status >= 200 && r.status < 300,
  });

  if (ok) {
    appendOk.add(1);
  } else {
    appendErr.add(1);
  }
}
