import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const appendLatency = new Trend('append_latency', true);
const appendOk = new Counter('append_ok');
const appendErr = new Counter('append_err');

const apiUrl = (__ENV.API_URL || 'http://node1:4000/v1').replace(/\/$/, '');
const sessionId = __ENV.SESSION_ID;

if (!sessionId) {
  throw new Error('SESSION_ID is required');
}

export const options = {
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

  const body = {
    type: 'content',
    payload: { text: `bench-${__VU}-${__ITER}` },
    producer_id: producerId,
    producer_seq: producerSeq,
    source: 'benchmark',
    metadata: { bench: true, vu: __VU },
  };

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
