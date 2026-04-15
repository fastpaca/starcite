import http from 'k6/http';
import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const appendLatency = new Trend('append_latency', true);
const rywLatency = new Trend('ryw_latency', true);
const appendOk = new Counter('append_ok');
const appendErr = new Counter('append_err');
const rywOk = new Counter('ryw_ok');
const rywErr = new Counter('ryw_err');
const rywTimeout = new Counter('ryw_timeout');
const rywGap = new Counter('ryw_gap');
const wsErr = new Counter('ws_err');

const defaultApi = (__ENV.API_URL || 'http://node1:4000/v1').replace(/\/$/, '');
const clusterNodes = parseNodes(__ENV.CLUSTER_NODES, defaultApi);
const vuRuntime = {};

export const options = {
  setupTimeout: __ENV.SETUP_TIMEOUT || '300s',
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

  return { sessions, clusterNodes };
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
  const cursorEpoch = runtime.cursorEpochBySession[sessionIndex];
  const wsUrl = socketUrl(tailNode);
  const tailTopic = `tail:${sessionId}`;
  const joinRef = `${__VU}-${__ITER}`;
  const wsTimeoutMs = Number(__ENV.WS_TIMEOUT_MS || 10000);

  const body = {
    type: 'content',
    payload: { text: `ryw-vu-${__VU}-s-${sessionIndex}-pseq-${producerSeq}` },
    producer_id: producerId,
    producer_seq: producerSeq,
    source: 'benchmark',
    metadata: { bench: true, scenario: 'tail_ryw_latency', vu: __VU, session_slot: sessionIndex },
  };

  const outcome = {
    appendAccepted: false,
    latestCursor: { seq: cursor, epoch: cursorEpoch },
    status: 'init',
  };

  let appendStartedAtMs = 0;
  let awaitedSeq = null;
  let joinAccepted = false;

  const res = ws.connect(wsUrl, { timeout: `${wsTimeoutMs}ms` }, function (socket) {
    socket.on('open', function () {
      const joinPayload = { cursor, batch_size: 1 };

      if (Number.isInteger(cursorEpoch) && cursorEpoch >= 0) {
        joinPayload.cursor_epoch = cursorEpoch;
      }

      socket.send(JSON.stringify([joinRef, joinRef, tailTopic, 'phx_join', joinPayload]));
    });

    socket.on('message', function (message) {
      const frame = parseFrame(message);

      if (frame.type === 'join_ok') {
        if (joinAccepted) {
          return;
        }

        joinAccepted = true;
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
          outcome.latestCursor = mergeCursor(outcome.latestCursor, cursorFromAppendReply(payload));

          if (!Number.isInteger(awaitedSeq) || awaitedSeq < 1) {
            outcome.status = 'invalid_append_reply';
            socket.close();
          }
        } else {
          appendErr.add(1);
          outcome.status = `append_${appendRes.status}`;
          socket.close();
        }

        return;
      }

      if (frame.type === 'join_error') {
        outcome.status = frame.reason || 'join_error';
        socket.close();
        return;
      }

      if (frame.type === 'gap') {
        rywGap.add(1);
        outcome.status = 'gap';
        socket.close();
        return;
      }

      if (frame.type === 'token_expired' || frame.type === 'node_draining') {
        outcome.status = frame.type;
        socket.close();
        return;
      }

      if (frame.cursor !== null) {
        outcome.latestCursor = mergeCursor(outcome.latestCursor, frame.cursor);
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

  runtime.cursorBySession[sessionIndex] = outcome.latestCursor.seq;
  runtime.cursorEpochBySession[sessionIndex] = outcome.latestCursor.epoch;

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
    cursorEpochBySession: Array.from({ length: sessionCount }, () => null),
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

function socketUrl(node) {
  const base = node.replace(/^http/, 'ws').replace(/\/v1$/, '');
  return `${base}/v1/socket/websocket?tenant_id=bench&vsn=2.0.0`;
}

function parseFrame(message) {
  const parsed = safeParse(message);

  if (Array.isArray(parsed) && parsed.length === 5) {
    const [, , , event, payload] = parsed;

    if (event === 'phx_reply') {
      if (payload && payload.status === 'ok') {
        return { type: 'join_ok', seqs: [], cursor: null };
      }

      if (payload && payload.status === 'error') {
        return {
          type: 'join_error',
          reason: payload.response && payload.response.reason ? payload.response.reason : 'join_error',
          seqs: [],
          cursor: null,
        };
      }
    }

    if (event === 'events') {
      const events = payload && Array.isArray(payload.events) ? payload.events : [];
      const seqs = events
        .map((item) => item && item.seq)
        .filter((seq) => Number.isInteger(seq) && seq > 0);
      const latestEvent = events
        .filter((item) => item && Number.isInteger(item.seq) && item.seq > 0)
        .sort((left, right) => left.seq - right.seq)
        .pop();

      return {
        type: 'events',
        seqs,
        cursor: latestEvent
          ? {
              seq: latestEvent.seq,
              epoch:
                Number.isInteger(latestEvent.epoch) && latestEvent.epoch >= 0
                  ? latestEvent.epoch
                  : null,
            }
          : null,
      };
    }

    if (event === 'gap') {
      return {
        type: 'gap',
        seqs: [],
        cursor: cursorFromGapPayload(payload),
      };
    }

    if (event === 'token_expired' || event === 'node_draining') {
      return { type: event, seqs: [], cursor: null };
    }
  }

  if (parsed && parsed.type === 'gap') {
    return { type: 'gap', seqs: [], cursor: cursorFromGapPayload(parsed) };
  }

  if (parsed && Number.isInteger(parsed.seq) && parsed.seq > 0) {
    return {
      type: 'events',
      seqs: [parsed.seq],
      cursor: {
        seq: parsed.seq,
        epoch: Number.isInteger(parsed.epoch) && parsed.epoch >= 0 ? parsed.epoch : null,
      },
    };
  }

  return { type: 'unknown', seqs: [], cursor: null };
}

function cursorFromAppendReply(payload) {
  if (!payload || !Number.isInteger(payload.cursor) || payload.cursor < 0) {
    return null;
  }

  const epoch = Number.isInteger(payload.cursor_epoch)
    ? payload.cursor_epoch
    : Number.isInteger(payload.epoch)
      ? payload.epoch
      : null;

  return { seq: payload.cursor, epoch };
}

function cursorFromGapPayload(payload) {
  if (!payload || !Number.isInteger(payload.next_cursor) || payload.next_cursor < 0) {
    return null;
  }

  return {
    seq: payload.next_cursor,
    epoch:
      Number.isInteger(payload.next_cursor_epoch) && payload.next_cursor_epoch >= 0
        ? payload.next_cursor_epoch
        : null,
  };
}

function mergeCursor(current, candidate) {
  if (!candidate) {
    return current;
  }

  if (!current || candidate.seq > current.seq) {
    return candidate;
  }

  if (candidate.seq === current.seq && current.epoch === null && candidate.epoch !== null) {
    return candidate;
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
