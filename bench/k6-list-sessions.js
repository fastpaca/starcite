/**
 * Starcite k6: Session List Benchmark
 *
 * Seeds a large session catalog, then measures GET /v1/sessions pagination
 * latency against the configured archive adapter.
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { config } from './k6-lib.js';

const totalSessions = parsePositiveInt(__ENV.BENCH_TOTAL_SESSIONS, 5000);
const listLimit = parsePositiveInt(__ENV.BENCH_LIST_LIMIT, 100);
const batchSize = parsePositiveInt(__ENV.BENCH_SEED_BATCH_SIZE, 25);
const maxCursorPages = parsePositiveInt(__ENV.BENCH_LIST_CURSOR_PAGES, 20);
const readinessTimeoutSeconds = parsePositiveInt(__ENV.BENCH_READY_TIMEOUT_SECONDS, 120);
const skipSeed = (__ENV.BENCH_SKIP_SEED || '').trim().toLowerCase() === 'true';

const listDuration = new Trend('list_duration', true);
const seedDuration = new Trend('seed_duration', true);
const listErrors = new Counter('list_errors');

export const options = {
  vus: parsePositiveInt(__ENV.BENCH_LIST_VUS, 12),
  duration: __ENV.BENCH_LIST_DURATION || '30s',
  setupTimeout: __ENV.BENCH_SETUP_TIMEOUT || '10m',
  thresholds: {
    list_errors: ['count==0'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  waitForApiReady(readinessTimeoutSeconds);

  const width = String(totalSessions).length;
  const ids = Array.from({ length: totalSessions }, (_unused, index) =>
    sessionIdFor(index + 1, width)
  );

  if (!skipSeed) {
    const startedAt = Date.now();

    for (let offset = 0; offset < ids.length; offset += batchSize) {
      const chunk = ids.slice(offset, offset + batchSize);
      const responses = http.batch(
        chunk.map((id) => [
          'POST',
          `${config.apiUrl}/sessions`,
          JSON.stringify({ id }),
          { headers: { 'Content-Type': 'application/json' }, timeout: '30s' },
        ])
      );

      responses.forEach((res, index) => {
        if (res.status !== 201 && res.status !== 409) {
          throw new Error(
            `Failed seeding session ${chunk[index]}: ${res.status} ${truncate(res.body)}`
          );
        }
      });
    }

    seedDuration.add(Date.now() - startedAt);
  }

  const pageCount = Math.max(1, Math.min(maxCursorPages, Math.ceil(totalSessions / listLimit)));
  const cursors = [null];

  for (let page = 1; page < pageCount; page++) {
    const cursorIndex = page * listLimit;

    if (cursorIndex < ids.length) {
      cursors.push(ids[cursorIndex - 1]);
    }
  }

  return {
    cursors,
    listLimit,
  };
}

export default function (data) {
  const cursor = data.cursors[Math.floor(Math.random() * data.cursors.length)];
  const query = cursor === null ? `limit=${data.listLimit}` : `limit=${data.listLimit}&cursor=${encodeURIComponent(cursor)}`;
  const res = http.get(`${config.apiUrl}/sessions?${query}`, { timeout: '30s' });

  listDuration.add(res.timings.duration);

  const body = safeParse(res.body);
  const ok =
    check(res, {
      'list status is 200': (response) => response.status === 200,
      'list returns sessions array': () => body !== null && Array.isArray(body.sessions),
      'list respects limit': () =>
        body !== null && Array.isArray(body.sessions) && body.sessions.length <= data.listLimit,
    }) &&
    body !== null &&
    Array.isArray(body.sessions);

  if (!ok) {
    listErrors.add(1);
  }
}

function sessionIdFor(index, width) {
  return `list-bench-${String(index).padStart(width, '0')}`;
}

function waitForApiReady(timeoutSeconds) {
  const deadline = Date.now() + timeoutSeconds * 1000;

  while (Date.now() < deadline) {
    const res = http.get(`${config.apiUrl}/sessions?limit=1`, { timeout: '5s' });

    if (res.status === 200) {
      return;
    }

    sleep(1);
  }

  throw new Error(`API did not become ready within ${timeoutSeconds}s`);
}

function parsePositiveInt(raw, fallbackValue) {
  const value = raw === undefined || raw === null || raw === '' ? fallbackValue : Number(raw);

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Expected positive integer, got ${raw}`);
  }

  return value;
}

function safeParse(body) {
  try {
    return JSON.parse(body);
  } catch (_error) {
    return null;
  }
}

function truncate(body) {
  if (typeof body !== 'string') {
    return '';
  }

  return body.length <= 240 ? body : `${body.slice(0, 237)}...`;
}
