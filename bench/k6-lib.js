/**
 * Starcite k6 Benchmark Library
 *
 * Helpers for exercising Starcite session primitives during load tests.
 */

import http from 'k6/http';
import { check, sleep } from 'k6';

function parseClusterNodes(value) {
  if (!value) return [];

  return value
    .split(',')
    .map((url) => url.trim().replace(/\/$/, ''))
    .filter((url) => url !== '');
}

const clusterNodes = parseClusterNodes(__ENV.CLUSTER_NODES);
if (clusterNodes.length === 0) {
  clusterNodes.push((__ENV.API_URL || 'http://localhost:4000/v1').replace(/\/$/, ''));
}

let nodeIndex = 0;

function getNextNode() {
  const node = clusterNodes[nodeIndex % clusterNodes.length];
  nodeIndex++;
  return node;
}

export const config = {
  apiUrl: clusterNodes[0],
  getNextNode,
  clusterNodes,
  runId: __ENV.RUN_ID || `${Date.now()}-${Math.floor(Math.random() * 10000)}`,
};

export const jsonHeaders = {
  headers: {
    'Content-Type': 'application/json',
  },
};

function buildUrl(path, params = {}, useLoadBalancer = true) {
  const query = Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join('&');

  const baseUrl = useLoadBalancer && clusterNodes.length > 1 ? getNextNode() : config.apiUrl;
  return query ? `${baseUrl}${path}?${query}` : `${baseUrl}${path}`;
}

export function waitForClusterReady(timeoutSeconds = 60) {
  const startTime = Date.now();
  const timeoutMs = timeoutSeconds * 1000;

  while (Date.now() - startTime < timeoutMs) {
    let allReady = true;

    for (const node of clusterNodes) {
      const res = http.get(`${node.replace('/v1', '')}/health/ready`, { timeout: '5s' });
      if (res.status !== 200) {
        allReady = false;
        break;
      }
    }

    if (allReady) {
      return true;
    }

    sleep(1);
  }

  throw new Error(`Cluster did not become ready within ${timeoutSeconds}s`);
}

export function sessionId(prefix, vuId, extra = '') {
  const suffix = extra ? `-${extra}` : '';
  return `${prefix}-${vuId}${suffix}`;
}

/**
 * Create a session using POST /v1/sessions.
 */
export function ensureSession(id, overrides = {}) {
  const payload = {
    id,
    creator_principal: overrides.creator_principal || {
      tenant_id: __ENV.BENCH_TENANT_ID || 'bench',
      id: __ENV.BENCH_PRINCIPAL_ID || 'bench-user',
      type: __ENV.BENCH_PRINCIPAL_TYPE || 'user',
    },
  };

  if (overrides.title) payload.title = overrides.title;
  if (overrides.metadata) payload.metadata = overrides.metadata;

  const res = http.post(buildUrl('/sessions'), JSON.stringify(payload), jsonHeaders);

  check(res, {
    [`ensure session ${id}`]: (r) => r.status === 201 || r.status === 409,
  });

  if (res.status >= 400 && res.status !== 409) {
    throw new Error(`Failed to ensure session ${id}: ${res.status} ${res.body}`);
  }

  return safeParse(res.body);
}

/**
 * Append one event using POST /v1/sessions/:id/append.
 */
export function appendEvent(id, event, opts = {}) {
  if (event.producer_id === undefined || event.producer_seq === undefined) {
    throw new Error('appendEvent requires producer_id and producer_seq');
  }

  const payload = {
    type: event.type,
    payload: event.payload,
    actor: event.actor,
    producer_id: event.producer_id,
    producer_seq: event.producer_seq,
  };

  if (event.source !== undefined) payload.source = event.source;
  if (event.metadata !== undefined) payload.metadata = event.metadata;
  if (event.refs !== undefined) payload.refs = event.refs;
  if (event.idempotency_key !== undefined) payload.idempotency_key = event.idempotency_key;
  if (opts.expectedSeq !== undefined && opts.expectedSeq !== null) payload.expected_seq = opts.expectedSeq;

  const res = http.post(buildUrl(`/sessions/${id}/append`), JSON.stringify(payload), jsonHeaders);

  return {
    res,
    json: safeParse(res.body),
  };
}

function safeParse(body) {
  try {
    return JSON.parse(body);
  } catch (_err) {
    return null;
  }
}
