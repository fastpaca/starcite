/**
 * Starcite k6 Benchmark Library
 *
 * Helpers for exercising Starcite session primitives during load tests.
 */

import http from 'k6/http';
import { check } from 'k6';

const clusterNodes = __ENV.CLUSTER_NODES
  ? __ENV.CLUSTER_NODES.split(',').map((url) => url.trim().replace(/\/$/, ''))
  : [(__ENV.API_URL || 'http://localhost:4000/v1').replace(/\/$/, '')];

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

function rootUrl(url) {
  return url.replace(/\/v1$/, '');
}

function buildUrl(path, params = {}, useLoadBalancer = true) {
  const query = Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join('&');

  const baseUrl = useLoadBalancer && clusterNodes.length > 1 ? getNextNode() : config.apiUrl;
  return query ? `${baseUrl}${path}?${query}` : `${baseUrl}${path}`;
}

function parsePrometheusLabels(raw) {
  if (!raw) return {};

  const labels = {};
  const labelPattern = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;
  let match;

  while ((match = labelPattern.exec(raw)) !== null) {
    labels[match[1]] = match[2].replace(/\\"/g, '"');
  }

  return labels;
}

function parseMetricSamples(body, metricName) {
  const samples = [];
  const lines = body.split('\n');

  for (const line of lines) {
    if (!line || line.startsWith('#') || !line.startsWith(metricName)) continue;

    const withoutName = line.slice(metricName.length);
    let labels = {};
    let valuePart = withoutName;

    if (withoutName.startsWith('{')) {
      const closingIdx = withoutName.indexOf('}');
      if (closingIdx < 0) continue;
      labels = parsePrometheusLabels(withoutName.slice(1, closingIdx));
      valuePart = withoutName.slice(closingIdx + 1);
    }

    const value = Number.parseFloat(valuePart.trim().split(/\s+/)[0]);
    if (Number.isNaN(value)) continue;

    samples.push({ labels, value });
  }

  return samples;
}

function firstMetricValue(samples) {
  if (!samples || samples.length === 0) return null;
  return samples[0].value;
}

/**
 * Poll archive lag and queue pressure from /metrics.
 *
 * Returns per-session lag summary for the provided session IDs and selected
 * queue gauges exposed by PromEx, plus BEAM memory gauges.
 */
export function getArchiveLagSnapshot(sessionIds, opts = {}) {
  if (!sessionIds || sessionIds.length === 0) {
    return null;
  }

  const useLoadBalancer = opts.useLoadBalancer || false;
  const node =
    useLoadBalancer && clusterNodes.length > 1 ? getNextNode() : (opts.node || clusterNodes[0]);
  const metricsUrl = `${rootUrl(node)}/metrics`;
  const res = http.get(metricsUrl, { timeout: opts.timeout || '5s' });

  if (res.status !== 200) {
    return {
      ok: false,
      status: res.status,
      metricsUrl,
      error: `metrics_status_${res.status}`,
    };
  }

  const lagSamples = parseMetricSamples(res.body, 'starcite_archive_lag');
  const pendingSamples = parseMetricSamples(res.body, 'starcite_archive_pending_rows');
  const queueAgeSamples = parseMetricSamples(res.body, 'starcite_archive_oldest_age_seconds');
  const beamAllocatedSamples = parseMetricSamples(
    res.body,
    'starcite_prom_ex_beam_memory_allocated_bytes',
  );
  const beamProcessesSamples = parseMetricSamples(
    res.body,
    'starcite_prom_ex_beam_memory_processes_total_bytes',
  );
  const beamEtsSamples = parseMetricSamples(res.body, 'starcite_prom_ex_beam_memory_ets_total_bytes');
  const beamBinarySamples = parseMetricSamples(
    res.body,
    'starcite_prom_ex_beam_memory_binary_total_bytes',
  );

  const lagBySession = {};
  for (const sample of lagSamples) {
    if (sample.labels && sample.labels.session_id) {
      lagBySession[sample.labels.session_id] = sample.value;
    }
  }

  const lagValues = [];
  let missing = 0;

  for (const sessionId of sessionIds) {
    if (Object.prototype.hasOwnProperty.call(lagBySession, sessionId)) {
      lagValues.push(lagBySession[sessionId]);
    } else {
      missing++;
    }
  }

  const lagSum = lagValues.reduce((acc, value) => acc + value, 0);
  const lagMax = lagValues.length > 0 ? Math.max(...lagValues) : 0;
  const lagAvg = lagValues.length > 0 ? lagSum / lagValues.length : 0;
  const pendingRows = firstMetricValue(pendingSamples) ?? 0;
  const queueAgeSeconds = firstMetricValue(queueAgeSamples) ?? 0;
  const beamMemoryAllocatedBytes = firstMetricValue(beamAllocatedSamples);
  const beamMemoryProcessesBytes = firstMetricValue(beamProcessesSamples);
  const beamMemoryEtsBytes = firstMetricValue(beamEtsSamples);
  const beamMemoryBinaryBytes = firstMetricValue(beamBinarySamples);
  const beamMemoryMissing =
    beamMemoryAllocatedBytes === null ||
    beamMemoryProcessesBytes === null ||
    beamMemoryEtsBytes === null ||
    beamMemoryBinaryBytes === null;

  return {
    ok: true,
    status: 200,
    metricsUrl,
    sampledSessions: lagValues.length,
    missingSessions: missing,
    lagAvg,
    lagMax,
    lagSum,
    pendingRows,
    queueAgeSeconds,
    beamMemoryAllocatedBytes,
    beamMemoryProcessesBytes,
    beamMemoryEtsBytes,
    beamMemoryBinaryBytes,
    beamMemoryMissing,
  };
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

    http.get('https://httpbin.test.k6.io/delay/1');
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
  const payload = {
    type: event.type,
    payload: event.payload,
    actor: event.actor,
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
