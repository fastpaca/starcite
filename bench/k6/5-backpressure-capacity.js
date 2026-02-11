/**
 * Starcite Scenario 5: Backpressure Capacity Sweep
 *
 * Ramps append QPS in fixed stages and reports:
 * - First QPS stage where 429/event_store_backpressure appears
 * - Archive lag and pending-row behavior under load
 * - Event-store memory usage as observed from /metrics
 */

import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Gauge, Rate, Trend } from 'k6/metrics';
import * as lib from './lib.js';

const appendAttempts = new Counter('append_attempts');
const appendBackpressure = new Counter('append_backpressure_total');
const appendBackpressureRate = new Rate('append_backpressure_rate');
const appendLatency = new Trend('append_latency', true);

const metricsProbeOk = new Rate('metrics_probe_ok');
const eventStoreMemoryBytesObserved = new Gauge('event_store_memory_bytes_observed');
const eventStoreBackpressureTotalObserved = new Gauge('event_store_backpressure_total_observed');
const archiveLagObserved = new Gauge('archive_lag_observed');
const archivePendingRowsObserved = new Gauge('archive_pending_rows_observed');

const startQps = Number(__ENV.START_QPS || 200);
const stepQps = Number(__ENV.STEP_QPS || 200);
const maxQps = Number(__ENV.MAX_QPS || 4000);
const stageDuration = __ENV.STAGE_DURATION || '20s';
const preAllocatedVUs = Number(__ENV.PREALLOCATED_VUS || 200);
const maxVUs = Number(__ENV.MAX_VUS || 1200);
const sessionCount = Number(__ENV.SESSION_COUNT || 400);
const probeIntervalSec = Number(__ENV.PROBE_INTERVAL_SEC || 2);
const eventStoreMaxSizeLabel = __ENV.EVENT_STORE_MAX_SIZE_LABEL || '2GB';

const stageRates = buildStageRates(startQps, stepQps, maxQps);
const stages = stageRates.map((rate) => ({ duration: stageDuration, target: rate }));
const probeDuration = `${Math.max(1, stageRates.length * durationToSeconds(stageDuration))}s`;

export const options = {
  setupTimeout: '120s',
  teardownTimeout: '120s',
  scenarios: {
    load: {
      executor: 'ramping-arrival-rate',
      startRate: stageRates[0],
      timeUnit: '1s',
      preAllocatedVUs,
      maxVUs,
      stages,
      exec: 'load',
      gracefulStop: '0s',
    },
    probe: {
      executor: 'constant-vus',
      vus: 1,
      duration: probeDuration,
      exec: 'probeMetrics',
      gracefulStop: '0s',
    },
  },
  thresholds: {
    metrics_probe_ok: [{ threshold: 'rate>0.99', abortOnFail: false }],
  },
};

export function setup() {
  console.log('Setting up backpressure-capacity benchmark...');
  console.log(`Run ID: ${lib.config.runId}`);
  console.log(`Event-store max-size label: ${eventStoreMaxSizeLabel}`);
  console.log(`QPS stages: ${stageRates.join(', ')}`);

  lib.waitForClusterReady(120);

  const sessions = [];

  for (let i = 0; i < sessionCount; i++) {
    const id = lib.sessionId('bp-capacity', i + 1);
    lib.ensureSession(id, {
      metadata: {
        bench: true,
        scenario: 'backpressure_capacity',
        slot: i + 1,
        run_id: lib.config.runId,
      },
    });
    sessions.push(id);
  }

  return {
    runId: lib.config.runId,
    sessions,
    stageRates,
    metricsUrl: `${lib.config.apiUrl.replace('/v1', '')}/metrics`,
    eventStoreMaxSizeLabel,
  };
}

export function load(data) {
  const iteration = exec.scenario.iterationInTest;
  const sessionId = data.sessions[iteration % data.sessions.length];
  const stage = stageForProgress(exec.scenario.progress, data.stageRates);
  const tags = { target_qps: String(stage.targetQps), stage: String(stage.index + 1) };

  const { res, json } = lib.appendEvent(sessionId, {
    type: 'content',
    payload: {
      text: `bp-capacity run=${data.runId} iter=${iteration} stage=${stage.targetQps}`,
    },
    actor: `agent:bp:${__VU}`,
    source: 'benchmark',
    metadata: {
      bench: true,
      scenario: 'backpressure_capacity',
      stage_qps: stage.targetQps,
      iter: iteration,
    },
  });

  appendAttempts.add(1, tags);
  appendLatency.add(res.timings.duration, tags);

  const isBackpressure =
    res.status === 429 && json && typeof json.error === 'string' && json.error === 'event_store_backpressure';

  appendBackpressure.add(isBackpressure ? 1 : 0, tags);
  appendBackpressureRate.add(isBackpressure, tags);
}

export function probeMetrics(data) {
  const res = http.get(data.metricsUrl, { timeout: '5s' });

  const ok = res.status === 200 && typeof res.body === 'string' && res.body.length > 0;
  metricsProbeOk.add(ok);

  if (!ok) {
    sleep(probeIntervalSec);
    return;
  }

  const snapshot = parsePrometheusSnapshot(res.body);

  eventStoreMemoryBytesObserved.add(snapshot.eventStoreMemoryBytes);
  eventStoreBackpressureTotalObserved.add(snapshot.eventStoreBackpressureTotal);
  archiveLagObserved.add(snapshot.archiveLagMax);
  archivePendingRowsObserved.add(snapshot.archivePendingRows);

  sleep(probeIntervalSec);
}

export function handleSummary(data) {
  const qpsWithBackpressure = findBackpressureQps(data.metrics);
  const qpsUntilBackpressure = qpsWithBackpressure.length > 0 ? qpsWithBackpressure[0].targetQps : null;

  const result = {
    run_id: data.setup_data && data.setup_data.runId,
    event_store_max_size_label:
      (data.setup_data && data.setup_data.eventStoreMaxSizeLabel) || eventStoreMaxSizeLabel,
    qps_until_backpressure: qpsUntilBackpressure,
    backpressure_by_qps: qpsWithBackpressure,
    append: {
      attempts: metricCount(data.metrics, 'append_attempts'),
      backpressure_total: metricCount(data.metrics, 'append_backpressure_total'),
      backpressure_rate: metricValue(data.metrics, 'append_backpressure_rate'),
      p95_ms: metricPercentile(data.metrics, 'append_latency', 'p(95)'),
      p99_ms: metricPercentile(data.metrics, 'append_latency', 'p(99)'),
    },
    lag: {
      archive_lag_max: metricMax(data.metrics, 'archive_lag_observed'),
      archive_pending_rows_max: metricMax(data.metrics, 'archive_pending_rows_observed'),
    },
    event_store: {
      memory_bytes_max: metricMax(data.metrics, 'event_store_memory_bytes_observed'),
      backpressure_total_metric_max: metricMax(
        data.metrics,
        'event_store_backpressure_total_observed'
      ),
    },
  };

  const stdout = [
    '',
    `run_id: ${result.run_id}`,
    `event_store_max_size_label: ${result.event_store_max_size_label}`,
    `qps_until_backpressure: ${result.qps_until_backpressure === null ? 'none' : result.qps_until_backpressure}`,
    `archive_lag_max: ${result.lag.archive_lag_max}`,
    `archive_pending_rows_max: ${result.lag.archive_pending_rows_max}`,
    `event_store_memory_bytes_max: ${result.event_store.memory_bytes_max}`,
    `append_backpressure_total: ${result.append.backpressure_total}`,
    '',
  ].join('\n');

  return {
    stdout,
    'backpressure-capacity-summary.json': JSON.stringify(result, null, 2),
  };
}

function buildStageRates(start, step, max) {
  const safeStart = Math.max(1, Math.floor(start));
  const safeStep = Math.max(1, Math.floor(step));
  const safeMax = Math.max(safeStart, Math.floor(max));
  const rates = [];

  for (let rate = safeStart; rate <= safeMax; rate += safeStep) {
    rates.push(rate);
  }

  if (rates[rates.length - 1] !== safeMax) {
    rates.push(safeMax);
  }

  return rates;
}

function durationToSeconds(raw) {
  const input = String(raw).trim();
  const match = input.match(/^(\d+)(ms|s|m|h)$/);
  if (!match) return 20;

  const amount = Number(match[1]);
  const unit = match[2];

  if (unit === 'ms') return Math.max(1, Math.floor(amount / 1000));
  if (unit === 's') return amount;
  if (unit === 'm') return amount * 60;
  if (unit === 'h') return amount * 3600;

  return 20;
}

function stageForProgress(progress, rates) {
  const bounded = Math.max(0, Math.min(0.999999, progress));
  const index = Math.min(rates.length - 1, Math.floor(bounded * rates.length));
  return { index, targetQps: rates[index] };
}

function parsePrometheusSnapshot(body) {
  const snapshot = {
    eventStoreMemoryBytes: 0,
    eventStoreBackpressureTotal: 0,
    archiveLagMax: 0,
    archivePendingRows: 0,
  };

  const lines = String(body).split('\n');

  for (const line of lines) {
    if (!line || line[0] === '#') continue;

    const idx = line.lastIndexOf(' ');
    if (idx <= 0) continue;

    const metricWithLabels = line.slice(0, idx);
    const valueRaw = line.slice(idx + 1).trim();
    const value = Number(valueRaw);
    if (!Number.isFinite(value)) continue;

    const name = metricWithLabels.split('{')[0];

    if (name === 'starcite_event_store_memory_bytes') {
      snapshot.eventStoreMemoryBytes = value;
      continue;
    }

    if (name === 'starcite_event_store_backpressure_total') {
      snapshot.eventStoreBackpressureTotal += value;
      continue;
    }

    if (name === 'starcite_archive_lag') {
      snapshot.archiveLagMax = Math.max(snapshot.archiveLagMax, value);
      continue;
    }

    if (name === 'starcite_archive_pending_rows') {
      snapshot.archivePendingRows = value;
    }
  }

  return snapshot;
}

function findBackpressureQps(metrics) {
  const rows = [];
  const entries = Object.entries(metrics || {});

  for (const [name, metric] of entries) {
    if (!name.startsWith('append_backpressure_total{')) continue;

    const match = name.match(/target_qps:([^,}]+)/);
    if (!match) continue;

    const parsed = Number(match[1].replace(/"/g, ''));
    if (!Number.isFinite(parsed)) continue;

    const count = metricCountValue(metric);
    if (count <= 0) continue;

    rows.push({ targetQps: parsed, count });
  }

  rows.sort((a, b) => a.targetQps - b.targetQps);
  return rows;
}

function metricCount(metrics, key) {
  const metric = metrics && metrics[key];
  return metricCountValue(metric);
}

function metricCountValue(metric) {
  if (!metric || typeof metric !== 'object') return 0;
  if (typeof metric.count === 'number') return metric.count;
  if (metric.values && typeof metric.values.count === 'number') return metric.values.count;
  if (typeof metric.passes === 'number' && typeof metric.fails === 'number') {
    return metric.passes + metric.fails;
  }
  if (
    metric.values &&
    typeof metric.values.passes === 'number' &&
    typeof metric.values.fails === 'number'
  ) {
    return metric.values.passes + metric.values.fails;
  }
  return 0;
}

function metricNumber(metric, key) {
  if (!metric || typeof metric !== 'object') return 0;
  if (typeof metric[key] === 'number') return metric[key];
  if (metric.values && typeof metric.values[key] === 'number') return metric.values[key];
  return 0;
}

function metricValue(metrics, key) {
  const metric = metrics && metrics[key];
  return metricNumber(metric, 'value');
}

function metricMax(metrics, key) {
  const metric = metrics && metrics[key];
  return metricNumber(metric, 'max');
}

function metricPercentile(metrics, key, percentile) {
  const metric = metrics && metrics[key];
  if (!metric || typeof metric !== 'object') return 0;
  const value =
    typeof metric[percentile] === 'number'
      ? metric[percentile]
      : metric.values && typeof metric.values[percentile] === 'number'
        ? metric.values[percentile]
        : null;
  return typeof value === 'number' ? value : 0;
}
