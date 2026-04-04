const tinyFallback = [
  {
    type: 'content',
    payload: { text: 'starcite benchmark payload' },
    source: 'benchmark',
    metadata: { scenario: 'fallback', corpus: 'tiny' },
  },
];

const realisticFallback = [
  {
    type: 'message.user',
    payload: {
      role: 'user',
      parts: [
        {
          type: 'text',
          text: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        },
      ],
    },
    source: 'benchmark',
    metadata: { surface: 'chat', shape: 'message.user' },
  },
  {
    type: 'agent.plan',
    payload: {
      steps: [
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
      ],
    },
    source: 'benchmark',
    metadata: { surface: 'chat', shape: 'agent.plan' },
  },
  {
    type: 'agent.streaming.chunk',
    payload: {
      text: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
      index: 0,
      finish_reason: null,
    },
    source: 'benchmark',
    metadata: { surface: 'chat', shape: 'agent.streaming.chunk' },
  },
  {
    type: 'message.assistant.delta',
    payload: {
      delta: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
      role: 'assistant',
    },
    source: 'benchmark',
    metadata: { surface: 'chat', shape: 'message.assistant.delta' },
  },
  {
    type: 'runtime.pi.event',
    payload: {
      level: 'info',
      message: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
      runtime: 'xxxxxxxxxxxx',
      sequence: 42,
    },
    source: 'benchmark',
    metadata: { surface: 'runtime', shape: 'runtime.pi.event' },
  },
  {
    type: 'agent.done',
    payload: {
      finish_reason: 'stop',
      usage: { input_tokens: 1532, output_tokens: 428, total_tokens: 1960 },
    },
    source: 'benchmark',
    metadata: { surface: 'chat', shape: 'agent.done' },
  },
];

const corpusMode = (__ENV.BENCH_EVENT_CORPUS_MODE || 'realistic').trim().toLowerCase();
const corpusFile = (__ENV.BENCH_EVENT_CORPUS_FILE || './corpus/default.json').trim();
const loadedCorpus = loadCorpus();

export const eventCorpus = loadedCorpus.events;
export const eventCorpusSummary = loadedCorpus.summary;

export function buildBenchmarkEvent(context) {
  const template =
    corpusMode === 'tiny'
      ? tinyEvent(context)
      : selectTemplate(eventCorpus, context);

  const event = {
    type: template.type,
    payload: materialize(cloneValue(template.payload || {}), context),
    producer_id: context.producerId,
    producer_seq: context.producerSeq,
  };

  const actor = context.actorOverride || template.actor;
  if (actor !== undefined && actor !== null && actor !== '') {
    event.actor = materialize(actor, context);
  }

  const source = template.source || 'benchmark';
  if (source !== undefined && source !== null && source !== '') {
    event.source = materialize(source, context);
  }

  const metadata = Object.assign(
    {},
    materialize(cloneValue(template.metadata || {}), context),
    context.metadata || {}
  );

  if (Object.keys(metadata).length > 0) {
    event.metadata = metadata;
  }

  if (template.refs && Object.keys(template.refs).length > 0) {
    event.refs = materialize(cloneValue(template.refs), context);
  }

  if (template.idempotency_key !== undefined && template.idempotency_key !== null) {
    event.idempotency_key = materialize(template.idempotency_key, context);
  }

  return event;
}

export function estimateAppendRequestBytes(event) {
  const body = {
    type: event.type,
    payload: event.payload,
    producer_id: event.producer_id,
    producer_seq: event.producer_seq,
  };

  if (event.actor !== undefined) body.actor = event.actor;
  if (event.source !== undefined) body.source = event.source;
  if (event.metadata !== undefined) body.metadata = event.metadata;
  if (event.refs !== undefined) body.refs = event.refs;
  if (event.idempotency_key !== undefined) body.idempotency_key = event.idempotency_key;

  return jsonByteLength(body);
}

export function estimatePayloadBytes(payload) {
  return jsonByteLength(payload);
}

function loadCorpus() {
  if (corpusMode === 'tiny') {
    return summarizeCorpus(tinyFallback, 'tiny_fallback');
  }

  const parsed = loadCorpusFile(corpusFile);
  if (parsed) {
    return summarizeCorpus(parsed.events, parsed.source || 'corpus_file');
  }

  return summarizeCorpus(realisticFallback, 'realistic_fallback');
}

function loadCorpusFile(path) {
  if (!path) {
    return null;
  }

  try {
    const raw = open(path);
    const parsed = JSON.parse(raw);
    const events = Array.isArray(parsed.events) ? parsed.events.filter(validTemplate) : [];

    if (events.length === 0) {
      return null;
    }

    return { source: parsed.source, events };
  } catch (_error) {
    return null;
  }
}

function summarizeCorpus(events, source) {
  const payloadSizes = events.map((event) => estimatePayloadBytes(event.payload || {}));
  const requestSizes = events.map((template) => {
    const request = {
      type: template.type,
      payload: template.payload || {},
      producer_id: 'writer:vu:1:p:1',
      producer_seq: 1,
    };

    if (template.source) {
      request.source = template.source;
    }

    if (template.metadata && Object.keys(template.metadata).length > 0) {
      request.metadata = template.metadata;
    }

    return estimateAppendRequestBytes(request);
  });

  return {
    events,
    summary: {
      source,
      eventCount: events.length,
      payloadP50: percentile(payloadSizes, 0.5),
      payloadP95: percentile(payloadSizes, 0.95),
      appendBodyP50: percentile(requestSizes, 0.5),
      appendBodyP95: percentile(requestSizes, 0.95),
    },
  };
}

function tinyEvent(context) {
  return {
    type: 'content',
    payload: {
      text: `starcite hot-path run=${context.runId} vu=${context.vuId} p=${context.producerSlot} seq=${context.producerSeq}`,
    },
    source: 'benchmark',
    metadata: { scenario: context.scenario, corpus: 'tiny' },
  };
}

function selectTemplate(events, context) {
  const index =
    Math.abs(
      context.vuId * 73856093 +
        context.sessionIndex * 19349663 +
        context.producerSlot * 83492791 +
        context.producerSeq
    ) % events.length;

  return events[index];
}

function validTemplate(event) {
  return (
    event &&
    typeof event.type === 'string' &&
    event.type !== '' &&
    event.payload &&
    typeof event.payload === 'object' &&
    !Array.isArray(event.payload)
  );
}

function percentile(values, p) {
  if (values.length === 0) {
    return 0;
  }

  const sorted = values.slice().sort((a, b) => a - b);
  const index = Math.floor((sorted.length - 1) * p);
  return sorted[index];
}

function jsonByteLength(value) {
  return String(JSON.stringify(value)).length;
}

function cloneValue(value) {
  if (Array.isArray(value)) {
    return value.map(cloneValue);
  }

  if (value && typeof value === 'object') {
    const clone = {};

    for (const [key, nested] of Object.entries(value)) {
      clone[key] = cloneValue(nested);
    }

    return clone;
  }

  return value;
}

function materialize(value, context) {
  if (typeof value === 'string') {
    return value
      .replaceAll('{{run_id}}', context.runId)
      .replaceAll('{{vu}}', String(context.vuId))
      .replaceAll('{{session_id}}', context.sessionId)
      .replaceAll('{{session_index}}', String(context.sessionIndex))
      .replaceAll('{{producer_id}}', context.producerId)
      .replaceAll('{{producer_seq}}', String(context.producerSeq))
      .replaceAll('{{producer_slot}}', String(context.producerSlot));
  }

  if (Array.isArray(value)) {
    return value.map((nested) => materialize(nested, context));
  }

  if (value && typeof value === 'object') {
    const materialized = {};

    for (const [key, nested] of Object.entries(value)) {
      materialized[key] = materialize(nested, context);
    }

    return materialized;
  }

  return value;
}
