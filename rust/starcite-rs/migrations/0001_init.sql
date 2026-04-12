CREATE TABLE IF NOT EXISTS sessions (
  id text PRIMARY KEY,
  title text,
  tenant_id text NOT NULL,
  creator_id text,
  creator_type text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_seq bigint NOT NULL DEFAULT 0,
  archived boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  version bigint NOT NULL DEFAULT 1,
  CONSTRAINT sessions_tenant_id_not_empty CHECK (tenant_id <> ''),
  CONSTRAINT sessions_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT sessions_last_seq_non_negative CHECK (last_seq >= 0),
  CONSTRAINT sessions_version_positive CHECK (version > 0)
);

CREATE INDEX IF NOT EXISTS sessions_tenant_archived_id_idx
ON sessions (tenant_id, archived, id);

CREATE INDEX IF NOT EXISTS sessions_metadata_gin_idx
ON sessions USING gin (metadata);

CREATE TABLE IF NOT EXISTS events (
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  seq bigint NOT NULL,
  type text NOT NULL,
  payload jsonb NOT NULL,
  actor text NOT NULL,
  source text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  refs jsonb NOT NULL DEFAULT '{}'::jsonb,
  idempotency_key text,
  producer_id text NOT NULL,
  producer_seq bigint NOT NULL,
  tenant_id text NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (session_id, seq),
  UNIQUE (session_id, producer_id, producer_seq),
  CONSTRAINT events_seq_positive CHECK (seq > 0),
  CONSTRAINT events_producer_seq_positive CHECK (producer_seq > 0),
  CONSTRAINT events_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT events_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT events_refs_is_object CHECK (jsonb_typeof(refs) = 'object'),
  CONSTRAINT events_tenant_id_not_empty CHECK (tenant_id <> '')
);

CREATE INDEX IF NOT EXISTS events_session_inserted_at_idx
ON events (session_id, inserted_at);

CREATE INDEX IF NOT EXISTS events_tenant_session_seq_idx
ON events (tenant_id, session_id, seq);

CREATE TABLE IF NOT EXISTS lifecycle_events (
  seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id text NOT NULL,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  event jsonb NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT lifecycle_events_tenant_id_not_empty CHECK (tenant_id <> ''),
  CONSTRAINT lifecycle_events_session_id_not_empty CHECK (session_id <> ''),
  CONSTRAINT lifecycle_events_event_is_object CHECK (jsonb_typeof(event) = 'object')
);

CREATE INDEX IF NOT EXISTS lifecycle_events_tenant_seq_idx
ON lifecycle_events (tenant_id, seq);

CREATE INDEX IF NOT EXISTS lifecycle_events_session_seq_idx
ON lifecycle_events (session_id, seq);
