CREATE TABLE IF NOT EXISTS session_producers (
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  producer_id text NOT NULL,
  last_producer_seq bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (session_id, producer_id),
  CONSTRAINT session_producers_producer_id_not_empty CHECK (producer_id <> ''),
  CONSTRAINT session_producers_last_producer_seq_positive CHECK (last_producer_seq > 0)
);

INSERT INTO session_producers (
  session_id,
  producer_id,
  last_producer_seq,
  updated_at
)
SELECT DISTINCT ON (session_id, producer_id)
  session_id,
  producer_id,
  producer_seq,
  inserted_at
FROM events
ORDER BY
  session_id,
  producer_id,
  producer_seq DESC,
  inserted_at DESC
ON CONFLICT (session_id, producer_id) DO UPDATE
SET
  last_producer_seq = GREATEST(
    session_producers.last_producer_seq,
    EXCLUDED.last_producer_seq
  ),
  updated_at = GREATEST(session_producers.updated_at, EXCLUDED.updated_at);
