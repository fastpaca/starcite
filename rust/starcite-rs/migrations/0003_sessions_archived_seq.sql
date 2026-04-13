ALTER TABLE sessions
ADD COLUMN IF NOT EXISTS archived_seq bigint NOT NULL DEFAULT 0;

UPDATE sessions
SET archived_seq = last_seq
WHERE archived_seq < last_seq;
