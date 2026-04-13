CREATE TABLE session_leases (
  session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
  owner_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX session_leases_owner_id_idx
  ON session_leases (owner_id);

CREATE INDEX session_leases_expires_at_idx
  ON session_leases (expires_at);
