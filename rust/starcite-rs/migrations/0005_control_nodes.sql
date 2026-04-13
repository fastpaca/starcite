CREATE TABLE control_nodes (
  node_id TEXT PRIMARY KEY,
  public_url TEXT,
  ops_url TEXT NOT NULL,
  draining BOOLEAN NOT NULL DEFAULT FALSE,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX control_nodes_expires_at_idx
  ON control_nodes (expires_at);

CREATE INDEX control_nodes_draining_idx
  ON control_nodes (draining);

ALTER TABLE session_leases
  ADD COLUMN standby_node_id TEXT;
