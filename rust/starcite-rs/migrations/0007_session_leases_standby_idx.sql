CREATE INDEX session_leases_standby_node_id_expires_at_idx
  ON session_leases (standby_node_id, expires_at)
  WHERE standby_node_id IS NOT NULL;
