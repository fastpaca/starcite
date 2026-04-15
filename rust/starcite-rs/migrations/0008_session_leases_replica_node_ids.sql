ALTER TABLE session_leases
  ADD COLUMN replica_node_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];

UPDATE session_leases
SET replica_node_ids = ARRAY_REMOVE(ARRAY[owner_id, standby_node_id], NULL)
WHERE replica_node_ids = ARRAY[]::TEXT[];

CREATE INDEX session_leases_replica_node_ids_gin_idx
  ON session_leases
  USING gin (replica_node_ids);
