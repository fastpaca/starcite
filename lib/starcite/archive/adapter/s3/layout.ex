defmodule Starcite.Archive.Adapter.S3.Layout do
  @moduledoc """
  Computes S3 keys and chunk boundaries for archive storage.

  Layout is intentionally simple and fixed:
  - Event chunks: `<prefix>/events/v1/<base64url(tenant_id)>/<base64url(session_id)>/<chunk_start>.ndjson`
  - Session blobs: `<prefix>/sessions/v1/<base64url(tenant_id)>/<base64url(session_id)>.json`
  - Session tenant index: `<prefix>/session-tenants/v1/<base64url(session_id)>.json`
  - One object is one cache-line chunk.

  Chunk boundaries are sequence-based. For a chunk size of `256`, sequence
  `1..256` maps to `1.ndjson`, `257..512` maps to `257.ndjson`, and so on.
  """

  @events_prefix "events/v1"
  @sessions_prefix "sessions/v1"
  @session_tenants_prefix "session-tenants/v1"
  @schema_prefix "schema"

  @spec group_event_rows([map()], pos_integer()) ::
          %{{String.t(), String.t(), pos_integer()} => [map()]}
  def group_event_rows(rows, chunk_size) do
    Enum.group_by(rows, fn %{tenant_id: tenant_id, session_id: session_id, seq: seq} ->
      {tenant_id, session_id, chunk_start_for(seq, chunk_size)}
    end)
  end

  @spec event_chunk_key(map(), String.t(), String.t(), pos_integer()) :: String.t()
  def event_chunk_key(%{prefix: prefix}, tenant_id, session_id, chunk_start),
    do:
      "#{prefix}/#{@events_prefix}/#{encode(tenant_id)}/#{encode(session_id)}/#{chunk_start}.ndjson"

  @spec legacy_event_chunk_key(map(), String.t(), pos_integer()) :: String.t()
  def legacy_event_chunk_key(%{prefix: prefix}, session_id, chunk_start),
    do: "#{prefix}/#{@events_prefix}/#{encode(session_id)}/#{chunk_start}.ndjson"

  @spec event_prefix(map()) :: String.t()
  def event_prefix(%{prefix: prefix}), do: "#{prefix}/#{@events_prefix}/"

  @spec session_prefix(map()) :: String.t()
  def session_prefix(%{prefix: prefix}), do: "#{prefix}/#{@sessions_prefix}/"

  @spec session_key(map(), String.t(), String.t()) :: String.t()
  def session_key(%{prefix: prefix}, tenant_id, session_id),
    do: "#{prefix}/#{@sessions_prefix}/#{encode(tenant_id)}/#{encode(session_id)}.json"

  @spec legacy_session_key(map(), String.t()) :: String.t()
  def legacy_session_key(%{prefix: prefix}, session_id),
    do: "#{prefix}/#{@sessions_prefix}/#{encode(session_id)}.json"

  @spec session_tenant_index_key(map(), String.t()) :: String.t()
  def session_tenant_index_key(%{prefix: prefix}, session_id),
    do: "#{prefix}/#{@session_tenants_prefix}/#{encode(session_id)}.json"

  @spec session_tenant_index_prefix(map()) :: String.t()
  def session_tenant_index_prefix(%{prefix: prefix}), do: "#{prefix}/#{@session_tenants_prefix}/"

  @spec schema_prefix(map()) :: String.t()
  def schema_prefix(%{prefix: prefix}), do: "#{prefix}/#{@schema_prefix}/"

  @spec schema_meta_key(map()) :: String.t()
  def schema_meta_key(%{prefix: prefix}), do: "#{prefix}/#{@schema_prefix}/meta.json"

  @spec chunk_starts_for_range(pos_integer(), pos_integer(), pos_integer()) :: [pos_integer()]
  def chunk_starts_for_range(from_seq, to_seq, chunk_size) do
    first = chunk_start_for(from_seq, chunk_size)
    last = chunk_start_for(to_seq, chunk_size)

    Enum.to_list(first..last//chunk_size)
  end

  @spec chunk_start_for(pos_integer(), pos_integer()) :: pos_integer()
  def chunk_start_for(seq, chunk_size), do: div(seq - 1, chunk_size) * chunk_size + 1

  defp encode(value), do: Base.url_encode64(value, padding: false)
end
