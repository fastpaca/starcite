defmodule Starcite.Storage.EventArchive.S3.Layout do
  @moduledoc """
  Computes S3 keys and chunk boundaries for legacy archive storage.
  """

  @events_prefix "events/v1"

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
